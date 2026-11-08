extends Node

var socket = WebSocketPeer.new()
# Cambia esta IP por la de la computadora que está corriendo el servidor.
# Si estás probando en la misma PC, usa "ws://127.0.0.1:8080"
# Si estás probando entre dos PCs en la red local, usa la IP de la PC servidor (ej: "ws://192.168.1.50:8080")
var server_url = "ws://127.0.0.1:8080"
#var server_url = "ws://107.172.249.230:8080"

# Ahora el ID es un entero (uint16) asignado por el servidor. 0 = aún sin asignar.
var my_id = 0
var remote_players = {} # id:int -> { x, y, z, rot, anim }  (misma estructura que antes)
var ping_ms = 0

signal player_spawned(id)
signal player_despawned(id)

# ───── Protocolo binario (debe coincidir con server.js) ─────
const C_POS := 0x01
const C_PING := 0x20
const S_WELCOME := 0x01
const S_JOINED := 0x02
const S_LEFT := 0x03
const S_CURRENT := 0x04
const S_SNAPSHOT := 0x10
const S_PONG := 0x21
const ENTITY_SIZE := 17

# El servidor transporta la animación como un número (0-255).
# EDITA esta lista con los nombres de animación que usa tu juego (el índice es el número que viaja por la red).
# Es importante que todos los clientes tengan EXACTAMENTE la misma lista.
const ANIM_NAMES := ["idle", "walk", "run", "jump"]

const SEND_INTERVAL_MS := 50   # 20 envíos/s: el servidor reparte a 20 Hz, enviar más no aporta nada
const PING_INTERVAL_MS := 2000

var _last_send_ms := 0
var _last_ping_ms := 0
var _was_open := false
var _close_reported := false


func _ready():
	socket.connect_to_url(server_url)


func _process(_delta):
	socket.poll()
	var state = socket.get_ready_state()

	if state == WebSocketPeer.STATE_OPEN:
		_was_open = true
		var packets_processed = 0
		# Limitamos a procesar un máximo de paquetes por frame para evitar congelamientos (lag spikes o freezes)
		while socket.get_available_packet_count() > 0 and packets_processed < 50:
			_handle_packet(socket.get_packet())
			packets_processed += 1

		var now := Time.get_ticks_msec()
		if now - _last_ping_ms >= PING_INTERVAL_MS:
			_last_ping_ms = now
			_send_ping(now)

	elif state == WebSocketPeer.STATE_CLOSED:
		if not _close_reported:
			_close_reported = true
			print("Conexión cerrada con el servidor. Motivo: ", socket.get_close_reason())
			# Limpia a los remotos para que no queden "fantasmas" en tu escena
			for id in remote_players.keys():
				emit_signal("player_despawned", id)
			remote_players.clear()
			my_id = 0


# Misma firma que antes: tu código del jugador no cambia.
func send_position(pos: Vector3, rot: float, anim_state: String):
	if socket.get_ready_state() != WebSocketPeer.STATE_OPEN:
		return
	var now := Time.get_ticks_msec()
	if now - _last_send_ms < SEND_INTERVAL_MS:
		return
	_last_send_ms = now

	var anim_idx := ANIM_NAMES.find(anim_state)
	if anim_idx < 0:
		anim_idx = 0

	var buf := PackedByteArray()
	buf.resize(18)
	buf.encode_u8(0, C_POS)
	buf.encode_float(1, pos.x)
	buf.encode_float(5, pos.y)
	buf.encode_float(9, pos.z)
	buf.encode_float(13, rot)
	buf.encode_u8(17, anim_idx)
	socket.send(buf, WebSocketPeer.WRITE_MODE_BINARY)


func _send_ping(now: int):
	var buf := PackedByteArray()
	buf.resize(5)
	buf.encode_u8(0, C_PING)
	buf.encode_u32(1, now)
	socket.send(buf, WebSocketPeer.WRITE_MODE_BINARY)


func _handle_packet(d: PackedByteArray):
	if d.size() == 0:
		return
	match d[0]:
		S_WELCOME:
			my_id = d.decode_u16(1)
			print("¡Conectado! Mi ID asignado es: ", my_id)

		S_JOINED:
			_apply_entity(d, 1)

		S_CURRENT, S_SNAPSHOT:
			var n := d.decode_u16(1)
			var off := 3
			for i in n:
				_apply_entity(d, off)
				off += ENTITY_SIZE

		S_LEFT:
			var id := d.decode_u16(1)
			if remote_players.has(id):
				remote_players.erase(id)
				emit_signal("player_despawned", id)

		S_PONG:
			ping_ms = Time.get_ticks_msec() - d.decode_u32(1)


# Lee una entidad (17 bytes) y actualiza/crea el jugador remoto.
func _apply_entity(d: PackedByteArray, off: int):
	var id := d.decode_u16(off)
	if id == my_id:
		return  # el snapshot incluye tu propio estado: se ignora

	var x := d.decode_float(off + 2)
	var y := d.decode_float(off + 6)
	var z := d.decode_float(off + 10)
	# Rotación llega en 0..2π; la devolvemos a -π..π como la enviaba el cliente
	var rot := float(d.decode_u16(off + 14)) / 65535.0 * TAU
	if rot > PI:
		rot -= TAU
	var anim_idx := d[off + 16]
	var anim: String = ANIM_NAMES[anim_idx] if anim_idx < ANIM_NAMES.size() else "idle"

	if not remote_players.has(id):
		remote_players[id] = { "x": x, "y": y, "z": z, "rot": rot, "anim": anim }
		emit_signal("player_spawned", id)
	else:
		var p = remote_players[id]
		p.x = x
		p.y = y
		p.z = z
		p.rot = rot
		p.anim = anim
