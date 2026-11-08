extends Node
## Ejemplo de cliente Godot 4 para el protocolo binario de server.js.
## Adáptalo a tu escena: crea esto como un Autoload (p. ej. "Net") o un nodo hijo del mundo.

const URL := "ws://127.0.0.1:8080"
const SEND_RATE := 20.0            # debe coincidir con TICK_RATE del servidor (no tiene sentido enviar más)
const INTERP_SPEED := 15.0         # suavizado de jugadores remotos

# Cliente → Servidor
const C_POS := 0x01
const C_PING := 0x20
# Servidor → Cliente
const S_WELCOME := 0x01
const S_JOINED := 0x02
const S_LEFT := 0x03
const S_CURRENT := 0x04
const S_SNAPSHOT := 0x10
const S_PONG := 0x21

const ENTITY_SIZE := 17

# Los estados de animación ahora son un número (u8), no un String. Ajusta a tu juego.
enum Anim { IDLE, WALK, RUN, JUMP }
const ANIM_NAMES := ["idle", "walk", "run", "jump"]

@export var player_scene: PackedScene     # escena del jugador remoto (Node3D)
@export var local_player: Node3D          # tu jugador local

var ws := WebSocketPeer.new()
var connected := false
var my_id := 0
var remotes: Dictionary = {}              # id:int → { "node": Node3D, "pos": Vector3, "rot": float }
var send_accum := 0.0
var ping_ms := 0


func _ready() -> void:
	ws.connect_to_url(URL)
	var t := Timer.new()
	t.wait_time = 2.0
	t.autostart = true
	t.timeout.connect(_send_ping)
	add_child(t)


func _process(delta: float) -> void:
	ws.poll()
	match ws.get_ready_state():
		WebSocketPeer.STATE_OPEN:
			connected = true
			while ws.get_available_packet_count() > 0:
				_handle_packet(ws.get_packet())
			_send_position(delta)
		WebSocketPeer.STATE_CLOSED:
			if connected:
				connected = false
				push_warning("Desconectado del servidor (%s)" % ws.get_close_reason())

	# Interpolación de jugadores remotos (suaviza los 20 snapshots/s a tus FPS)
	var w := clampf(delta * INTERP_SPEED, 0.0, 1.0)
	for r in remotes.values():
		var n: Node3D = r["node"]
		n.global_position = n.global_position.lerp(r["pos"], w)
		n.rotation.y = lerp_angle(n.rotation.y, r["rot"], w)


# ───────────── Envío ─────────────
func _send_position(delta: float) -> void:
	if local_player == null:
		return
	send_accum += delta
	if send_accum < 1.0 / SEND_RATE:
		return
	send_accum = 0.0

	var p := local_player.global_position
	var buf := PackedByteArray()
	buf.resize(18)
	buf.encode_u8(0, C_POS)
	buf.encode_float(1, p.x)
	buf.encode_float(5, p.y)
	buf.encode_float(9, p.z)
	buf.encode_float(13, local_player.rotation.y)
	buf.encode_u8(17, _current_anim())
	ws.send(buf, WebSocketPeer.WRITE_MODE_BINARY)


func _current_anim() -> int:
	# TODO: devuelve el estado real de tu jugador local
	return Anim.IDLE


func _send_ping() -> void:
	if not connected:
		return
	var buf := PackedByteArray()
	buf.resize(5)
	buf.encode_u8(0, C_PING)
	buf.encode_u32(1, Time.get_ticks_msec())
	ws.send(buf, WebSocketPeer.WRITE_MODE_BINARY)


# ───────────── Recepción ─────────────
func _handle_packet(d: PackedByteArray) -> void:
	if d.size() == 0:
		return
	match d[0]:
		S_WELCOME:
			my_id = d.decode_u16(1)
		S_JOINED:
			_apply_entity(d, 1, true)
		S_LEFT:
			_remove_remote(d.decode_u16(1))
		S_CURRENT, S_SNAPSHOT:
			var n := d.decode_u16(1)
			var off := 3
			for i in n:
				_apply_entity(d, off, d[0] == S_CURRENT)
				off += ENTITY_SIZE
		S_PONG:
			ping_ms = Time.get_ticks_msec() - d.decode_u32(1)


func _apply_entity(d: PackedByteArray, off: int, snap_now: bool) -> void:
	var id := d.decode_u16(off)
	if id == my_id:
		return  # el snapshot incluye tu propio estado: se ignora (útil para reconciliación futura)

	var pos := Vector3(d.decode_float(off + 2), d.decode_float(off + 6), d.decode_float(off + 10))
	var rot := float(d.decode_u16(off + 14)) / 65535.0 * TAU   # 0..2π (lerp_angle lo maneja bien)
	var anim := d[off + 16]

	var r = remotes.get(id)
	if r == null:
		var node: Node3D = player_scene.instantiate()
		get_parent().add_child(node)
		node.global_position = pos
		node.rotation.y = rot
		r = { "node": node, "pos": pos, "rot": rot }
		remotes[id] = r

	r["pos"] = pos
	r["rot"] = rot
	if snap_now:
		r["node"].global_position = pos
		r["node"].rotation.y = rot

	# TODO: aplica la animación a tu nodo remoto
	if r["node"].has_method("set_anim"):
		r["node"].set_anim(ANIM_NAMES[anim] if anim < ANIM_NAMES.size() else "idle")


func _remove_remote(id: int) -> void:
	var r = remotes.get(id)
	if r != null:
		r["node"].queue_free()
		remotes.erase(id)
