const { WebSocketServer, WebSocket } = require('ws');

// ───────────────────────── Configuración ─────────────────────────
const PORT = process.env.PORT || 8080;
const TICK_RATE = 20;                       // snapshots por segundo
const TICK_MS = 1000 / TICK_RATE;
const FULL_SYNC_TICKS = TICK_RATE * 2;      // cada 2 s se reenvía el estado completo (cura deltas saltados)
const HEARTBEAT_MS = 30000;                 // ping cada 30 s; sin pong → se termina la conexión
const MAX_BUFFERED_BYTES = 64 * 1024;       // backpressure: no se envía a clientes con >64 KB pendientes
const MAX_MSGS_PER_SEC = 150;               // por encima de esto se considera flood y se expulsa
const WORLD_LIMIT = 1_000_000;              // las coordenadas se limitan a ±WORLD_LIMIT
const MAX_PLAYERS = 65534;                  // los IDs son uint16

// ───────────────────────── Protocolo binario (little-endian) ─────────────────────────
// Cliente → Servidor
//   0x01 POS   : u8 tipo, f32 x, f32 y, f32 z, f32 rot(rad), u8 anim        (18 bytes)
//   0x20 PING  : u8 tipo, u32 timestamp_cliente                              (5 bytes)
// Servidor → Cliente
//   0x01 WELCOME        : u8 tipo, u16 id                                    (3 bytes)
//   0x02 PLAYER_JOINED  : u8 tipo, entidad                                   (18 bytes)
//   0x03 PLAYER_LEFT    : u8 tipo, u16 id                                    (3 bytes)
//   0x04 CURRENT_PLAYERS: u8 tipo, u16 n, n × entidad
//   0x10 SNAPSHOT       : u8 tipo, u16 n, n × entidad   (solo los que cambiaron; incluye al propio jugador)
//   0x21 PONG           : u8 tipo, u32 timestamp_cliente
// entidad = u16 id, f32 x, f32 y, f32 z, u16 rot (0..65535 ≙ 0..2π), u8 anim   (17 bytes)
const C_POS = 0x01, C_PING = 0x20;
const S_WELCOME = 0x01, S_JOINED = 0x02, S_LEFT = 0x03, S_CURRENT = 0x04, S_SNAPSHOT = 0x10, S_PONG = 0x21;
const ENTITY_SIZE = 17;
const POS_MSG_SIZE = 18;
const TWO_PI = Math.PI * 2;

// ───────────────────────── Estado ─────────────────────────
const players = new Map();     // id → { id, ws, x, y, z, rot(u16), anim(u8) }
const dirty = new Set();       // jugadores que cambiaron desde el último tick
let nextId = 1;

function allocId() {
    if (players.size >= MAX_PLAYERS) return 0;
    while (true) {
        if (nextId > 0xFFFF) nextId = 1;
        const id = nextId++;
        if (!players.has(id)) return id;
    }
}

function writeEntity(buf, off, p) {
    buf.writeUInt16LE(p.id, off);
    buf.writeFloatLE(p.x, off + 2);
    buf.writeFloatLE(p.y, off + 6);
    buf.writeFloatLE(p.z, off + 10);
    buf.writeUInt16LE(p.rot, off + 14);
    buf.writeUInt8(p.anim, off + 16);
}

function buildEntityList(type, list) {
    const buf = Buffer.allocUnsafe(3 + list.length * ENTITY_SIZE);
    buf.writeUInt8(type, 0);
    buf.writeUInt16LE(list.length, 1);
    let off = 3;
    for (const p of list) {
        writeEntity(buf, off, p);
        off += ENTITY_SIZE;
    }
    return buf;
}

function sendTo(ws, buf) {
    if (ws.readyState === WebSocket.OPEN) ws.send(buf);
}

function broadcast(buf, exceptId = 0) {
    for (const p of players.values()) {
        if (p.id !== exceptId) sendTo(p.ws, buf);
    }
}

function sanitizeCoord(v) {
    if (!Number.isFinite(v)) return 0;
    return v > WORLD_LIMIT ? WORLD_LIMIT : v < -WORLD_LIMIT ? -WORLD_LIMIT : v;
}

function encodeRot(rad) {
    if (!Number.isFinite(rad)) return 0;
    const n = ((rad % TWO_PI) + TWO_PI) % TWO_PI;
    return Math.round((n / TWO_PI) * 0xFFFF);
}

// ───────────────────────── Servidor ─────────────────────────
const wss = new WebSocketServer({
    port: PORT,
    maxPayload: 64,              // los mensajes válidos miden ≤ 18 bytes
    perMessageDeflate: false,    // sin compresión: menos CPU y latencia
    clientTracking: false        // llevamos nuestro propio registro (players)
});

wss.on('error', (err) => console.error('[wss error]', err.message));

wss.on('connection', (ws) => {
    ws.on('error', (err) => console.error('[socket error]', err.message));

    const id = allocId();
    if (id === 0) {
        ws.close(1013, 'server full');
        return;
    }

    const player = { id, ws, x: 0, y: 0, z: 0, rot: 0, anim: 0 };
    ws.isAlive = true;
    ws.on('pong', () => { ws.isAlive = true; });

    // 1. Bienvenida con ID
    const welcome = Buffer.allocUnsafe(3);
    welcome.writeUInt8(S_WELCOME, 0);
    welcome.writeUInt16LE(id, 1);
    ws.send(welcome);

    // 2. Jugadores ya conectados (en un solo paquete)
    if (players.size > 0) {
        ws.send(buildEntityList(S_CURRENT, Array.from(players.values())));
    }

    // 3. Registrar y avisar a los demás
    players.set(id, player);
    const joined = Buffer.allocUnsafe(1 + ENTITY_SIZE);
    joined.writeUInt8(S_JOINED, 0);
    writeEntity(joined, 1, player);
    broadcast(joined, id);

    console.log(`> Jugador conectado: ${id} (total ${players.size})`);

    // Rate limiting: ventana de 1 s
    let windowStart = Date.now();
    let windowCount = 0;

    ws.on('message', (data, isBinary) => {
        if (!isBinary) return; // solo protocolo binario

        const now = Date.now();
        if (now - windowStart >= 1000) {
            windowStart = now;
            windowCount = 0;
        }
        if (++windowCount > MAX_MSGS_PER_SEC) {
            ws.close(1008, 'rate limit');
            return;
        }

        const msg = data; // Buffer (ws entrega Buffer por defecto)
        if (msg.length === 0) return;

        switch (msg[0]) {
            case C_POS: {
                if (msg.length !== POS_MSG_SIZE) return;
                // Siempre se guarda la última posición (nunca se descarta el mensaje más reciente);
                // la emisión ya está limitada por el tick.
                player.x = sanitizeCoord(msg.readFloatLE(1));
                player.y = sanitizeCoord(msg.readFloatLE(5));
                player.z = sanitizeCoord(msg.readFloatLE(9));
                player.rot = encodeRot(msg.readFloatLE(13));
                player.anim = msg[17];
                dirty.add(player);
                break;
            }
            case C_PING: {
                if (msg.length !== 5) return;
                const pong = Buffer.allocUnsafe(5);
                pong.writeUInt8(S_PONG, 0);
                msg.copy(pong, 1, 1, 5);
                sendTo(ws, pong);
                break;
            }
        }
    });

    ws.on('close', () => {
        if (!players.delete(id)) return;
        dirty.delete(player);
        console.log(`> Jugador desconectado: ${id} (total ${players.size})`);

        const left = Buffer.allocUnsafe(3);
        left.writeUInt8(S_LEFT, 0);
        left.writeUInt16LE(id, 1);
        broadcast(left);
    });
});

// ───────────────────────── Tick: snapshots con deltas ─────────────────────────
let tickCount = 0;
const tickTimer = setInterval(() => {
    tickCount++;
    if (players.size < 2) { dirty.clear(); return; }

    const full = tickCount % FULL_SYNC_TICKS === 0;
    const list = full ? Array.from(players.values()) : Array.from(dirty);
    dirty.clear();
    if (list.length === 0) return;

    // Se serializa UNA vez y se reutiliza el mismo Buffer para todos
    const snapshot = buildEntityList(S_SNAPSHOT, list);
    for (const p of players.values()) {
        // Backpressure: a un cliente saturado se le salta el tick (el full-sync lo resincroniza)
        if (p.ws.bufferedAmount > MAX_BUFFERED_BYTES) continue;
        sendTo(p.ws, snapshot);
    }
}, TICK_MS);

// ───────────────────────── Heartbeat ─────────────────────────
const heartbeatTimer = setInterval(() => {
    for (const p of players.values()) {
        if (!p.ws.isAlive) {
            p.ws.terminate();
            continue;
        }
        p.ws.isAlive = false;
        p.ws.ping();
    }
}, HEARTBEAT_MS);

// ───────────────────────── Apagado limpio y robustez ─────────────────────────
let shuttingDown = false;
function shutdown(reason) {
    if (shuttingDown) return;
    shuttingDown = true;
    console.log(`[Servidor] Apagando (${reason})...`);
    clearInterval(tickTimer);
    clearInterval(heartbeatTimer);
    for (const p of players.values()) p.ws.close(1001, 'server shutdown');
    wss.close(() => process.exit(0));
    setTimeout(() => process.exit(0), 2000).unref();
}
process.on('SIGINT', () => shutdown('SIGINT'));
process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('uncaughtException', (err) => {
    console.error('[uncaughtException]', err);
    shutdown('uncaughtException');
});
process.on('unhandledRejection', (err) => console.error('[unhandledRejection]', err));

console.log(`[Servidor] Corriendo en puerto ${PORT} (tick ${TICK_RATE} Hz, protocolo binario)`);
