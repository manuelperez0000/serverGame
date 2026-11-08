const { WebSocketServer, WebSocket } = require('ws');

const PORT = process.env.PORT || 8080;
const wss = new WebSocketServer({ port: PORT });

const players = new Map();

wss.on('connection', (ws) => {
    const playerId = 'player_' + Date.now();
    console.log(`> Jugador conectado: ${playerId}`);

    // 1. Enviar ID de bienvenida
    ws.send(JSON.stringify({ type: 'WELCOME', id: playerId }));

    // 2. [OPTIMIZACIÓN] Enviar todos los jugadores actuales en UN SOLO PAQUETE
    if (players.size > 0) {
        const existingPlayers = Array.from(players.entries()).map(([id, data]) => ({
            id,
            ...data
        }));
        ws.send(JSON.stringify({ type: 'CURRENT_PLAYERS', players: existingPlayers }));
    }

    // Variables para Rate Limiting (Anti-Flood / Anti-Spam)
    let lastMessageTime = 0;
    const MIN_MESSAGE_INTERVAL = 15; // Máximo aprox. ~66 mensajes por segundo por cliente

    ws.on('message', (message) => {
        let data;
        try {
            data = JSON.parse(message);
        } catch (e) {
            return; // Ignorar JSON malformado
        }

        if (data.type === 'UPDATE_POS') {
            // [SEGURIDAD] Rate Limiting básico
            const now = Date.now();
            if (now - lastMessageTime < MIN_MESSAGE_INTERVAL) return;
            lastMessageTime = now;

            // [SEGURIDAD] Sanitización básica de tipos de datos
            const posX = Number(data.x) || 0;
            const posY = Number(data.y) || 0;
            const posZ = Number(data.z) || 0;
            const posRot = Number(data.rot) || 0;
            const animState = String(data.anim || 'idle');

            // Guardar posición limpia
            players.set(playerId, { 
                x: posX, 
                y: posY, 
                z: posZ, 
                rot: posRot, 
                anim: animState 
            });

            // Preparar payload optimizado para difusión
            const payload = JSON.stringify({
                type: 'PLAYER_MOVED',
                id: playerId,
                x: posX,
                y: posY,
                z: posZ,
                rot: posRot,
                anim: animState
            });

            // Reenviar a los demás de forma eficiente
            wss.clients.forEach((client) => {
                if (client !== ws && client.readyState === WebSocket.OPEN) {
                    client.send(payload);
                }
            });
        }
    });

    ws.on('close', () => {
        players.delete(playerId);
        console.log(`> Jugador desconectado: ${playerId}`);

        const payload = JSON.stringify({ type: 'PLAYER_DISCONNECTED', id: playerId });
        wss.clients.forEach((client) => {
            if (client.readyState === WebSocket.OPEN) {
                client.send(payload);
            }
        });
    });
});

console.log(`[Servidor Senior] Corriendo en puerto ${PORT}`);