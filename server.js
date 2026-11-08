const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 8080;
const wss = new WebSocketServer({ port: PORT });

const players = new Map();

wss.on('connection', (ws) => {
    const playerId = 'player_' + Date.now();
    console.log(`> Jugador conectado: ${playerId}`);

    // 1. Enviar ID de bienvenida
    ws.send(JSON.stringify({ type: 'WELCOME', id: playerId }));

    // 2. Enviar jugadores existentes al nuevo
    players.forEach((data, id) => {
        ws.send(JSON.stringify({ type: 'PLAYER_JOINED', id, ...data }));
    });

    ws.on('message', (message) => {
        let data;
        try {
            data = JSON.parse(message);
        } catch (e) {
            return;
        }

        if (data.type === 'UPDATE_POS') {
            // Guardar posición
            players.set(playerId, { 
                x: data.x, 
                y: data.y, 
                z: data.z, 
                rot: data.rot, 
                anim: data.anim 
            });

            // Reenviar a los demás INCLUYENDO EL ID
            const payload = JSON.stringify({
                type: 'PLAYER_MOVED',
                id: playerId, // <-- ¡Clave para que Godot sepa quién se mueve!
                x: data.x,
                y: data.y,
                z: data.z,
                rot: data.rot,
                anim: data.anim
            });

            wss.clients.forEach((client) => {
                if (client !== ws && client.readyState === ws.OPEN) {
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
            if (client.readyState === ws.OPEN) {
                client.send(payload);
            }
        });
    });
});

console.log(`[Servidor] Corriendo en puerto ${PORT}`);