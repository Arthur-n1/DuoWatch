const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const html = fs.readFileSync(path.join(__dirname, 'public', 'index.html'));

const server = http.createServer((req, res) => {
  if (req.url === '/config') {
    const iceServers = [
      { urls: ['stun:stun.l.google.com:19302', 'stun:stun1.l.google.com:19302'] },
    ];
    // TURN optionnel (si la connexion échoue entre certains réseaux, ex : 4G, réseau d'entreprise)
    if (process.env.TURN_URL) {
      iceServers.push({
        urls: process.env.TURN_URL.split(',').map(s => s.trim()),
        username: process.env.TURN_USER,
        credential: process.env.TURN_PASS,
      });
    }
    res.writeHead(200, { 'Content-Type': 'application/json' });
    return res.end(JSON.stringify({ iceServers }));
  }
  if (req.url === '/health') {
    res.writeHead(200);
    return res.end('ok');
  }
  res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
  res.end(html);
});

const wss = new WebSocketServer({ server });
const rooms = new Map(); // nom du salon -> Set de sockets (2 max)

wss.on('connection', ws => {
  ws.isAlive = true;
  ws.on('pong', () => (ws.isAlive = true));

  ws.on('message', raw => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    if (msg.type === 'join') {
      const room = String(msg.room || '').trim().toLowerCase().slice(0, 50);
      if (!room || ws.room) return;
      const peers = rooms.get(room) || new Set();
      if (peers.size >= 2) return ws.send(JSON.stringify({ type: 'full' }));
      peers.add(ws);
      rooms.set(room, peers);
      ws.room = room;
      ws.send(JSON.stringify({ type: 'joined' }));
      if (peers.size === 2) {
        let i = 0;
        for (const p of peers) p.send(JSON.stringify({ type: 'ready', polite: i++ === 1 }));
      }
      return;
    }

    // Tout le reste est relayé à l'autre personne du salon
    const peers = rooms.get(ws.room);
    if (!peers) return;
    for (const p of peers) if (p !== ws && p.readyState === 1) p.send(raw.toString());
  });

  ws.on('close', () => {
    const peers = rooms.get(ws.room);
    if (!peers) return;
    peers.delete(ws);
    if (peers.size === 0) rooms.delete(ws.room);
    else for (const p of peers) p.send(JSON.stringify({ type: 'peer-left' }));
  });
});

// Keep-alive pour éviter que les WebSockets soient coupées par le proxy
setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) { ws.terminate(); continue; }
    ws.isAlive = false;
    ws.ping();
  }
}, 25000);

server.listen(PORT, () => console.log(`Duo Watch en ligne sur le port ${PORT}`));
