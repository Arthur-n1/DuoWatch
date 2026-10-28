const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const html = fs.readFileSync(path.join(__dirname, 'public', 'index.html'));
const MAX_FILMS = 10;

// ---------- Recherche films / séries (API iTunes, gratuite, sans clé) ----------
const searchCache = new Map();
const bigArt = u => (u ? u.replace(/\/\d+x\d+bb\./, '/600x600bb.') : '');

async function searchTitles(q) {
  const key = q.toLowerCase();
  if (searchCache.has(key)) return searchCache.get(key);
  const term = encodeURIComponent(q);
  const get = url => fetch(url, { signal: AbortSignal.timeout(5000) }).then(r => r.json()).then(j => j.results || []).catch(() => []);
  const [movies, seasons] = await Promise.all([
    get(`https://itunes.apple.com/search?term=${term}&country=fr&media=movie&entity=movie&limit=6`),
    get(`https://itunes.apple.com/search?term=${term}&country=fr&media=tvShow&entity=tvSeason&limit=10`),
  ]);
  const films = movies.map(r => ({
    title: r.trackName, year: (r.releaseDate || '').slice(0, 4), poster: bigArt(r.artworkUrl100), type: 'Film',
  }));
  const shows = [];
  const seen = new Set();
  for (const r of seasons) {
    const title = r.artistName || (r.collectionName || '').replace(/,?\s*(saison|season)\s*\d+.*$/i, '');
    if (!title || seen.has(title.toLowerCase())) continue;
    seen.add(title.toLowerCase());
    shows.push({ title, year: (r.releaseDate || '').slice(0, 4), poster: bigArt(r.artworkUrl100), type: 'Série' });
  }
  // on alterne films et séries
  const out = [];
  for (let i = 0; out.length < 8 && (i < films.length || i < shows.length); i++) {
    if (films[i]) out.push(films[i]);
    if (shows[i]) out.push(shows[i]);
  }
  searchCache.set(key, out);
  if (searchCache.size > 500) searchCache.delete(searchCache.keys().next().value);
  return out;
}

// ---------- HTTP ----------
const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, 'http://x');
  if (url.pathname === '/config') {
    const iceServers = [{ urls: ['stun:stun.l.google.com:19302', 'stun:stun1.l.google.com:19302'] }];
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
  if (url.pathname === '/search') {
    const q = (url.searchParams.get('q') || '').trim().slice(0, 80);
    const results = q.length >= 2 ? await searchTitles(q) : [];
    res.writeHead(200, { 'Content-Type': 'application/json' });
    return res.end(JSON.stringify(results));
  }
  if (url.pathname === '/health') {
    res.writeHead(200);
    return res.end('ok v3');
  }
  res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
  res.end(html);
});

// ---------- Salons ----------
// room = { peers:Set<ws>, films:[], phase:'idle'|'ranking'|'done', ranks:Map(id -> {name, order}), result, cleanup }
const rooms = new Map();
const json = o => JSON.stringify(o);
const send = (ws, o) => ws.readyState === 1 && ws.send(json(o));
const broadcast = (room, o) => room.peers.forEach(p => send(p, o));

function getRoom(name) {
  let room = rooms.get(name);
  if (!room) {
    room = { peers: new Set(), films: [], phase: 'idle', ranks: new Map(), result: null, cleanup: null };
    rooms.set(name, room);
  }
  clearTimeout(room.cleanup);
  return room;
}

function presence(room) {
  broadcast(room, {
    type: 'presence',
    people: [...room.peers].map(p => ({ id: p.id, name: p.name, mic: p.mic, sharing: p.sharing })),
  });
}

function filmsState(room) {
  broadcast(room, {
    type: 'films', films: room.films, phase: room.phase,
    done: [...room.ranks].map(([id, r]) => ({ id, name: r.name })), result: room.result,
  });
}

function resetRanking(room) {
  room.phase = 'idle';
  room.ranks.clear();
  room.result = null;
}

// Classement commun : méthode de Borda (1er = n-1 pts, dernier = 0), départage par meilleure place
function computeResult(room) {
  const n = room.films.length;
  const score = new Map(room.films.map(f => [f.id, { score: 0, best: n }]));
  const perPerson = [];
  for (const [id, { name, order }] of room.ranks) {
    order.forEach((fid, i) => {
      const s = score.get(fid);
      s.score += n - 1 - i;
      s.best = Math.min(s.best, i);
    });
    perPerson.push({ id, name, top: order.slice(0, 3) });
  }
  const top = [...score]
    .sort((a, b) => b[1].score - a[1].score || a[1].best - b[1].best)
    .slice(0, 3)
    .map(([id, s]) => ({ id, score: s.score }));
  return { top, perPerson, max: room.ranks.size * (n - 1) };
}

const wss = new WebSocketServer({ server, maxPayload: 256 * 1024 });

wss.on('connection', ws => {
  ws.id = crypto.randomUUID().slice(0, 8);
  ws.isAlive = true;
  ws.mic = true;
  ws.sharing = false;
  ws.on('pong', () => (ws.isAlive = true));

  ws.on('message', raw => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    if (msg.type === 'join') {
      const roomName = String(msg.room || '').trim().toLowerCase().slice(0, 50);
      if (!roomName || ws.room) return;
      const room = getRoom(roomName);
      if (room.peers.size >= 2) return send(ws, { type: 'full' });
      ws.name = String(msg.name || '').trim().slice(0, 24) || 'Invité';
      ws.room = room;
      room.peers.add(ws);
      send(ws, { type: 'joined', id: ws.id, version: 3 });
      presence(room);
      filmsState(room);
      room.peers.forEach(p => p !== ws && send(p, { type: 'system', text: `${ws.name} a rejoint le salon` }));
      if (room.peers.size === 2) {
        let i = 0;
        for (const p of room.peers) send(p, { type: 'ready', polite: i++ === 1 });
      }
      return;
    }

    const room = ws.room;
    if (!room) return;

    switch (msg.type) {
      case 'mic':
        ws.mic = !!msg.on;
        return presence(room);

      case 'chat': {
        const text = String(msg.text || '').trim().slice(0, 500);
        if (text) broadcast(room, { type: 'chat', id: ws.id, from: ws.name, text, ts: Date.now() });
        return;
      }

      case 'film-add': {
        const f = msg.film || {};
        const title = String(f.title || '').trim().slice(0, 100);
        if (!title || room.phase === 'ranking' || room.films.length >= MAX_FILMS) return;
        if (room.films.some(x => x.title.toLowerCase() === title.toLowerCase())) return;
        const poster = /^https:\/\/[\w.-]+\.mzstatic\.com\//.test(f.poster || '') ? f.poster : '';
        room.films.push({
          id: crypto.randomUUID().slice(0, 8), title, poster,
          year: String(f.year || '').slice(0, 4), type: String(f.type || '').slice(0, 10), by: ws.name,
        });
        if (room.phase === 'done') resetRanking(room);
        return filmsState(room);
      }

      case 'film-remove':
        if (room.phase === 'ranking') return;
        room.films = room.films.filter(f => f.id !== msg.id);
        if (room.phase === 'done') resetRanking(room);
        return filmsState(room);

      case 'rank-start':
        if (room.films.length < 3 || room.peers.size < 2) return;
        resetRanking(room);
        room.phase = 'ranking';
        broadcast(room, { type: 'rank-start', by: ws.name });
        return filmsState(room);

      case 'rank-done': {
        if (room.phase !== 'ranking') return;
        const ids = room.films.map(f => f.id);
        const order = Array.isArray(msg.order) ? msg.order.map(String) : [];
        if (order.length !== ids.length || new Set(order).size !== ids.length || !order.every(id => ids.includes(id))) return;
        room.ranks.set(ws.id, { name: ws.name, order });
        if (room.ranks.size >= 2) {
          room.result = computeResult(room);
          room.phase = 'done';
        }
        return filmsState(room);
      }

      case 'rank-cancel':
      case 'rank-reset':
        resetRanking(room);
        if (msg.type === 'rank-cancel') broadcast(room, { type: 'rank-cancel', by: ws.name });
        return filmsState(room);

      case 'screen':
      case 'screen-stop':
        ws.sharing = msg.type === 'screen';
        presence(room);
        break; // + relayé ci-dessous
    }

    // Signalisation WebRTC (et screen/screen-stop) : relayée à l'autre personne
    for (const p of room.peers) if (p !== ws && p.readyState === 1) p.send(raw.toString());
  });

  ws.on('close', () => {
    const room = ws.room;
    if (!room) return;
    room.peers.delete(ws);
    if (room.peers.size === 0) {
      // on garde la liste de films 30 min (rechargement de page, reconnexion…)
      room.cleanup = setTimeout(() => {
        for (const [k, r] of rooms) if (r === room) rooms.delete(k);
      }, 30 * 60 * 1000);
    } else {
      broadcast(room, { type: 'peer-left' });
      broadcast(room, { type: 'system', text: `${ws.name} est parti` });
      presence(room);
    }
  });
});

setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) { ws.terminate(); continue; }
    ws.isAlive = false;
    ws.ping();
  }
}, 25000);

server.listen(PORT, () => console.log(`Duo Watch en ligne sur le port ${PORT}`));
