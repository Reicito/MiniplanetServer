'use strict';
const http = require('node:http');
const crypto = require('node:crypto');
const { WebSocketServer, WebSocket } = require('ws');
const definitions = require('./rooms.json');
const rooms = new Map(Object.entries(definitions).map(([id, config]) => [id, { config, players: new Map(), dirty: new Map() }]));
const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' });
  res.end(JSON.stringify({ ok: true, players: [...rooms.values()].reduce((n, r) => n + r.players.size, 0) }));
});
const wss = new WebSocketServer({ server, path: '/ws', maxPayload: 4096, perMessageDeflate: false });
function send(ws, data) {
  if (ws.readyState !== WebSocket.OPEN) return;
  if (ws.bufferedAmount > 262144) { ws.close(1013, 'Slow connection'); return; }
  ws.send(typeof data === 'string' ? data : JSON.stringify(data));
}
function broadcast(room, data, except) {
  const encoded = JSON.stringify(data);
  for (const player of room.players.values()) if (player !== except) send(player.ws, encoded);
}
function inside(point, polygon) {
  if (!Array.isArray(point) || point.length !== 2 || !point.every(Number.isFinite)) return false;
  let result = false;
  for (let i = 0, j = polygon.length - 1; i < polygon.length; j = i++) {
    const [xi, yi] = polygon[i], [xj, yj] = polygon[j];
    if ((yi > point[1]) !== (yj > point[1]) && point[0] < (xj - xi) * (point[1] - yi) / (yj - yi) + xi) result = !result;
  }
  return result;
}
function publicPlayer(p) { return { id: p.id, name: p.name, s: p.state, typing: p.typing }; }
function typing(p, active) {
  p.typingUntil = Date.now() + 4000;
  if (p.typing === active) return;
  p.typing = active;
  broadcast(p.room, { t: 'typing', id: p.id, active }, p);
}
function allow(p, kind, rate, burst) {
  const now = Date.now();
  const bucket = p.rates[kind] || { tokens: burst, at: now };
  bucket.tokens = Math.min(burst, bucket.tokens + (now - bucket.at) * rate / 1000);
  bucket.at = now;
  p.rates[kind] = bucket;
  if (bucket.tokens < 1) return false;
  bucket.tokens--;
  return true;
}
wss.on('connection', ws => {
  let player;
  ws.alive = true;
  ws.on('pong', () => { ws.alive = true; });
  const helloTimeout = setTimeout(() => { if (!player) ws.close(1008, 'Hello required'); }, 10000);
  ws.on('message', raw => {
    let message;
    try { message = JSON.parse(raw.toString()); } catch { ws.close(1008, 'Invalid JSON'); return; }
    if (!message || typeof message !== 'object') return;
    if (message.t === 'pong') { ws.alive = true; return; }
    if (!player) {
      if (message.t !== 'hello' || message.v !== 1) { ws.close(1008, 'Protocol version'); return; }
      const room = rooms.get(message.room);
      if (!room || room.players.size >= room.config.capacity) { ws.close(1008, 'Room unavailable'); return; }
      let pos = inside(message.pos, room.config.polygon) ? message.pos.slice() : room.config.spawn.slice();
      // Separate coincident spawns so a second tester is immediately visible.
      for (let i = 0; i < 20 && [...room.players.values()].some(p => Math.hypot(p.state[0] - pos[0], p.state[1] - pos[1]) < 18); i++) {
        const candidate = [room.config.spawn[0] + 28 * ((i % 9) - 4), room.config.spawn[1] + 26 * Math.floor(i / 9)];
        if (inside(candidate, room.config.polygon)) pos = candidate;
      }
      const name = String(message.name || 'Guest').replace(/[\u0000-\u001f\u007f]/g, '').slice(0, 24) || 'Guest';
      player = { id: crypto.randomUUID(), name, ws, room, state: [...pos, 0, 0, 0], lastMove: Date.now(), typing: false, typingUntil: 0, rates: {} };
      clearTimeout(helloTimeout);
      send(ws, { t: 'welcome', id: player.id, name, self: pos, players: [...room.players.values()].map(publicPlayer) });
      room.players.set(player.id, player);
      broadcast(room, { t: 'join', player: publicPlayer(player) }, player);
      return;
    }
    if (message.t === 'rename') {
      if (!allow(player, 'rename', 1, 3)) {
        send(ws, { t: 'renamed_self', name: player.name });
        return;
      }
      if (typeof message.name !== 'string') return;
      const name = [...message.name.replace(/[\u0000-\u001f\u007f]/g, '').trim()].slice(0, 24).join('');
      if (!name) { send(ws, { t: 'renamed_self', name: player.name }); return; }
      player.name = name;
      send(ws, { t: 'renamed_self', name });
      broadcast(player.room, { t: 'renamed', id: player.id, name }, player);
    } else if (message.t === 'move' && allow(player, 'move', 30, 30)) {
      const s = message.s;
      if (!Array.isArray(s) || s.length !== 5 || !s.every(Number.isFinite) || !Number.isInteger(s[4]) || s[4] < 0 || s[4] > 7) return;
      const now = Date.now(), config = player.room.config;
      const elapsed = Math.min((now - player.lastMove) / 1000, 1);
      const distance = Math.hypot(s[0] - player.state[0], s[1] - player.state[1]);
      if (!inside(s.slice(0, 2), config.polygon) || Math.hypot(s[2], s[3]) > config.maxSpeed + 1 || distance > config.maxSpeed * elapsed + 24) {
        send(ws, { t: 'correct', p: player.state.slice(0, 2) });
        return;
      }
      player.lastMove = now;
      player.state = s;
      player.room.dirty.set(player.id, [player.id, ...s]);
    } else if (message.t === 'chat' && allow(player, 'chat', 6, 6)) {
      if (typeof message.text !== 'string') return;
      const text = [...message.text.trim()].slice(0, 200).join('');
      if (!text) return;
      typing(player, false);
      broadcast(player.room, { t: 'chat', id: player.id, text }, player);
    } else if (message.t === 'typing' && allow(player, 'typing', 4, 4)) {
      if (typeof message.active === 'boolean') typing(player, message.active);
    }
  });
  ws.on('error', () => {});
  ws.on('close', () => {
    clearTimeout(helloTimeout);
    if (!player) return;
    player.room.players.delete(player.id);
    player.room.dirty.delete(player.id);
    broadcast(player.room, { t: 'leave', id: player.id });
  });
});
const tick = setInterval(() => {
  for (const room of rooms.values()) {
    for (const p of room.players.values()) if (p.typing && Date.now() > p.typingUntil) typing(p, false);
    if (room.dirty.size) { broadcast(room, { t: 'states', s: [...room.dirty.values()] }); room.dirty.clear(); }
  }
}, 50);
const heartbeat = setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.alive) { ws.terminate(); continue; }
    ws.alive = false;
    ws.ping();
    send(ws, { t: 'ping' });
  }
}, 20000);
server.listen(Number(process.env.PORT || 10000), '0.0.0.0', () => console.log(`MiniPlanet listening on ${server.address().port}`));
function shutdown() {
  clearInterval(tick); clearInterval(heartbeat);
  for (const ws of wss.clients) ws.close(1001, 'Server restarting');
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(0), 2000).unref();
}
process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);
