'use strict';
const http = require('node:http');
const crypto = require('node:crypto');
const { WebSocketServer, WebSocket } = require('ws');
const definitions = require('./rooms.json');
const { UserStore } = require('./user_store.cjs');
const store = new UserStore();
const onlineUsers = new Map();
let resetting = false;
const adminKey = process.env.ADMIN_KEY || '';
function adminMatches(value) {
  if (!adminKey || typeof value !== 'string' || value.length > 256) return false;
  const digest = v => crypto.createHash('sha256').update(v).digest();
  return crypto.timingSafeEqual(digest(value), digest(adminKey));
}
async function refreshBuddies(p) {
  const buddies = await store.list(p.userId);
  send(p.ws, {t:'buddies', buddies:buddies.map(b => ({...b, online:onlineUsers.has(b.id)}))});
}
async function notifyFriends(p) {
  const friends = await store.list(p.userId);
  for (const f of friends) { const other=onlineUsers.get(f.id); if(other) await refreshBuddies(other); }
}
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
function broadcastPopulation() {
  const count = [...rooms.values()].reduce((total, room) => total + room.players.size, 0);
  const encoded = JSON.stringify({ t: 'population', count });
  for (const room of rooms.values()) for (const player of room.players.values()) send(player.ws, encoded);
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
function publicPlayer(p) { return { id: p.id, user_id: p.userId, name: p.name, s: p.state, typing: p.typing }; }
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
  let inbox = Promise.resolve(), queued = 0;
  ws.on('message', raw => {
    if (++queued > 64) { ws.close(1008, 'Too many queued messages'); return; }
    inbox = inbox.then(() => handle(raw)).catch(error => { console.error('Packet handling failed:', error.message); send(ws,{t:'social_notice',text:'Server could not save that change. Please try again.'}); }).finally(() => queued--);
  });
  async function handle(raw) {
    let message;
    try { message = JSON.parse(raw.toString()); } catch { ws.close(1008, 'Invalid JSON'); return; }
    if (!message || typeof message !== 'object') return;
    if (message.t === 'pong') { ws.alive = true; return; }
    if (resetting) { ws.close(1012,'Users resetting'); return; }
    if (ws.readyState !== WebSocket.OPEN) return;
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
      const identity = await store.login(message.token, name);
      if (ws.readyState !== WebSocket.OPEN || resetting) return;
      const previous = onlineUsers.get(identity.id);
      if (previous) previous.ws.close(1008,'Profile opened in another window');
      player = { id: crypto.randomUUID(), userId:identity.id, admin:false, name, ws, room, state: [...pos, 0, 0, 0], lastMove: Date.now(), typing: false, typingUntil: 0, rates: {} };
      clearTimeout(helloTimeout);
      send(ws, { t: 'welcome', id: player.id, user_id:identity.id, token:identity.token, name, self: pos, players: [...room.players.values()].map(publicPlayer) });
      room.players.set(player.id, player);
      onlineUsers.set(player.userId,player);
      broadcast(room, { t: 'join', player: publicPlayer(player) }, player);
      broadcastPopulation();
      await refreshBuddies(player);
      await notifyFriends(player);
      for (const request of await store.incoming(player.userId)) send(ws,{t:'buddy_request', ...request});
      return;
    }
    if (message.t === 'admin_auth') {
      if (!allow(player,'admin_auth',0.1,2)) return;
      player.admin=adminMatches(message.key);
      send(ws,{t:'admin_access',allowed:player.admin});
    } else if (message.t === 'admin_reset') {
      if (!player.admin || !allow(player,'admin_reset',0.5,3)) {send(ws,{t:'social_notice',text:'Administrator access required.'});return;}
      if (message.scope === 'buddies') {
        await store.clearBuddies();
        for (const p of onlineUsers.values()) {send(p.ws,{t:'requests_cleared'});await refreshBuddies(p);}
        send(ws,{t:'social_notice',text:'All buddies and pending requests cleared.'});
      } else if (message.scope === 'users') {
        resetting=true;
        try {
          await store.clearUsers();
          for (const p of onlineUsers.values()) {send(p.ws,{t:'identity_reset'});p.ws.close(1012,'User IDs reset');}
          onlineUsers.clear();
        } finally {resetting=false;}
      }
    } else if (message.t === 'buddies_get') {
      if (allow(player,'buddies_get',1,2)) await refreshBuddies(player);
    } else if (message.t === 'buddy_response') {
      if (!allow(player,'buddy_response',2,4) || typeof message.request_id !== 'string' || typeof message.accept !== 'boolean') return;
      const from=await store.respond(player.userId,message.request_id,message.accept);
      if (!from) {send(ws,{t:'social_notice',text:'That request is no longer pending.'});return;}
      await refreshBuddies(player);
      const sender=onlineUsers.get(from);
      if(sender) {await refreshBuddies(sender);send(sender.ws,{t:'social_notice',text:player.name+(message.accept?' accepted your buddy request.':' declined your buddy request.')});}
    } else if (message.t === 'buddy_remove') {
      if (!allow(player,'buddy_remove',2,4) || typeof message.user_id !== 'string') return;
      await store.remove(player.userId,message.user_id);
      await refreshBuddies(player);
      const other=onlineUsers.get(message.user_id);if(other) await refreshBuddies(other);
    } else if (message.t === 'buddy_request') {
      if (!allow(player, 'buddy', 0.2, 2) || typeof message.target !== 'string') return;
      const recipient = player.room.players.get(message.target);
      if (!recipient || recipient === player) return;
      const request=await store.request(player.userId,recipient.userId);
      if(request.error) {send(ws,{t:'social_notice',text:request.error});return;}
      send(recipient.ws,{t:'buddy_request',...request,name:player.name});
      send(ws,{t:'social_notice',text:'Buddy request sent to '+recipient.name+'.'});
    } else if (message.t === 'rename') {
      if (!allow(player, 'rename', 1, 3)) {
        send(ws, { t: 'renamed_self', name: player.name });
        return;
      }
      if (typeof message.name !== 'string') return;
      const name = [...message.name.replace(/[\u0000-\u001f\u007f]/g, '').trim()].slice(0, 24).join('');
      if (!name) { send(ws, { t: 'renamed_self', name: player.name }); return; }
      await store.rename(player.userId,name);
      player.name = name;
      send(ws, { t: 'renamed_self', name });
      broadcast(player.room, { t: 'renamed', id: player.id, name }, player);
      await notifyFriends(player);
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
  }
  ws.on('error', () => {});
  ws.on('close', () => {
    clearTimeout(helloTimeout);
    if (!player) return;
    player.room.players.delete(player.id);
    player.room.dirty.delete(player.id);
    broadcast(player.room, { t: 'leave', id: player.id });
    broadcastPopulation();
    if (onlineUsers.get(player.userId) === player) {
      onlineUsers.delete(player.userId);
      notifyFriends(player).catch(error => console.error('Presence update failed:',error.message));
    }
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
store.open().then(() => server.listen(Number(process.env.PORT || 10000), '0.0.0.0', () => console.log(`MiniPlanet listening on ${server.address().port} (${store.kind})`))).catch(error => {console.error('Database startup failed:',error.message);process.exit(1);});
function shutdown() {
  clearInterval(tick); clearInterval(heartbeat);
  for (const ws of wss.clients) ws.close(1001, 'Server restarting');
  server.close(async () => {await store.close();process.exit(0);});
  setTimeout(() => process.exit(0), 2000).unref();
}
process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);
