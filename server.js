import http from "http";
import { WebSocketServer } from "ws";

const PORT = Number(process.env.PORT || 8080);
const MAX_BYTES = 65536;

const rooms = new Map(); // code -> { hostId, hostWs, peers: Map, lastState, updatedAt }
const clients = new Map(); // ws -> { clientId, roomCode, mode }

function now() { return Date.now(); }

function send(ws, obj) {
  try { if (ws && ws.readyState === 1) ws.send(JSON.stringify(obj)); } catch {}
}

function broadcast(code, obj) {
  const r = rooms.get(code);
  if (!r) return;
  if (r.hostWs) send(r.hostWs, obj);
  for (const ws of r.peers.values()) send(ws, obj);
}

function norm(code) {
  return String(code || "")
    .trim()
    .toUpperCase()
    .replace(/[^A-Z0-9]/g, "")
    .slice(0, 6);
}

function touch(code) {
  const r = rooms.get(code);
  if (r) r.updatedAt = now();
}

function peerStatus(code) {
  const r = rooms.get(code);
  const connected = !!(r && r.peers.size > 0);
  broadcast(code, { t: "peer_status", connected });
}

const server = http.createServer((req, res) => {
  if (req.url === "/") {
    res.writeHead(200, { "content-type": "application/json" });
    res.end(JSON.stringify({ ok: true, rooms: rooms.size, ts: now() }));
    return;
  }
  res.writeHead(404);
  res.end();
});

const wss = new WebSocketServer({ server, clientTracking: false });

wss.on("connection", (ws) => {
  ws.on("message", (buf) => {
    if (!buf || buf.length > MAX_BYTES) {
      send(ws, { t: "status", payload: { ok: false, error: "payload_too_large" } });
      try { ws.close(); } catch {}
      return;
    }

    let msg;
    try { msg = JSON.parse(buf.toString()); } catch { return; }
    if (!msg || !msg.t) return;

    if (msg.t === "hello") {
      const clientId = String(msg.clientId || "").trim();
      if (!clientId) return send(ws, { t: "status", payload: { ok: false, error: "missing_clientId" } });
      clients.set(ws, { clientId, roomCode: "", mode: "off" });
      return send(ws, { t: "status", payload: { ok: true } });
    }

    const c = clients.get(ws);
    if (!c || !c.clientId) return;

    if (msg.t === "host") {
      const code = norm(msg.roomCode);
      if (code.length !== 6) return send(ws, { t: "status", payload: { ok: false, error: "invalid_roomCode" } });

      const r = rooms.get(code) || { hostId: "", hostWs: null, peers: new Map(), lastState: null, updatedAt: now() };
      r.hostId = c.clientId;
      r.hostWs = ws;
      r.updatedAt = now();
      rooms.set(code, r);

      clients.set(ws, { ...c, roomCode: code, mode: "host" });
      send(ws, { t: "host_ok", roomCode: code, hostId: c.clientId });
      if (r.lastState) send(ws, { t: "state", payload: r.lastState });
      peerStatus(code);
      touch(code);
      return;
    }

    if (msg.t === "join") {
      const code = norm(msg.roomCode);
      const r = rooms.get(code);
      if (!r) return send(ws, { t: "status", payload: { ok: false, error: "room_not_found" } });

      r.peers.set(c.clientId, ws);
      clients.set(ws, { ...c, roomCode: code, mode: "join" });

      send(ws, { t: "join_ok", roomCode: code });
      if (r.lastState) send(ws, { t: "state", payload: r.lastState });
      peerStatus(code);
      touch(code);
      return;
    }

    if (msg.t === "leave") {
      const code = norm(msg.roomCode || c.roomCode);
      const r = rooms.get(code);
      if (!r) return;

      if (r.hostId === c.clientId) { r.hostId = ""; r.hostWs = null; }
      else r.peers.delete(c.clientId);

      clients.set(ws, { ...c, roomCode: "", mode: "off" });
      peerStatus(code);
      touch(code);
      return;
    }

    if (msg.t === "patch") {
      const code = norm(msg.roomCode || c.roomCode);
      const r = rooms.get(code);
      if (!r) return;
      if (r.hostWs) send(r.hostWs, { t: "patch", from: c.clientId, patch: msg.patch });
      touch(code);
      return;
    }

    if (msg.t === "state") {
      const code = norm(msg.roomCode || c.roomCode);
      const r = rooms.get(code);
      if (!r) return;
      if (r.hostId !== c.clientId) return;
      r.lastState = msg.payload || null;
      r.updatedAt = now();
      broadcast(code, { t: "state", payload: r.lastState });
      touch(code);
      return;
    }

    if (msg.t === "get_state") {
      const code = norm(msg.roomCode || c.roomCode);
      const r = rooms.get(code);
      if (!r) return;
      if (r.hostWs) send(r.hostWs, { t: "get_state", from: c.clientId });
      else if (r.lastState) send(ws, { t: "state", payload: r.lastState });
      touch(code);
      return;
    }

    if (msg.t === "event") {
      const code = norm(msg.roomCode || c.roomCode);
      const r = rooms.get(code);
      if (!r) return;

      if (r.hostId === c.clientId) broadcast(code, { t: "event", event: msg.event });
      else if (r.hostWs) send(r.hostWs, { t: "event", from: c.clientId, event: msg.event });

      touch(code);
      return;
    }
  });

  ws.on("close", () => {
    const c = clients.get(ws);
    if (!c) return;

    const code = c.roomCode;
    const r = rooms.get(code);

    if (r) {
      if (r.hostId === c.clientId) { r.hostId = ""; r.hostWs = null; }
      else r.peers.delete(c.clientId);
      peerStatus(code);
      touch(code);
    }

    clients.delete(ws);
  });
});

server.listen(PORT, () => {
  console.log("Dominia Relay listening on :" + PORT);
});
