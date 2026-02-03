// server.js — Dominia Relay (Render) — Host-authoritative, rooms, state sync
// Compatible BOTH protocols:
// 1) Canon protocol (recommended): hello + host/join + get_state + patch + state
// 2) Legacy client protocol: ?room=XXXXXX&role=host|join + need_state + action

import http from "http";
import { WebSocketServer } from "ws";
import { URL } from "url";

const PORT = Number(process.env.PORT || 8080);
const MAX_BYTES = 64 * 1024;

// Room model:
// rooms: code -> { hostId, hostWs, peers: Map<clientId, ws>, lastState, updatedAt }
// clients: ws -> { clientId, roomCode, mode, lastSeen }
const rooms = new Map();
const clients = new Map();

const ROOM_TTL_MS = 15 * 60 * 1000; // 15 min idle => cleanup
const HEARTBEAT_MS = 25 * 1000;

function now() {
  return Date.now();
}

function norm(code) {
  return String(code || "")
    .trim()
    .toUpperCase()
    .replace(/[^A-Z0-9]/g, "")
    .slice(0, 6);
}

function safeJsonParse(str) {
  try {
    return JSON.parse(str);
  } catch {
    return null;
  }
}

function send(ws, obj) {
  try {
    if (ws && ws.readyState === 1) ws.send(JSON.stringify(obj));
  } catch {}
}

function touchRoom(code) {
  const r = rooms.get(code);
  if (r) r.updatedAt = now();
}

function peerStatus(code) {
  const r = rooms.get(code);
  if (!r) return;
  const connected = r.peers.size > 0;
  broadcast(code, { t: "peer_status", connected });
}

function broadcast(code, obj) {
  const r = rooms.get(code);
  if (!r) return;
  if (r.hostWs) send(r.hostWs, obj);
  for (const ws of r.peers.values()) send(ws, obj);
}

function getClient(ws) {
  return clients.get(ws) || null;
}

function ensureClient(ws, maybeClientId) {
  let c = clients.get(ws);
  if (c?.clientId) return c;

  // if hello not provided (legacy), generate a stable session id for this socket
  const clientId =
    String(maybeClientId || "").trim() ||
    "c_" + Math.random().toString(16).slice(2) + now().toString(16);

  c = { clientId, roomCode: "", mode: "off", lastSeen: now() };
  clients.set(ws, c);
  return c;
}

function leaveRoom(ws, explicitCode) {
  const c = getClient(ws);
  const code = norm(explicitCode || c?.roomCode);
  if (!code) return;

  const r = rooms.get(code);
  if (!r) {
    if (c) clients.set(ws, { ...c, roomCode: "", mode: "off", lastSeen: now() });
    return;
  }

  if (c && r.hostId === c.clientId) {
    r.hostId = "";
    r.hostWs = null;
  } else if (c) {
    r.peers.delete(c.clientId);
  }

  if (c) clients.set(ws, { ...c, roomCode: "", mode: "off", lastSeen: now() });

  // cleanup empty rooms quickly (no host + no peers)
  if (!r.hostWs && r.peers.size === 0) {
    rooms.delete(code);
  } else {
    touchRoom(code);
    peerStatus(code);
  }
}

function joinAsHost(ws, roomCode) {
  const c = ensureClient(ws);
  const code = norm(roomCode);
  if (code.length !== 6) {
    send(ws, { t: "status", payload: { ok: false, error: "invalid_roomCode" } });
    return;
  }

  const r =
    rooms.get(code) || {
      hostId: "",
      hostWs: null,
      peers: new Map(),
      lastState: null,
      updatedAt: now(),
    };

  r.hostId = c.clientId;
  r.hostWs = ws;
  r.updatedAt = now();
  rooms.set(code, r);

  clients.set(ws, { ...c, roomCode: code, mode: "host", lastSeen: now() });

  send(ws, { t: "host_ok", roomCode: code, hostId: c.clientId });
  if (r.lastState) send(ws, { t: "state", payload: r.lastState });

  peerStatus(code);
  touchRoom(code);
}

function joinAsPeer(ws, roomCode) {
  const c = ensureClient(ws);
  const code = norm(roomCode);
  if (code.length !== 6) {
    send(ws, { t: "status", payload: { ok: false, error: "invalid_roomCode" } });
    return;
  }

  const r = rooms.get(code);
  if (!r) {
    send(ws, { t: "status", payload: { ok: false, error: "room_not_found" } });
    return;
  }

  r.peers.set(c.clientId, ws);
  clients.set(ws, { ...c, roomCode: code, mode: "join", lastSeen: now() });

  send(ws, { t: "join_ok", roomCode: code });

  // If state exists, push immediately
  if (r.lastState) send(ws, { t: "state", payload: r.lastState });

  peerStatus(code);
  touchRoom(code);
}

// ---- HTTP endpoints (same process as WS) ----
const server = http.createServer((req, res) => {
  const u = new URL(req.url || "/", "http://localhost");

  if (u.pathname === "/" || u.pathname === "/health") {
    res.writeHead(200, { "content-type": "application/json" });
    res.end(
      JSON.stringify({
        ok: true,
        rooms: rooms.size,
        clients: clients.size,
        ts: now(),
      })
    );
    return;
  }

  if (u.pathname === "/debug") {
    const out = [];
    for (const [code, r] of rooms.entries()) {
      out.push({
        code,
        hasHost: !!r.hostWs,
        peers: r.peers.size,
        hasState: !!r.lastState,
        updatedAt: r.updatedAt,
      });
    }
    res.writeHead(200, { "content-type": "application/json" });
    res.end(
      JSON.stringify({
        ok: true,
        rooms: out,
        roomsCount: rooms.size,
        clients: clients.size,
        uptimeSec: Math.floor(process.uptime()),
        ts: now(),
      })
    );
    return;
  }

  res.writeHead(404);
  res.end();
});

// ---- WS server ----
const wss = new WebSocketServer({ server, clientTracking: false });

wss.on("connection", (ws, req) => {
  // heartbeat
  ws.isAlive = true;
  ws.on("pong", () => {
    ws.isAlive = true;
    const c = getClient(ws);
    if (c) clients.set(ws, { ...c, lastSeen: now() });
  });

  // ---- Legacy auto-join via query params (?room=XXXXXX&role=host|join) ----
  // Your current client uses this.
  try {
    const u = new URL(req.url || "/", "http://localhost");
    const qRoom = norm(u.searchParams.get("room"));
    const qRole = String(u.searchParams.get("role") || "").toLowerCase();

    // Create client if not exists
    ensureClient(ws);

    if (qRoom.length === 6 && (qRole === "host" || qRole === "join")) {
      if (qRole === "host") joinAsHost(ws, qRoom);
      else joinAsPeer(ws, qRoom);

      // For legacy join: proactively ask state from host
      if (qRole === "join") {
        const r = rooms.get(qRoom);
        if (r?.hostWs) send(r.hostWs, { t: "get_state", from: getClient(ws)?.clientId || "" });
      }
    }
  } catch {}

  ws.on("message", (buf) => {
    if (!buf || buf.length > MAX_BYTES) {
      send(ws, { t: "status", payload: { ok: false, error: "payload_too_large" } });
      try {
        ws.close();
      } catch {}
      return;
    }

    const msg = safeJsonParse(buf.toString());
    if (!msg || typeof msg !== "object") return;

    // ---- Normalize legacy aliases to canonical protocol ----
    // legacy: {t:"need_state"} => canonical: get_state
    // legacy: {t:"action", payload} => canonical: patch
    const t = String(msg.t || "").trim();

    // Ensure client existence even for legacy clients (no hello)
    if (t !== "hello") ensureClient(ws);

    // ---- Canon: hello ----
    if (t === "hello") {
      const clientId = String(msg.clientId || "").trim();
      if (!clientId) {
        send(ws, { t: "status", payload: { ok: false, error: "missing_clientId" } });
        return;
      }
      clients.set(ws, { clientId, roomCode: "", mode: "off", lastSeen: now() });
      send(ws, { t: "status", payload: { ok: true } });
      return;
    }

    const c = getClient(ws);
    if (!c || !c.clientId) return;

    // ---- Canon: host/join/leave ----
    if (t === "host") {
      joinAsHost(ws, msg.roomCode);
      return;
    }
    if (t === "join") {
      joinAsPeer(ws, msg.roomCode);
      return;
    }
    if (t === "leave") {
      leaveRoom(ws, msg.roomCode || c.roomCode);
      return;
    }

    // ---- Canon: patch (join -> host) ----
    // legacy alias: action -> patch
    if (t === "patch" || t === "action") {
      const code = norm(msg.roomCode || c.roomCode);
      const r = rooms.get(code);
      if (!r) return;

      // Only forward to host
      if (r.hostWs) {
        send(r.hostWs, {
          t: "patch",
          from: c.clientId,
          patch: t === "patch" ? msg.patch : msg.payload,
        });
      }
      touchRoom(code);
      return;
    }

    // ---- Canon: state (host-only) ----
    if (t === "state") {
      const code = norm(msg.roomCode || c.roomCode);
      const r = rooms.get(code);
      if (!r) return;

      // Only host may publish state
      if (r.hostId !== c.clientId) return;

      r.lastState = msg.payload || null;
      r.updatedAt = now();

      broadcast(code, { t: "state", payload: r.lastState });
      touchRoom(code);
      return;
    }

    // ---- Canon: get_state (join -> host) ----
    // legacy alias: need_state -> get_state
    if (t === "get_state" || t === "need_state") {
      const code = norm(msg.roomCode || c.roomCode);
      const r = rooms.get(code);
      if (!r) return;

      // Ask host to send a fresh state; if no host, fallback to cached lastState.
      if (r.hostWs) {
        send(r.hostWs, { t: "get_state", from: c.clientId });
      } else if (r.lastState) {
        send(ws, { t: "state", payload: r.lastState });
      }
      touchRoom(code);
      return;
    }

    // ---- Optional event channel (kept from your existing server) ----
    if (t === "event") {
      const code = norm(msg.roomCode || c.roomCode);
      const r = rooms.get(code);
      if (!r) return;

      if (r.hostId === c.clientId) {
        broadcast(code, { t: "event", event: msg.event });
      } else if (r.hostWs) {
        send(r.hostWs, { t: "event", from: c.clientId, event: msg.event });
      }

      touchRoom(code);
      return;
    }
  });

  ws.on("close", () => {
    const c = getClient(ws);
    if (!c) return;
    leaveRoom(ws, c.roomCode);
    clients.delete(ws);
  });

  ws.on("error", () => {
    // Avoid noisy logs; cleanup on close will handle state
  });
});

// ---- Heartbeat + Cleanup ----
setInterval(() => {
  const ts = now();

  // Ping clients
  for (const ws of clients.keys()) {
    if (ws.isAlive === false) {
      try {
        ws.terminate();
      } catch {}
      continue;
    }
    ws.isAlive = false;
    try {
      ws.ping();
    } catch {}
  }

  // Cleanup stale rooms (no activity)
  for (const [code, r] of rooms.entries()) {
    const idle = ts - (r.updatedAt || ts);
    const hasAnyone = !!r.hostWs || r.peers.size > 0;

    // also prune dead peers
    for (const [id, pws] of r.peers.entries()) {
      if (!pws || pws.readyState !== 1) r.peers.delete(id);
    }
    if (r.hostWs && r.hostWs.readyState !== 1) {
      r.hostWs = null;
      r.hostId = "";
    }

    if (!hasAnyone || (!r.hostWs && r.peers.size === 0) || idle > ROOM_TTL_MS) {
      rooms.delete(code);
    } else {
      // update if we pruned someone
      r.updatedAt = r.updatedAt || ts;
    }
  }
}, HEARTBEAT_MS);

server.listen(PORT, () => {
  console.log("Dominia Relay listening on :" + PORT);
});
