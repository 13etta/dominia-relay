import http from "http";
import { WebSocketServer } from "ws";

const PORT = Number(process.env.PORT || 8080);

// Anti-crash / anti-flood
const MAX_BYTES = 256_000;
const ROOM_TTL_MS = 1000 * 60 * 60 * 6; // 6h
const GC_INTERVAL_MS = 60_000;

const rooms = new Map(); // roomCode -> { hostWs, joins:Set<ws>, lastState, createdAt, updatedAt, needStateCount }
const clients = new Map(); // ws -> { roomCode, role, connectedAt }

const now = () => Date.now();

function safeJsonParse(buf) {
  try {
    const s = Buffer.isBuffer(buf) ? buf.toString("utf8") : String(buf);
    return JSON.parse(s);
  } catch {
    return null;
  }
}

function send(ws, obj) {
  try {
    if (ws && ws.readyState === 1) ws.send(JSON.stringify(obj));
  } catch {}
}

function roomStats(roomCode) {
  const r = rooms.get(roomCode);
  if (!r) return null;
  return {
    room: roomCode,
    hasHost: !!r.hostWs,
    joins: r.joins.size,
    updatedAt: r.updatedAt,
    needStateCount: r.needStateCount,
  };
}

function broadcastToJoins(roomCode, obj) {
  const r = rooms.get(roomCode);
  if (!r) return;
  for (const ws of r.joins) send(ws, obj);
}

function cleanupRoomIfEmpty(roomCode) {
  const r = rooms.get(roomCode);
  if (!r) return;
  const hasHost = !!r.hostWs && r.hostWs.readyState === 1;
  const joinsAlive = [...r.joins].some((ws) => ws.readyState === 1);
  if (!hasHost && !joinsAlive) rooms.delete(roomCode);
}

function normalizeRoomCode(v) {
  return String(v || "")
    .trim()
    .toUpperCase()
    .replace(/[^A-Z0-9]/g, "")
    .slice(0, 6);
}

function parseQuery(url) {
  const out = {};
  const q = String(url || "").split("?")[1] || "";
  for (const part of q.split("&")) {
    if (!part) continue;
    const [k, v] = part.split("=");
    out[decodeURIComponent(k || "")] = decodeURIComponent(v || "");
  }
  return out;
}

const server = http.createServer((req, res) => {
  if (req.url === "/" || req.url?.startsWith("/?")) {
    // Health + stats
    let clientsCount = 0;
    for (const ws of clients.keys()) if (ws.readyState === 1) clientsCount++;

    res.writeHead(200, { "content-type": "application/json" });
    res.end(JSON.stringify({ ok: true, rooms: rooms.size, clients: clientsCount, ts: now() }));
    return;
  }

  if (req.url === "/rooms") {
    const list = [];
    for (const [code] of rooms) {
      const st = roomStats(code);
      if (st) list.push(st);
    }
    res.writeHead(200, { "content-type": "application/json" });
    res.end(JSON.stringify({ ok: true, list, ts: now() }));
    return;
  }

  res.writeHead(404);
  res.end();
});

const wss = new WebSocketServer({ server, clientTracking: false });

wss.on("connection", (ws, req) => {
  const q = parseQuery(req?.url || "");
  const roomCode = normalizeRoomCode(q.room);
  const role = String(q.role || "").toLowerCase(); // "host" | "join"

  if (roomCode.length !== 6 || (role !== "host" && role !== "join")) {
    send(ws, { t: "status", status: "error", message: "Invalid room/role in query params." });
    try { ws.close(); } catch {}
    return;
  }

  // Ensure room exists
  let r = rooms.get(roomCode);
  if (!r) {
    r = { hostWs: null, joins: new Set(), lastState: null, createdAt: now(), updatedAt: now(), needStateCount: 0 };
    rooms.set(roomCode, r);
  }

  clients.set(ws, { roomCode, role, connectedAt: now() });
  r.updatedAt = now();

  // Attach client
  if (role === "host") {
    // Replace existing host (last one wins)
    r.hostWs = ws;
    send(ws, { t: "status", status: "connected", room: roomCode, role });
    // If we already have lastState, send it back to host too (safe)
    if (r.lastState) send(ws, { t: "state", payload: r.lastState });
  } else {
    r.joins.add(ws);
    send(ws, { t: "status", status: "connected", room: roomCode, role });

    // If we have state, give it immediately. Otherwise request from host.
    if (r.lastState) {
      send(ws, { t: "state", payload: r.lastState });
    } else if (r.hostWs && r.hostWs.readyState === 1) {
      r.needStateCount += 1;
      send(r.hostWs, { t: "need_state", count: r.needStateCount, room: roomCode });
    }
  }

  // Receive messages
  ws.on("message", (buf) => {
    if (!buf || buf.length > MAX_BYTES) {
      send(ws, { t: "status", status: "error", message: "payload_too_large" });
      try { ws.close(); } catch {}
      return;
    }

    const msg = safeJsonParse(buf);
    if (!msg || typeof msg !== "object") return;

    const c = clients.get(ws);
    if (!c) return;

    const rr = rooms.get(c.roomCode);
    if (!rr) return;

    rr.updatedAt = now();

    // Host publishes canonical state
    if (msg.t === "state") {
      if (c.role !== "host") return;
      rr.lastState = msg.payload || null;
      rr.updatedAt = now();
      broadcastToJoins(c.roomCode, { t: "state", payload: rr.lastState });
      return;
    }

    // Join sends an action -> forwarded to host
    if (msg.t === "action") {
      if (c.role !== "join") return;
      if (rr.hostWs && rr.hostWs.readyState === 1) {
        send(rr.hostWs, { t: "action", payload: msg.payload || null });
      } else if (rr.lastState) {
        // No host but we can at least send cached state back (best-effort)
        send(ws, { t: "state", payload: rr.lastState });
      }
      return;
    }

    // Join can request state explicitly
    if (msg.t === "need_state") {
      if (c.role !== "join") return;
      if (rr.lastState) {
        send(ws, { t: "state", payload: rr.lastState });
      } else if (rr.hostWs && rr.hostWs.readyState === 1) {
        rr.needStateCount += 1;
        send(rr.hostWs, { t: "need_state", count: rr.needStateCount, room: c.roomCode });
      }
      return;
    }
  });

  ws.on("close", () => {
    const c = clients.get(ws);
    clients.delete(ws);

    if (!c) return;
    const rr = rooms.get(c.roomCode);
    if (!rr) return;

    if (c.role === "host") {
      if (rr.hostWs === ws) rr.hostWs = null;
      // Ask remaining joins to request state when a new host connects (optional)
    } else {
      rr.joins.delete(ws);
    }

    rr.updatedAt = now();
    cleanupRoomIfEmpty(c.roomCode);
  });
});

// GC
setInterval(() => {
  const t = now();
  for (const [code, r] of rooms) {
    const idle = t - r.updatedAt;
    if (idle > ROOM_TTL_MS) rooms.delete(code);
  }
}, GC_INTERVAL_MS);

server.listen(PORT, () => {
  console.log("[DOMINIA] Relay listening on :" + PORT);
});
