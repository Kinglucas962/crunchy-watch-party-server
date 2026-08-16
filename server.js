import http from "node:http";
import { pathToFileURL } from "node:url";
import express from "express";
import { createHash, randomUUID, timingSafeEqual } from "node:crypto";
import { WebSocket, WebSocketServer } from "ws";

const PORT = process.env.PORT === undefined ? 8080 : Number(process.env.PORT);
const RECONNECT_GRACE_MS = 15000;
const MAX_CHAT_MESSAGES = 100;
const MAX_CUSTOM_PHOTO_LENGTH = 100000;
const MAX_SOCKET_PAYLOAD = 512 * 1024;
const MAX_BUFFERED_BYTES = 1024 * 1024;
const ALLOWED_ORIGINS = new Set(
  String(process.env.ALLOWED_ORIGINS || "")
    .split(",")
    .map((origin) => origin.trim())
    .filter(Boolean)
);
const rooms = new Map();

const RATE_LIMITS = {
  JOIN_ROOM: { limit: 6, windowMs: 30000 },
  CHAT_MESSAGE: { limit: 8, windowMs: 10000 },
  REACTION: { limit: 15, windowMs: 10000 },
  PLAYER_STATE: { limit: 40, windowMs: 5000 },
  NAVIGATE: { limit: 10, windowMs: 30000 },
  UPDATE_PROFILE: { limit: 5, windowMs: 60000 },
  SET_CONTROL_MODE: { limit: 10, windowMs: 60000 }
};

function hashToken(token) {
  return createHash("sha256").update(token).digest();
}

function tokenMatches(token, expectedHash) {
  if (!token || !expectedHash) return false;
  const receivedHash = hashToken(String(token));
  return receivedHash.length === expectedHash.length
    && timingSafeEqual(receivedHash, expectedHash);
}

function createSessionToken() {
  return `${randomUUID()}${randomUUID()}`.replaceAll("-", "");
}

function sanitizeCustomPhoto(avatar, value) {
  const photo = String(value || "");
  return avatar === "custom"
    && /^data:image\/(?:png|jpeg|webp|gif);base64,/i.test(photo)
    && photo.length <= MAX_CUSTOM_PHOTO_LENGTH
      ? photo
      : "";
}

export function isAllowedCrunchyrollUrl(value) {
  try {
    const url = new URL(String(value));
    return url.protocol === "https:"
      && (url.hostname === "crunchyroll.com" || url.hostname.endsWith(".crunchyroll.com"));
  } catch {
    return false;
  }
}

export function sanitizePlayerState(value) {
  if (!value || typeof value !== "object" || Array.isArray(value)) return null;

  let serialized;
  try {
    serialized = JSON.stringify(value);
  } catch {
    return null;
  }
  if (serialized.length > 8192 || Object.keys(value).length > 24) return null;

  const sanitized = {};
  for (const [key, item] of Object.entries(value)) {
    if (!/^[a-zA-Z][a-zA-Z0-9_]{0,39}$/.test(key)) return null;
    if (typeof item === "number") {
      if (!Number.isFinite(item)) return null;
      sanitized[key] = item;
    } else if (typeof item === "string") {
      if (item.length > 2048) return null;
      sanitized[key] = item;
    } else if (typeof item === "boolean" || item === null) {
      sanitized[key] = item;
    } else {
      return null;
    }
  }

  if (sanitized.pageUrl && !isAllowedCrunchyrollUrl(sanitized.pageUrl)) return null;
  return sanitized;
}

function exceedsRateLimit(socket, type) {
  const rule = RATE_LIMITS[type];
  if (!rule) return false;

  const now = Date.now();
  const current = socket.rateLimits.get(type);
  if (!current || now - current.startedAt >= rule.windowMs) {
    socket.rateLimits.set(type, { count: 1, startedAt: now });
    return false;
  }

  current.count += 1;
  return current.count > rule.limit;
}

function safeSend(socket, payload) {
  if (!socket || socket.readyState !== WebSocket.OPEN) return;
  if (socket.bufferedAmount > MAX_BUFFERED_BYTES) {
    socket.close(1013, "Cliente muito lento");
    return;
  }
  socket.send(JSON.stringify(payload));
}

function getRoom(roomId) {
  if (!rooms.has(roomId)) {
    rooms.set(roomId, {
      hostClientId: null,
      clients: new Map(),
      lastState: null,
      messages: [],
      controlMode: "host",
      roomName: "Watch Party",
      lastAction: null
    });
  }
  return rooms.get(roomId);
}

function connectedClients(room) {
  return [...room.clients.entries()]
    .filter(([, client]) => client.socket?.readyState === WebSocket.OPEN);
}

function connectedCount(room) {
  return connectedClients(room).length;
}

function participantSnapshot(room) {
  return connectedClients(room).map(([clientId, client]) => ({
    clientId,
    name: client.name,
    avatar: client.avatar || "initial",
    avatarFrame: client.avatarFrame || "none",
    customPhoto: client.avatar === "custom" ? client.customPhoto || "" : "",
    nameColor: client.nameColor || "#f47521"
  }));
}

function roomSnapshot(room) {
  return {
    type: "ROOM_SNAPSHOT",
    hostClientId: room.hostClientId,
    participantCount: connectedCount(room),
    participants: participantSnapshot(room),
    messages: room.messages,
    controlMode: room.controlMode,
    roomName: room.roomName,
    lastAction: room.lastAction
  };
}

function broadcast(room, payload, exceptClientId = null) {
  for (const [clientId, client] of room.clients) {
    if (clientId === exceptClientId) continue;
    safeSend(client.socket, payload);
  }
}

function broadcastSnapshot(room) {
  broadcast(room, roomSnapshot(room));
}

function chooseNewHost(room) {
  for (const [clientId, client] of room.clients) {
    if (client.socket?.readyState === WebSocket.OPEN) return clientId;
  }
  return null;
}

function pushSystemMessage(room, text) {
  const message = {
    id: randomUUID(),
    system: true,
    text,
    createdAt: Date.now()
  };
  room.messages.push(message);
  if (room.messages.length > MAX_CHAT_MESSAGES) room.messages.shift();
  return message;
}

function finalizeDisconnect(roomId, clientId) {
  const room = rooms.get(roomId);
  const client = room?.clients.get(clientId);
  if (!room || !client || client.socket) return;

  const oldName = client.name;
  room.clients.delete(clientId);

  if (room.clients.size === 0) {
    rooms.delete(roomId);
    console.log(`Sala ${roomId} removida.`);
    return;
  }

  if (room.hostClientId === clientId) {
    room.hostClientId = chooseNewHost(room);
    if (room.hostClientId) {
      broadcast(room, { type: "HOST_CHANGED", hostClientId: room.hostClientId });
    }
  }

  pushSystemMessage(room, `${oldName} saiu da sala.`);
  broadcastSnapshot(room);
}

function markDisconnected(socket) {
  const { roomId, clientId } = socket.meta || {};
  const room = rooms.get(roomId);
  const client = room?.clients.get(clientId);
  if (!room || !client || client.socket !== socket) return;

  client.socket = null;
  clearTimeout(client.disconnectTimer);
  client.disconnectTimer = setTimeout(
    () => finalizeDisconnect(roomId, clientId),
    RECONNECT_GRACE_MS
  );
}

const app = express();
app.get("/", (_req, res) => {
  res.status(200).json({
    ok: true,
    service: "Crunchy Watch Party",
    version: "0.9.1",
    rooms: rooms.size
  });
});
app.get("/health", (_req, res) => res.status(200).send("ok"));

const server = http.createServer(app);
const wss = new WebSocketServer({
  server,
  maxPayload: MAX_SOCKET_PAYLOAD,
  verifyClient(info, done) {
    if (ALLOWED_ORIGINS.size === 0 || ALLOWED_ORIGINS.has(info.origin)) {
      done(true);
      return;
    }
    done(false, 403, "Origem não permitida");
  }
});

wss.on("connection", (socket) => {
  socket.meta = {};
  socket.isAlive = true;
  socket.rateLimits = new Map();
  socket.on("pong", () => { socket.isAlive = true; });
  socket.on("error", console.error);

  socket.on("message", (rawData) => {
    let message;
    try {
      message = JSON.parse(rawData.toString());
    } catch {
      safeSend(socket, { type: "ERROR", message: "Mensagem inválida." });
      return;
    }

    if (!message || typeof message !== "object" || Array.isArray(message)) {
      safeSend(socket, { type: "ERROR", message: "Formato de mensagem inválido." });
      return;
    }

    if (exceedsRateLimit(socket, message.type)) {
      safeSend(socket, { type: "ERROR", code: "RATE_LIMIT", message: "Muitas ações em pouco tempo." });
      return;
    }

    if (message.type === "JOIN_ROOM") {
      const roomId = String(message.roomId || "").trim().toUpperCase();
      const clientId = String(message.clientId || "").trim();
      const name = String(message.name || "Convidado").trim().slice(0, 24) || "Convidado";
      const avatar = String(message.avatar || "initial").slice(0, 20);
      const allowedFrames = new Set(["none", "neon", "fire", "ice", "heart", "gold"]);
      const avatarFrame = allowedFrames.has(String(message.avatarFrame)) ? String(message.avatarFrame) : "none";
      const rawCustomPhoto = String(message.customPhoto || "");
      const customPhoto = sanitizeCustomPhoto(avatar, rawCustomPhoto);
      const nameColor = /^#[0-9a-fA-F]{6}$/.test(String(message.nameColor || ""))
        ? String(message.nameColor)
        : "#f47521";
      const requestedRoomName = String(message.roomName || "").trim().slice(0, 40);
      const joinedState = message.state ? sanitizePlayerState(message.state) : null;

      if (!/^[A-Z0-9_-]{4,32}$/.test(roomId)
        || !/^[a-zA-Z0-9_-]{8,80}$/.test(clientId)) {
        safeSend(socket, { type: "ERROR", message: "Sala ou cliente inválido." });
        return;
      }

      const roomAlreadyExists = rooms.has(roomId);
      const existingRoom = roomAlreadyExists ? rooms.get(roomId) : null;
      const existingClient = existingRoom?.clients.get(clientId) || null;

      if (!message.createRoom && !roomAlreadyExists) {
        safeSend(socket, { type: "ERROR", message: "Essa sala não existe." });
        return;
      }

      if (message.createRoom && roomAlreadyExists && !existingClient) {
        safeSend(socket, { type: "ERROR", message: "Esse código de sala já está em uso." });
        return;
      }

      if (existingClient && !tokenMatches(message.sessionToken, existingClient.sessionTokenHash)) {
        safeSend(socket, {
          type: "ERROR",
          code: "INVALID_SESSION",
          message: "Não foi possível confirmar esta reconexão."
        });
        return;
      }

      const room = getRoom(roomId);
      const previous = room.clients.get(clientId);
      const isNewParticipant = !previous;
      const sessionToken = previous ? null : createSessionToken();

      if (previous) {
        clearTimeout(previous.disconnectTimer);
        if (previous.socket && previous.socket !== socket) previous.socket.close();
      }

      socket.meta = { roomId, clientId };
      room.clients.set(clientId, {
        socket,
        name,
        avatar,
        avatarFrame,
        customPhoto,
        nameColor,
        sessionTokenHash: previous?.sessionTokenHash || hashToken(sessionToken),
        disconnectTimer: null
      });

      if (!room.hostClientId) {
        room.hostClientId = clientId;
      }

      const clientIsHost = room.hostClientId === clientId;
      if (clientIsHost && requestedRoomName) room.roomName = requestedRoomName;
      if (joinedState && clientIsHost) room.lastState = joinedState;

      if (isNewParticipant) {
        pushSystemMessage(room, `${name} entrou na sala.`);
      }

      safeSend(socket, {
        type: "ROOM_JOINED",
        roomId,
        isHost: clientIsHost,
        hostClientId: room.hostClientId,
        participantCount: connectedCount(room),
        participants: participantSnapshot(room),
        messages: room.messages,
        controlMode: room.controlMode,
        roomName: room.roomName,
        lastAction: room.lastAction,
        ...(sessionToken ? { sessionToken } : {})
      });

      broadcastSnapshot(room);

      if (clientIsHost && joinedState) {
        if (joinedState.pageUrl) {
          broadcast(room, {
            type: "NAVIGATE",
            pageUrl: joinedState.pageUrl,
            reason: "host-reconnected"
          }, clientId);
        }
        broadcast(room, {
          type: "PLAYER_STATE",
          state: room.lastState,
          serverSentAt: Date.now()
        }, clientId);
      } else if (room.lastState) {
        safeSend(socket, {
          type: "PLAYER_STATE",
          state: room.lastState,
          serverSentAt: Date.now()
        });
      } else if (!clientIsHost) {
        safeSend(room.clients.get(room.hostClientId)?.socket, { type: "REQUEST_STATE" });
      }

      console.log(`${name} entrou/reconectou na sala ${roomId}. Conectados: ${connectedCount(room)}`);
      return;
    }

    const { roomId, clientId } = socket.meta;
    const room = rooms.get(roomId);

    if (!room || !clientId) {
      safeSend(socket, { type: "ERROR", message: "Entre em uma sala primeiro." });
      return;
    }

    const activeClient = room.clients.get(clientId);
    if (!activeClient || activeClient.socket !== socket) {
      safeSend(socket, { type: "ERROR", code: "STALE_CONNECTION", message: "Esta conexão não está mais ativa." });
      return;
    }

    if (message.type === "PING") {
      safeSend(socket, { type: "PONG", serverSentAt: Date.now() });
      return;
    }

    if (message.type === "LEAVE_ROOM") {
      const leavingClient = room.clients.get(clientId);
      if (!leavingClient || leavingClient.socket !== socket) return;
      safeSend(socket, { type: "LEFT_ROOM", roomId });
      clearTimeout(leavingClient.disconnectTimer);
      leavingClient.socket = null;
      socket.meta = {};
      finalizeDisconnect(roomId, clientId);
      socket.close(1000, "Saída voluntária");
      return;
    }

    if (message.type === "UPDATE_PROFILE") {
      const client = room.clients.get(clientId);
      if (!client) return;

      const nextName = String(message.name || client.name || "Convidado")
        .trim()
        .slice(0, 24) || "Convidado";
      const nextAvatar = String(message.avatar || client.avatar || "initial").slice(0, 20);
      const allowedFrames = new Set(["none", "neon", "fire", "ice", "heart", "gold"]);
      const nextAvatarFrame = allowedFrames.has(String(message.avatarFrame))
        ? String(message.avatarFrame)
        : client.avatarFrame || "none";
      const rawNextPhoto = String(message.customPhoto || "");
      const nextPhoto = sanitizeCustomPhoto(nextAvatar, rawNextPhoto);
      const nextColor = /^#[0-9a-fA-F]{6}$/.test(String(message.nameColor || ""))
        ? String(message.nameColor)
        : client.nameColor || "#f47521";

      client.name = nextName;
      client.avatar = nextAvatar;
      client.avatarFrame = nextAvatarFrame;
      client.customPhoto = nextPhoto;
      client.nameColor = nextColor;

      broadcastSnapshot(room);
      return;
    }

    if (message.type === "SET_CONTROL_MODE") {
      if (room.hostClientId !== clientId) {
        safeSend(socket, {
          type: "ERROR",
          message: "Somente o anfitrião pode alterar o controle da reprodução."
        });
        return;
      }

      room.controlMode = message.controlMode === "everyone" ? "everyone" : "host";
      broadcast(room, {
        type: "CONTROL_MODE_CHANGED",
        controlMode: room.controlMode
      });
      broadcastSnapshot(room);
      return;
    }

    if (message.type === "REACTION") {
      const client = room.clients.get(clientId);
      const allowedReactions = new Set(["😂", "😭", "❤️", "🔥", "😱", "👏"]);
      const reaction = String(message.reaction || "").slice(0, 8);
      if (!client || !allowedReactions.has(reaction)) return;

      const reactionMessage = {
        id: randomUUID(),
        system: false,
        reaction,
        clientId,
        name: client.name,
        avatar: client.avatar || "initial",
        avatarFrame: client.avatarFrame || "none",
        nameColor: client.nameColor,
        createdAt: Date.now()
      };

      room.messages.push(reactionMessage);
      if (room.messages.length > MAX_CHAT_MESSAGES) room.messages.shift();

      broadcast(room, {
        type: "REACTION_EVENT",
        reactionEvent: reactionMessage
      });

      broadcast(room, {
        type: "CHAT_MESSAGE",
        message: reactionMessage
      });
      return;
    }

    if (message.type === "CHAT_MESSAGE") {
      const client = room.clients.get(clientId);
      const text = String(message.text || "").trim().slice(0, 300);
      if (!client || !text) return;

      const chatMessage = {
        id: randomUUID(),
        system: false,
        clientId,
        name: client.name,
        avatar: client.avatar || "initial",
        avatarFrame: client.avatarFrame || "none",
        nameColor: client.nameColor,
        text,
        createdAt: Date.now()
      };

      room.messages.push(chatMessage);
      if (room.messages.length > MAX_CHAT_MESSAGES) room.messages.shift();

      broadcast(room, { type: "CHAT_MESSAGE", message: chatMessage });
      return;
    }

    if (message.type === "NAVIGATE") {
      if (room.hostClientId !== clientId) return;
      const pageUrl = String(message.pageUrl || "");
      if (!isAllowedCrunchyrollUrl(pageUrl)) {
        safeSend(socket, { type: "ERROR", code: "INVALID_URL", message: "URL da Crunchyroll inválida." });
        return;
      }
      room.lastState = { ...(room.lastState || {}), pageUrl };
      broadcast(room, {
        type: "NAVIGATE",
        pageUrl,
        reason: message.reason,
        serverSentAt: Date.now()
      }, clientId);
      return;
    }

    if (message.type === "PLAYER_STATE") {
      const canControl = room.hostClientId === clientId || room.controlMode === "everyone";
      if (!canControl) return;

      const nextState = sanitizePlayerState(message.state);
      if (!nextState) {
        safeSend(socket, { type: "ERROR", code: "INVALID_STATE", message: "Estado do player inválido." });
        return;
      }

      room.lastState = nextState;

      const controller = room.clients.get(clientId);
      const reasonLabels = {
        play: "reproduziu o vídeo",
        pause: "pausou o vídeo",
        seeked: "avançou ou voltou o vídeo",
        ratechange: "alterou a velocidade"
      };

      if (reasonLabels[message.reason]) {
        room.lastAction = {
          id: randomUUID(),
          text: `${controller?.name || "Alguém"} ${reasonLabels[message.reason]}.`,
          createdAt: Date.now()
        };
        broadcast(room, { type: "ACTION_NOTICE", action: room.lastAction });
      }

      broadcast(room, {
        type: "PLAYER_STATE",
        state: nextState,
        reason: message.reason,
        controlledByClientId: clientId,
        serverSentAt: Date.now()
      }, clientId);
    }
  });

  socket.on("close", () => markDisconnected(socket));
});

const keepAlive = setInterval(() => {
  for (const socket of wss.clients) {
    if (socket.isAlive === false) {
      socket.terminate();
      continue;
    }
    socket.isAlive = false;
    socket.ping();
  }
}, 30000);

wss.on("close", () => clearInterval(keepAlive));

export function startServer(port = PORT, host = "0.0.0.0") {
  return server.listen(port, host, () => {
    const address = server.address();
    const activePort = typeof address === "object" && address ? address.port : port;
    console.log(`Servidor Watch Party v0.9.1 iniciado na porta ${activePort}`);
    console.log(`Local: ws://localhost:${activePort}`);
  });
}

const isMainModule = process.argv[1]
  && import.meta.url === pathToFileURL(process.argv[1]).href;

if (isMainModule) startServer();

export { server, wss };
