import http from "node:http";
import express from "express";
import { randomUUID } from "node:crypto";
import { WebSocket, WebSocketServer } from "ws";

const PORT = Number(process.env.PORT || 8080);
const RECONNECT_GRACE_MS = 15000;
const MAX_CHAT_MESSAGES = 100;
const rooms = new Map();

function safeSend(socket, payload) {
  if (!socket || socket.readyState !== WebSocket.OPEN) return;
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
    version: "0.8.0",
    rooms: rooms.size
  });
});
app.get("/health", (_req, res) => res.status(200).send("ok"));

const server = http.createServer(app);
const wss = new WebSocketServer({ server });

wss.on("connection", (socket) => {
  socket.meta = {};
  socket.isAlive = true;
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

    if (message.type === "JOIN_ROOM") {
      const roomId = String(message.roomId || "").trim().toUpperCase();
      const clientId = String(message.clientId || "").trim();
      const name = String(message.name || "Convidado").trim().slice(0, 24) || "Convidado";
      const avatar = String(message.avatar || "initial").slice(0, 20);
      const allowedFrames = new Set(["none", "neon", "fire", "ice", "heart", "gold"]);
      const avatarFrame = allowedFrames.has(String(message.avatarFrame)) ? String(message.avatarFrame) : "none";
      const rawCustomPhoto = String(message.customPhoto || "");
      const customPhoto = avatar === "custom"
        && rawCustomPhoto.startsWith("data:image/")
        && rawCustomPhoto.length <= 420000
          ? rawCustomPhoto
          : "";
      const nameColor = /^#[0-9a-fA-F]{6}$/.test(String(message.nameColor || ""))
        ? String(message.nameColor)
        : "#f47521";
      const requestedRoomName = String(message.roomName || "").trim().slice(0, 40);

      if (!roomId || !clientId) {
        safeSend(socket, { type: "ERROR", message: "Sala ou cliente inválido." });
        return;
      }

      let roomAlreadyExists = rooms.has(roomId);
      let existingRoom = roomAlreadyExists ? rooms.get(roomId) : null;
      let existingClient = existingRoom?.clients.get(clientId) || null;

      if (roomAlreadyExists && connectedCount(existingRoom) === 0) {
        rooms.delete(roomId);
        roomAlreadyExists = false;
        existingRoom = null;
        existingClient = null;
      }

      if (!message.createRoom && !roomAlreadyExists) {
        safeSend(socket, { type: "ERROR", message: "Essa sala não existe." });
        return;
      }

      if (message.createRoom && roomAlreadyExists && !existingClient) {
        safeSend(socket, { type: "ERROR", message: "Esse código de sala já está em uso." });
        return;
      }

      const room = getRoom(roomId);
      const previous = room.clients.get(clientId);
      const isNewParticipant = !previous;

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
        disconnectTimer: null
      });

      const currentHost = room.clients.get(room.hostClientId);
      const hostIsConnected = currentHost?.socket?.readyState === WebSocket.OPEN;

      if (!room.hostClientId || !hostIsConnected || message.createRoom) {
        room.hostClientId = clientId;
      }

      const clientIsHost = room.hostClientId === clientId;
      if (clientIsHost && requestedRoomName) room.roomName = requestedRoomName;
      if (message.state && clientIsHost) room.lastState = message.state;

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
        lastAction: room.lastAction
      });

      broadcastSnapshot(room);

      if (clientIsHost && message.state) {
        broadcast(room, {
          type: "NAVIGATE",
          pageUrl: message.state.pageUrl,
          reason: "host-reconnected"
        }, clientId);
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

    if (message.type === "PING") {
      safeSend(socket, { type: "PONG", serverSentAt: Date.now() });
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
      const nextPhoto = nextAvatar === "custom"
        && rawNextPhoto.startsWith("data:image/")
        && rawNextPhoto.length <= 420000
          ? rawNextPhoto
          : "";
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
        customPhoto: client.avatar === "custom" ? client.customPhoto || "" : "",
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
        customPhoto: client.avatar === "custom" ? client.customPhoto || "" : "",
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
      if (!pageUrl.includes("crunchyroll.com")) return;
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

      room.lastState = message.state;

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
        state: message.state,
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

server.listen(PORT, "0.0.0.0", () => {
  console.log(`Servidor Watch Party v0.8.0 iniciado na porta ${PORT}`);
  console.log("Local: ws://localhost:" + PORT);
});
