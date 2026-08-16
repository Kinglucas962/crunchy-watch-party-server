import assert from "node:assert/strict";
import { after, before, test } from "node:test";
import { WebSocket } from "ws";
import {
  isAllowedCrunchyrollUrl,
  sanitizePlayerState,
  server,
  startServer,
  wss
} from "../server.js";

let baseUrl;

function openSocket() {
  return new Promise((resolve, reject) => {
    const socket = new WebSocket(baseUrl);
    socket.once("open", () => resolve(socket));
    socket.once("error", reject);
  });
}

function nextMessage(socket, expectedType, timeoutMs = 1500) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      cleanup();
      reject(new Error(`Tempo esgotado aguardando ${expectedType}`));
    }, timeoutMs);

    const onMessage = (raw) => {
      const message = JSON.parse(raw.toString());
      if (message.type !== expectedType) return;
      cleanup();
      resolve(message);
    };

    const cleanup = () => {
      clearTimeout(timer);
      socket.off("message", onMessage);
    };

    socket.on("message", onMessage);
  });
}

async function sendAndWait(socket, payload, type) {
  const pending = nextMessage(socket, type);
  socket.send(JSON.stringify(payload));
  return pending;
}

before(async () => {
  await new Promise((resolve) => startServer(0, "127.0.0.1").once("listening", resolve));
  const address = server.address();
  baseUrl = `ws://127.0.0.1:${address.port}`;
});

after(async () => {
  for (const socket of wss.clients) socket.terminate();
  await new Promise((resolve) => wss.close(resolve));
  if (server.listening) await new Promise((resolve) => server.close(resolve));
});

test("aceita apenas URLs HTTPS oficiais da Crunchyroll", () => {
  assert.equal(isAllowedCrunchyrollUrl("https://www.crunchyroll.com/watch/ABC"), true);
  assert.equal(isAllowedCrunchyrollUrl("https://static.crunchyroll.com/image.png"), true);
  assert.equal(isAllowedCrunchyrollUrl("http://www.crunchyroll.com/watch/ABC"), false);
  assert.equal(isAllowedCrunchyrollUrl("https://crunchyroll.com.evil.example/watch/ABC"), false);
});

test("rejeita estados grandes, aninhados ou com URL externa", () => {
  assert.deepEqual(sanitizePlayerState({ currentTime: 12, paused: true }), {
    currentTime: 12,
    paused: true
  });
  assert.equal(sanitizePlayerState({ nested: { value: true } }), null);
  assert.equal(sanitizePlayerState({ pageUrl: "https://example.com/video" }), null);
  assert.equal(sanitizePlayerState({ text: "x".repeat(9000) }), null);
});

test("protege identidade e função de anfitrião durante reconexões", async () => {
  const host = await openSocket();
  const hostJoined = await sendAndWait(host, {
    type: "JOIN_ROOM",
    roomId: "SAFE1234",
    clientId: "host-client-1234",
    name: "Host",
    createRoom: true
  }, "ROOM_JOINED");

  assert.equal(hostJoined.isHost, true);
  assert.ok(hostJoined.sessionToken);

  const guest = await openSocket();
  const guestJoined = await sendAndWait(guest, {
    type: "JOIN_ROOM",
    roomId: "SAFE1234",
    clientId: "guest-client-1234",
    name: "Guest",
    createRoom: false
  }, "ROOM_JOINED");

  assert.equal(guestJoined.isHost, false);
  assert.ok(guestJoined.sessionToken);

  const attacker = await openSocket();
  const attackResult = await sendAndWait(attacker, {
    type: "JOIN_ROOM",
    roomId: "SAFE1234",
    clientId: "host-client-1234",
    name: "Attacker",
    createRoom: true
  }, "ERROR");

  assert.equal(attackResult.code, "INVALID_SESSION");

  const reconnectedGuest = await openSocket();
  const guestReconnect = await sendAndWait(reconnectedGuest, {
    type: "JOIN_ROOM",
    roomId: "SAFE1234",
    clientId: "guest-client-1234",
    name: "Guest",
    createRoom: true,
    sessionToken: guestJoined.sessionToken
  }, "ROOM_JOINED");

  assert.equal(guestReconnect.isHost, false);
  assert.equal(guestReconnect.hostClientId, "host-client-1234");

  host.terminate();
  guest.terminate();
  attacker.terminate();
  reconnectedGuest.terminate();
});

test("não inclui a foto Base64 em cada mensagem de chat", async () => {
  const host = await openSocket();
  await sendAndWait(host, {
    type: "JOIN_ROOM",
    roomId: "PHOTO123",
    clientId: "photo-client-1234",
    name: "Photo",
    avatar: "custom",
    customPhoto: `data:image/png;base64,${"a".repeat(100)}`,
    createRoom: true
  }, "ROOM_JOINED");

  const chat = await sendAndWait(host, {
    type: "CHAT_MESSAGE",
    text: "Olá"
  }, "CHAT_MESSAGE");

  assert.equal(chat.message.text, "Olá");
  assert.equal("customPhoto" in chat.message, false);
  host.terminate();
});
