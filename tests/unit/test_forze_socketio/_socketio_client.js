// A real socket.io-client, for test_connection_drain_wire's env-gated reconnect test.
// Usage: node _socketio_client.js URL — logs each event on stdout, one per line, until killed.
const { io } = require("socket.io-client");

const socket = io(process.argv[2], {
  transports: ["websocket"],
  reconnectionDelay: 200,
  reconnectionDelayMax: 400,
});

socket.on("connect", () => console.log("connect"));
socket.on("disconnect", (reason) => console.log(`disconnect ${reason}`));
socket.on("connect_error", (err) => console.log(`connect_error active=${socket.active}`));
socket.io.on("reconnect_attempt", () => console.log("reconnect_attempt"));
