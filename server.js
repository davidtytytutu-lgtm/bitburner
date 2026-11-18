
const http = require("http");
const crypto = require("crypto");
const { WebSocketServer, WebSocket } = require("ws");

const PORT = process.env.PORT || 10000;
const HOST = "0.0.0.0";
const TOKEN = process.env.REMOTE_API_TOKEN;

const allowedMethods = [
    "pushFile",
    "getFile",
    "getFileMetadata",
    "deleteFile",
    "getFileNames",
    "getAllFiles",
    "getAllFileMetadata",
    "calculateRam",
    "getDefinitionFile",
    "getSaveFile",
    "getAllServers"
];

let gameSocket = null;
let requestId = 0;
const pending = new Map();

function json(res, status, data) {
    res.writeHead(status, {
        "Content-Type": "application/json; charset=utf-8",
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type, Authorization"
    });

    res.end(JSON.stringify(data));
}

function authorized(req) {
    if (!TOKEN) return false;

    const supplied = req.headers.authorization || "";
    const expected = `Bearer ${TOKEN}`;

    const a = Buffer.from(supplied);
    const b = Buffer.from(expected);

    return a.length === b.length &&
        crypto.timingSafeEqual(a, b);
}

const server = http.createServer((req, res) => {
    if (req.method === "OPTIONS") {
        return json(res, 204, {});
    }

    if (req.url === "/" && req.method === "GET") {
        return json(res, 200, {
            name: "BITBURNER REMOTE SERVER",
            status: "online",
            websocket: gameSocket?.readyState === WebSocket.OPEN
                ? "connected"
                : "waiting",
            version: "2.0.0"
        });
    }

    if (req.url === "/api/status" && req.method === "GET") {
        return json(res, 200, {
            status: "online",
            gameConnected: gameSocket?.readyState === WebSocket.OPEN,
            pendingRequests: pending.size,
            uptime: process.uptime(),
            timestamp: Date.now()
        });
    }

    if (req.url === "/api/rpc" && req.method === "POST") {
        if (!authorized(req)) {
            return json(res, 401, {
                error: "Unauthorized"
            });
        }

        let body = "";

        req.on("data", chunk => {
            body += chunk;

            if (body.length > 1024 * 1024) {
                req.destroy();
            }
        });

        req.on("end", async () => {
            try {
                const data = JSON.parse(body);
                const { method, params = {} } = data;

                if (!allowedMethods.includes(method)) {
                    return json(res, 400, {
                        error: "Unknown method"
                    });
                }

                if (!gameSocket ||
                    gameSocket.readyState !== WebSocket.OPEN) {
                    return json(res, 503, {
                        error: "Bitburner is not connected"
                    });
                }

                const id = ++requestId;

                const message = {
                    jsonrpc: "2.0",
                    id,
                    method,
                    params
                };

                const result = await new Promise((resolve, reject) => {
                    const timeout = setTimeout(() => {
                        pending.delete(id);
                        reject(new Error("Request timeout"));
                    }, 15000);

                    pending.set(id, {
                        resolve: value => {
                            clearTimeout(timeout);
                            resolve(value);
                        },
                        reject: error => {
                            clearTimeout(timeout);
                            reject(error);
                        }
                    });

                    gameSocket.send(JSON.stringify(message), error => {
                        if (error) {
                            clearTimeout(timeout);
                            pending.delete(id);
                            reject(error);
                        }
                    });
                });

                return json(res, 200, {
                    jsonrpc: "2.0",
                    id,
                    result
                });

            } catch (error) {
                return json(res, 500, {
                    error: error.message
                });
            }
        });

        return;
    }

    return json(res, 404, {
        error: "Not Found"
    });
});

const wss = new WebSocketServer({ noServer: true });

server.on("upgrade", (req, socket, head) => {
    wss.handleUpgrade(req, socket, head, ws => {
        wss.emit("connection", ws, req);
    });
});

wss.on("connection", ws => {
    console.log("[WSS] Bitburner connected");

    if (gameSocket && gameSocket !== ws) {
        gameSocket.close(4000, "New connection");
    }

    gameSocket = ws;

    ws.on("message", message => {
        try {
            const data = JSON.parse(message.toString());

            if (data.id !== undefined && pending.has(data.id)) {
                const request = pending.get(data.id);
                pending.delete(data.id);

                if (data.error) {
                    request.reject(new Error(data.error));
                } else {
                    request.resolve(data.result);
                }
            } else {
                console.log("[WSS] Received message:", data);
            }

        } catch (error) {
            console.error("[WSS] Invalid message:", error.message);
        }
    });

    ws.on("close", () => {
        console.log("[WSS] Bitburner disconnected");

        if (gameSocket === ws) {
            gameSocket = null;
        }

        for (const [id, request] of pending) {
            request.reject(new Error("Bitburner disconnected"));
            pending.delete(id);
        }
    });

    ws.on("error", error => {
        console.error("[WSS] Error:", error.message);
    });
});

// WebSocket heartbeat
const heartbeat = setInterval(() => {
    if (gameSocket?.readyState === WebSocket.OPEN) {
        gameSocket.ping();
    }
}, 30000);

server.listen(PORT, HOST, () => {
    console.log("================================");
    console.log("   BITBURNER REMOTE SERVER");
    console.log("================================");
    console.log("STATUS : ONLINE");
    console.log("HOST   :", HOST);
    console.log("PORT   :", PORT);
    console.log("WSS    : ENABLED");
    console.log("================================");
});

server.on("close", () => clearInterval(heartbeat));
