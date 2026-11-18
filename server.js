
const http = require("http");
const { WebSocketServer, WebSocket } = require("ws");

const PORT = process.env.PORT || 10000;
const HOST = "0.0.0.0";

const VERSION = "1.0.0";

let gameSocket = null;
let requestId = 0;

const pending = new Map();

const METHODS = [
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

// HTTP JSON response
function json(res, status, data) {
    res.writeHead(status, {
        "Content-Type": "application/json; charset=utf-8",
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type"
    });

    res.end(JSON.stringify(data));
}

// Remote API connection status
function isConnected() {
    return gameSocket?.readyState === WebSocket.OPEN;
}

// Send JSON-RPC request to Bitburner
function sendRPC(method, params = {}) {
    return new Promise((resolve, reject) => {

        if (!isConnected()) {
            return reject(new Error("Bitburner is offline"));
        }

        if (!METHODS.includes(method)) {
            return reject(new Error("Unknown method"));
        }

        const id = ++requestId;

        const message = {
            jsonrpc: "2.0",
            id,
            method,
            params
        };

        const timeout = setTimeout(() => {
            pending.delete(id);
            reject(new Error("Request timeout"));
        }, 15000);

        pending.set(id, {
            resolve: result => {
                clearTimeout(timeout);
                resolve(result);
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
}

// HTTP server
const server = http.createServer((req, res) => {

    if (req.method === "OPTIONS") {
        return json(res, 204, {});
    }

    if (req.url === "/" && req.method === "GET") {
        return json(res, 200, {
            name: "BITBURNER REMOTE SERVER",
            status: "online",
            version: VERSION,
            websocket: "enabled",
            gameConnected: isConnected()
        });
    }

    if (req.url === "/api/status" && req.method === "GET") {
        return json(res, 200, {
            status: "online",
            gameConnected: isConnected(),
            pendingRequests: pending.size,
            uptime: process.uptime(),
            version: VERSION,
            timestamp: Date.now()
        });
    }

    if (req.url === "/api/rpc" && req.method === "POST") {

        let body = "";

        req.on("data", chunk => {
            body += chunk.toString();

            if (body.length > 1024 * 1024) {
                res.destroy();
                req.destroy();
            }
        });

        req.on("end", async () => {
            try {
                const data = JSON.parse(body);

                if (!data || typeof data.method !== "string") {
                    return json(res, 400, {
                        error: "Invalid request"
                    });
                }

                const result = await sendRPC(
                    data.method,
                    data.params || {}
                );

                return json(res, 200, {
                    jsonrpc: "2.0",
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

// WebSocket server
const wss = new WebSocketServer({
    noServer: true,
    maxPayload: 1024 * 1024
});

// HTTP -> WebSocket upgrade
server.on("upgrade", (req, socket, head) => {

    if (req.url !== "/") {
        socket.write("HTTP/1.1 404 Not Found\r\n\r\n");
        socket.destroy();
        return;
    }

    wss.handleUpgrade(req, socket, head, ws => {
        wss.emit("connection", ws, req);
    });
});

// WebSocket connection
wss.on("connection", ws => {

    console.log("[WSS] New Bitburner connection");

    // One active game connection at a time
    if (gameSocket && gameSocket !== ws) {
        gameSocket.close(4000, "New connection");
    }

    gameSocket = ws;

    console.log("[WSS] BITBURNER ONLINE");

    ws.on("message", message => {

        try {
            const data = JSON.parse(message.toString());

            if (data.id !== undefined && pending.has(data.id)) {

                const request = pending.get(data.id);
                pending.delete(data.id);

                if (data.error) {
                    request.reject(new Error(
                        typeof data.error === "string"
                            ? data.error
                            : JSON.stringify(data.error)
                    ));
                } else {
                    request.resolve(data.result);
                }

                return;
            }

            console.log("[WSS] Message received");

        } catch (error) {
            console.error("[WSS] Invalid JSON:", error.message);
        }
    });

    ws.on("close", () => {

        console.log("[WSS] BITBURNER DISCONNECTED");

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

// Heartbeat
const heartbeat = setInterval(() => {

    if (isConnected()) {
        gameSocket.ping();
    }

}, 30000);

// Start server
server.listen(PORT, HOST, () => {

    console.log("");
    console.log("================================");
    console.log("   BITBURNER REMOTE SERVER");
    console.log("================================");
    console.log("STATUS : ONLINE");
    console.log("HOST   :", HOST);
    console.log("PORT   :", PORT);
    console.log("WSS    : ENABLED");
    console.log("API    : JSON-RPC 2.0");
    console.log("VERSION:", VERSION);
    console.log("================================");
    console.log("");
});

server.on("close", () => {
    clearInterval(heartbeat);
});
