
const http = require("http");

const PORT = process.env.PORT || 10000;
const HOST = "0.0.0.0";

const server = http.createServer((req, res) => {
    res.setHeader("Content-Type", "application/json; charset=utf-8");
    res.setHeader("Access-Control-Allow-Origin", "*");
    res.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
    res.setHeader("Access-Control-Allow-Headers", "Content-Type");

    if (req.method === "OPTIONS") {
        res.writeHead(204);
        return res.end();
    }

    if (req.url === "/") {
        res.writeHead(200);
        return res.end(JSON.stringify({
            name: "BITBURNER SERVER",
            status: "online",
            version: "1.0.0",
            message: "Server is running!"
        }));
    }

    if (req.url === "/api/status") {
        res.writeHead(200);
        return res.end(JSON.stringify({
            status: "online",
            uptime: process.uptime(),
            timestamp: Date.now(),
            port: PORT
        }));
    }

    res.writeHead(404);
    res.end(JSON.stringify({
        error: "Not Found",
        status: 404
    }));
});

server.listen(PORT, HOST, () => {
    console.log("================================");
    console.log("       BITBURNER SERVER");
    console.log("================================");
    console.log(`STATUS : ONLINE`);
    console.log(`HOST   : ${HOST}`);
    console.log(`PORT   : ${PORT}`);
    console.log(`URL    : http://localhost:${PORT}`);
    console.log("================================");
});
