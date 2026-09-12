import { connect } from 'cloudflare:sockets';

const UuidRegex = /^[0-9a-f]{8}-[0-9a-f]{4}-[4][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

export default {
    async fetch(request, env, ctx) {
        try {
            const upgradeHeader = request.headers.get('Upgrade');
            if (!upgradeHeader || upgradeHeader !== 'websocket') {
                return new Response('Ariobarzan Relay Active', { status: 200 });
            }
            const webSocketPair = new WebSocketPair();
            [webSocketPair[0], webSocketPair[1]].forEach((ws) => ws.accept());
            ctx.waitUntil(handleWebSocket(webSocketPair[1], env.UUID));
            return new Response(null, {
                status: 101,
                webSocket: webSocketPair[0],
            });
        } catch (err) {
            return new Response(err.toString(), { status: 500 });
        }
    },
};

async function handleWebSocket(webSocket, userID) {
    let remoteSocket = null;
    let wsClosed = false;

    webSocket.addEventListener('message', async (event) => {
        if (wsClosed) return;
        const message = event.data;
        
        if (!remoteSocket) {
            try {
                const header = processVlessHeader(message, userID);
                if (!header) {
                    webSocket.close();
                    return;
                }
                remoteSocket = connect({ hostname: header.address, port: header.port });
                
                const writer = remoteSocket.writable.getWriter();
                await writer.write(header.rest);
                writer.releaseLock();

                // خواندن داده‌ها از ریموت و ارسال به وب‌سوکت
                remoteSocket.readable.pipeTo(new WritableStream({
                    write(chunk) {
                        if (!wsClosed && webSocket.readyState === WebSocket.OPEN) {
                            webSocket.send(chunk);
                        }
                    },
                    close() {
                        safeClose();
                    },
                    abort(err) {
                        safeClose();
                    }
                })).catch(() => safeClose());

            } catch (e) {
                safeClose();
            }
        } else {
            try {
                const writer = remoteSocket.writable.getWriter();
                await writer.write(message);
                writer.releaseLock();
            } catch (e) {
                safeClose();
            }
        }
    });

    webSocket.addEventListener('close', () => { wsClosed = true; safeClose(); });
    webSocket.addEventListener('error', () => { wsClosed = true; safeClose(); });

    function safeClose() {
        wsClosed = true;
        try { remoteSocket?.close(); } catch {}
        try { webSocket.close(); } catch {}
    }
}

function processVlessHeader(buffer, userID) {
    if (buffer.byteLength < 24) return null;
    const view = new DataView(buffer);
    
    // بررسی UUID
    const uuidBytes = new Uint8Array(buffer, 1, 16);
    const uuid = bytesToUuid(uuidBytes);
    if (uuid !== userID) return null;

    const optLength = view.getUint8(17);
    const commandIndex = 18 + optLength;
    const command = view.getUint8(commandIndex);
    
    // فقط دستور TCP (مقدار 1) پشتیبانی می‌شود
    if (command !== 1) return null;

    let portIndex = commandIndex + 1;
    const port = view.getUint16(portIndex);
    let addressIndex = portIndex + 2;
    const addressType = view.getUint8(addressIndex);
    
    let addressLength = 0;
    let address = "";
    let headerLength = 0;

    if (addressType === 1) { // IPv4
        addressLength = 4;
        addressIndex += 1;
        address = new Uint8Array(buffer, addressIndex, 4).join('.');
        headerLength = addressIndex + 4;
    } else if (addressType === 2) { // Domain
        addressLength = view.getUint8(addressIndex + 1);
        addressIndex += 2;
        address = new TextDecoder().decode(new Uint8Array(buffer, addressIndex, addressLength));
        headerLength = addressIndex + addressLength;
    } else if (addressType === 3) { // IPv6
        addressLength = 16;
        addressIndex += 1;
        const arr = [];
        for (let i = 0; i < 8; i++) {
            arr.push(view.getUint16(addressIndex + i * 2).toString(16));
        }
        address = arr.join(':');
        headerLength = addressIndex + 16;
    } else {
        return null;
    }

    const rest = buffer.slice(headerLength);
    return { address, port, rest };
}

function bytesToUuid(bytes) {
    const hex = Array.from(bytes).map(b => b.toString(16).padStart(2, '0')).join('');
    return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}
