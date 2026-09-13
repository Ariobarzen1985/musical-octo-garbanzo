import { connect } from "cloudflare:sockets";

export default {
  async fetch(request, env) {
    const upgrade = request.headers.get("Upgrade");
    if (upgrade !== "websocket") {
      return new Response("Ariobarzan relay is running.", { status: 200 });
    }

    const allowedUuid = (env.UUID || "").replace(/-/g, "").toLowerCase();
    if (!allowedUuid) {
      return new Response("UUID not configured", { status: 500 });
    }

    const pair = new WebSocketPair();
    const client = pair[0];
    const server = pair[1];
    server.accept();

    let remoteSocket = null;
    let sessionStarted = false;
    let isUdp = false;
    let udpPort = null;

    server.addEventListener("message", async (event) => {
      try {
        const data = event.data;
        const buf = data instanceof ArrayBuffer ? new Uint8Array(data) : new Uint8Array(await data.arrayBuffer());

        if (!sessionStarted) {
          const parsed = parseVlessHeader(buf, allowedUuid);
          if (!parsed) {
            server.close(1008, "invalid vless header");
            return;
          }
          sessionStarted = true;
          isUdp = parsed.cmd === 2;
          udpPort = parsed.port;

          // پاسخ هندشیک VLESS: نسخه + بدون addon
          server.send(new Uint8Array([parsed.version, 0]));

          if (isUdp) {
            if (udpPort === 53) {
              // درخواست‌های DNS را با DNS-over-HTTPS واقعی پاسخ می‌دهیم
              await handleUdpFrames(parsed.rawClientData, server);
            } else {
              // Cloudflare Workers نمی‌تواند UDP خام (مثل QUIC روی پورت ۴۴۳) را
              // رله کند؛ به‌جای هنگ‌کردن طولانی، فوری می‌بندیم تا کلاینت سریع
              // به TCP سوییچ کند (اگر «Block QUIC» در کلاینت فعال باشد، اصلاً
              // به این حالت نمی‌رسد).
              server.close(1000, "udp not supported on workers");
            }
            return;
          }

          remoteSocket = connect({ hostname: parsed.addr, port: parsed.port });
          const writer = remoteSocket.writable.getWriter();
          if (parsed.rawClientData.length > 0) {
            await writer.write(parsed.rawClientData);
          }
          writer.releaseLock();
          pumpRemoteToClient(remoteSocket, server);
        } else if (isUdp) {
          if (udpPort === 53) {
            await handleUdpFrames(buf, server);
          }
        } else {
          const writer = remoteSocket.writable.getWriter();
          await writer.write(buf);
          writer.releaseLock();
        }
      } catch (err) {
        try { server.close(1011, "relay error"); } catch (_) {}
      }
    });

    server.addEventListener("close", () => {
      try { remoteSocket && remoteSocket.close(); } catch (_) {}
    });

    return new Response(null, { status: 101, webSocket: client });
  },
};

function parseVlessHeader(buf, expectedUuidHex) {
  if (buf.length < 24) return null;
  const version = buf[0];

  let uuidHex = "";
  for (let i = 1; i <= 16; i++) uuidHex += buf[i].toString(16).padStart(2, "0");
  if (uuidHex !== expectedUuidHex) return null;

  let offset = 17;
  const optLen = buf[offset];
  offset += 1 + optLen;

  const cmd = buf[offset]; // 1 = TCP, 2 = UDP
  offset += 1;

  const port = (buf[offset] << 8) + buf[offset + 1];
  offset += 2;

  const addrType = buf[offset];
  offset += 1;

  let addr;
  if (addrType === 1) {
    addr = buf[offset] + "." + buf[offset + 1] + "." + buf[offset + 2] + "." + buf[offset + 3];
    offset += 4;
  } else if (addrType === 2) {
    const len = buf[offset];
    offset += 1;
    addr = new TextDecoder().decode(buf.slice(offset, offset + len));
    offset += len;
  } else if (addrType === 3) {
    const parts = [];
    for (let i = 0; i < 8; i++) {
      parts.push(((buf[offset] << 8) + buf[offset + 1]).toString(16));
      offset += 2;
    }
    addr = parts.join(":");
  } else {
    return null;
  }

  const rawClientData = buf.slice(offset);
  return { version, addr, port, cmd, rawClientData };
}

async function pumpRemoteToClient(remoteSocket, ws) {
  const reader = remoteSocket.readable.getReader();
  try {
    while (true) {
      const { value, done } = await reader.read();
      if (done) break;
      ws.send(value);
    }
  } catch (_) {
    // اتصال قطع شد
  } finally {
    try { ws.close(); } catch (_) {}
  }
}

// ============================================================
//  رله‌ی DNS با فریم‌بندی استاندارد VLESS UDP (پیشوند طول ۲ بایتی)
// ============================================================
async function handleUdpFrames(buf, ws) {
  let offset = 0;
  while (offset + 2 <= buf.length) {
    const len = (buf[offset] << 8) + buf[offset + 1];
    offset += 2;
    if (offset + len > buf.length) break;
    const packet = buf.slice(offset, offset + len);
    offset += len;

    try {
      const respBytes = await resolveDnsOverHttps(packet);
      const respLen = respBytes.length;
      const framed = new Uint8Array(2 + respLen);
      framed[0] = (respLen >> 8) & 0xff;
      framed[1] = respLen & 0xff;
      framed.set(respBytes, 2);
      ws.send(framed);
    } catch (_) {
      // این کوئری خاص را نادیده می‌گیریم؛ بقیه‌ی جریان ادامه پیدا می‌کند
    }
  }
}

async function resolveDnsOverHttps(queryBytes) {
  const res = await fetch("https://cloudflare-dns.com/dns-query", {
    method: "POST",
    headers: {
      "content-type": "application/dns-message",
      "accept": "application/dns-message",
    },
    body: queryBytes,
  });
  const buf = await res.arrayBuffer();
  return new Uint8Array(buf);
}
