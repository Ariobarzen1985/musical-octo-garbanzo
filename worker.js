import { connect } from 'cloudflare:sockets';

export default {
	async fetch(request, env, ctx) {
		try {
			const upgradeHeader = request.headers.get('Upgrade');
			if (!upgradeHeader || upgradeHeader !== 'websocket') {
				return new Response('Ariobarzen Worker is Active and Running!', { status: 200 });
			}
			return await handleWebSocket(request, env.UUID);
		} catch (err) {
			return new Response(err.toString(), { status: 500 });
		}
	},
};

async function handleWebSocket(request, userID) {
	const webSocketPair = new WebSocketPair();
	const [client, server] = Object.values(webSocketPair);

	server.accept();

	let address = '';
	let port = '';
	let decodedHeader = false;
	let tcpSocket = null;
	let writer = null;

	const logError = (msg) => {
		// خطاها مدیریت می‌شوند تا ورکر کرش نکند
	};

	server.addEventListener('message', async (event) => {
		try {
			const data = event.data;
			if (!decodedHeader) {
				const vlessResponse = parseVlessHeader(data, userID);
				if (!vlessResponse) {
					server.close();
					return;
				}
				address = vlessResponse.address;
				port = vlessResponse.port;
				decodedHeader = true;

				tcpSocket = connect({ hostname: address, port: port });
				writer = tcpSocket.writable.getWriter();

				await writer.write(vlessResponse.rawHeaderData);

				// خواندن داده‌ها از شبکه و ارسال به کلاینت
				ctxStreamToWebSocket(tcpSocket, server, logError);
			} else {
				if (writer) {
					await writer.write(data);
				}
			}
		} catch (err) {
			logError(err);
		}
	});

	server.addEventListener('close', () => {
		if (tcpSocket) tcpSocket.close();
	});

	return new Response(null, {
		status: 101,
		webSocket: client,
	});
}

function parseVlessHeader(buffer, userID) {
	const view = new DataView(buffer);
	if (view.byteLength < 24) return null;

	// چک کردن UUID
	const uuidBytes = new Uint8Array(buffer, 1, 16);
	const parsedUUID = bytesToUuid(uuidBytes);
	if (parsedUUID.toLowerCase() !== userID.toLowerCase()) {
		return null;
	}

	const optLength = view.getUint8(17);
	const command = view.getUint8(18 + optLength);
	
	// فرمان TCP = 1
	if (command !== 1) return null;

	let portIndex = 19 + optLength;
	const port = view.getUint16(portIndex);
	const addressType = view.getUint8(portIndex + 2);

	let addressIndex = portIndex + 3;
	let address = '';

	if (addressType === 1) { // IPv4
		address = Array.from(new Uint8Array(buffer, addressIndex, 4)).join('.');
		addressIndex += 4;
	} else if (addressType === 2) { // Domain
		const domainLength = view.getUint8(addressIndex);
		addressIndex += 1;
		address = new TextDecoder().decode(new Uint8Array(buffer, addressIndex, domainLength));
		addressIndex += domainLength;
	} else if (addressType === 3) { // IPv6
		const ipv6 = [];
		for (let i = 0; i < 8; i++) {
			ipv6.push(view.getUint16(addressIndex + i * 2).toString(16));
		}
		address = ipv6.join(':');
		addressIndex += 16;
	} else {
		return null;
	}

	const rawHeaderData = buffer.slice(addressIndex);
	return { address, port, rawHeaderData };
}

function bytesToUuid(byteArray) {
	const hex = Array.from(byteArray, (byte) => ('0' + (byte & 0xff).toString(16)).slice(-2));
	return [
		hex.slice(0, 4).join(''),
		hex.slice(4, 6).join(''),
		hex.slice(6, 8).join(''),
		hex.slice(8, 10).join(''),
		hex.slice(10, 16).join(''),
	].join('-');
}

async function ctxStreamToWebSocket(tcpSocket, webSocket, logError) {
	try {
		const reader = tcpSocket.readable.getReader();
		while (true) {
			const { value, done } = await reader.read();
			if (done) break;
			if (value) {
				webSocket.send(value);
			}
		}
	} catch (error) {
		logError(error);
	} finally {
		try {
			webSocket.close();
		} catch (e) {}
	}
}
