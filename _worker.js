const WS_READY_STATE_OPEN = 1;
const WS_READY_STATE_CLOSING = 2;

// cmliu 优质 ProxyIP（2025年12月仍在活跃维护，大陆访问极佳）
const PROXY_IPS = [
  'ProxyIP.SG.CMLiussss.net:443',  // 新加坡（优先，最稳定低延迟）
  'ProxyIP.HK.CMLiussss.net:443',  // 香港
  'ProxyIP.JP.CMLiussss.net:443',  // 日本
  'ProxyIP.US.CMLiussss.net:443',  // 美国
  'ProxyIP.KR.CMLiussss.net:443',  // 韩国（备选）
];

const encoder = new TextEncoder();

import { connect } from 'cloudflare:sockets';

export default {
  async fetch(request) {
    try {
      const url = new URL(request.url);
      const upgradeHeader = request.headers.get('Upgrade');

      // 非 WebSocket 请求：返回友好页面
      if (!upgradeHeader || upgradeHeader.toLowerCase() !== 'websocket') {
        return new Response(
          `<h1>WebSocket Proxy Running</h1>
          <p>Worker 运行正常，仅支持 WebSocket 连接。</p>
          <p>地址: wss://${url.hostname}</p>
          <p>Token: qwe123</p>
          <hr>
          <small>Time: ${new Date().toISOString()} | 使用 cmliu ProxyIP 优化</small>`,
          {
            status: 200,
            headers: { 'Content-Type': 'text/html; charset=utf-8' }
          }
        );
      }

      // Token 验证（可自行修改）
      const token = 'qwe123';

      if (token && request.headers.get('Sec-WebSocket-Protocol') !== token) {
        return new Response('Unauthorized: Invalid token', { status: 401 });
      }

      const webSocketPair = new WebSocketPair();
      const [client, server] = Object.values(webSocketPair);
      server.accept();

      handleSession(server).catch((err) => {
        console.error('handleSession error:', err);
        safeCloseWebSocket(server);
      });

      const responseHeaders = new Headers();
      if (token) {
        responseHeaders.set('Sec-WebSocket-Protocol', token);
      }

      return new Response(null, {
        status: 101,
        webSocket: client,
        headers: responseHeaders
      });

    } catch (err) {
      console.error('fetch error:', err);
      return new Response(`Internal Error: ${err.message || err}`, { status: 500 });
    }
  },
};

async function handleSession(webSocket) {
  let remoteSocket = null;
  let remoteWriter = null;
  let remoteReader = null;
  let isClosed = false;

  const cleanup = () => {
    if (isClosed) return;
    isClosed = true;

    try { remoteWriter?.releaseLock(); } catch {}
    try { remoteReader?.releaseLock(); } catch {}
    try { remoteSocket?.close(); } catch {}

    remoteSocket = remoteWriter = remoteReader = null;
    safeCloseWebSocket(webSocket);
  };

  const pumpRemoteToWebSocket = async () => {
    if (!remoteReader) return;
    try {
      while (!isClosed) {
        const { done, value } = await remoteReader.read();
        if (done) break;
        if (webSocket.readyState !== WS_READY_STATE_OPEN) break;
        if (value && value.byteLength > 0) {
          webSocket.send(value);
        }
      }
    } catch (e) {
      console.error('pump error:', e);
    } finally {
      cleanup();
    }
  };

  const parseAddress = (addr) => {
    try {
      if (addr.startsWith('[')) {
        const end = addr.indexOf(']');
        if (end === -1) throw new Error('Invalid IPv6');
        return {
          host: addr.substring(1, end),
          port: parseInt(addr.substring(end + 2), 10)
        };
      }
      const sep = addr.lastIndexOf(':');
      if (sep === -1) throw new Error('Invalid address');
      return {
        host: addr.substring(0, sep),
        port: parseInt(addr.substring(sep + 1), 10)
      };
    } catch {
      throw new Error('Address parse failed');
    }
  };

  const connectToRemote = async (targetAddr, firstFrameData) => {
    let host, port;
    try {
      ({ host, port } = parseAddress(targetAddr));
    } catch {
      try { webSocket.send('ERROR:Invalid address'); } catch {}
      return;
    }

    // 连接顺序：1. 原始域名（最重要，能访问推特等 CF 站点） 2. cmliu ProxyIP 加速
    const attempts = [host, ...PROXY_IPS];

    for (const attempt of attempts) {
      if (isClosed) return;

      // ProxyIP 需要解析成 host + port
      let connectHost = attempt;
      let connectPort = port;
      if (attempt.includes(':')) {
        const parts = attempt.split(':');
        connectHost = parts[0];
        connectPort = parseInt(parts[1], 10);
      }

      try {
        remoteSocket = connect({
          hostname: connectHost,
          port: connectPort
        });

        await remoteSocket.opened;

        remoteWriter = remoteSocket.writable.getWriter();
        remoteReader = remoteSocket.readable.getReader();

        if (firstFrameData) {
          await remoteWriter.write(encoder.encode(firstFrameData));
        }

        webSocket.send('CONNECTED');
        pumpRemoteToWebSocket();
        return;

      } catch (err) {
        console.error(`Connect failed: \( {connectHost}: \){connectPort}`, err);

        try { remoteWriter?.releaseLock(); } catch {}
        try { remoteReader?.releaseLock(); } catch {}
        try { remoteSocket?.close(); } catch {}
        remoteSocket = remoteWriter = remoteReader = null;
      }
    }

    // 所有尝试失败
    try { webSocket.send('ERROR:All attempts failed'); } catch {}
    cleanup();
  };

  webSocket.addEventListener('message', async (event) => {
    if (isClosed) return;

    try {
      const data = event.data;

      if (typeof data === 'string') {
        if (data.startsWith('CONNECT:')) {
          const sep = data.indexOf('|', 8);
          if (sep === -1) return;
          const addr = data.substring(8, sep);
          const payload = data.substring(sep + 1);
          await connectToRemote(addr, payload);
        } else if (data.startsWith('DATA:')) {
          if (remoteWriter) {
            await remoteWriter.write(encoder.encode(data.substring(5)));
          }
        } else if (data === 'CLOSE') {
          cleanup();
        }
      } else if (data instanceof ArrayBuffer && remoteWriter) {
        await remoteWriter.write(new Uint8Array(data));
      }
    } catch (err) {
      console.error('message error:', err);
      try { webSocket.send('ERROR:' + (err.message || 'Unknown')); } catch {}
      cleanup();
    }
  });

  webSocket.addEventListener('close', cleanup);
  webSocket.addEventListener('error', (e) => {
    console.error('WebSocket error:', e);
    cleanup();
  });
}

function safeCloseWebSocket(ws) {
  try {
    if (ws.readyState === WS_READY_STATE_OPEN || ws.readyState === WS_READY_STATE_CLOSING) {
      ws.close(1000, 'Normal closure');
    }
  } catch {}
		  }
