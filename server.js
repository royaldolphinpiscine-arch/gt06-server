// server.js — Serveur TCP GT06 pour trackers SinoTrack ST903
const net = require('net');

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_KEY;

async function db(method, table, body, params = '') {
  const headers = {
    'apikey': SUPABASE_KEY,
    'Authorization': `Bearer ${SUPABASE_KEY}`,
    'Content-Type': 'application/json',
    'Prefer': 'return=minimal',
  };
  if (method === 'POST') headers['Prefer'] = 'resolution=merge-duplicates,return=minimal';
  const res = await fetch(`${SUPABASE_URL}/rest/v1/${table}${params}`, {
    method, headers, body: body ? JSON.stringify(body) : undefined
  });
  if (!res.ok) console.error(`DB ${method} ${table}:`, await res.text());
}

const sessions = new Map();

function bcdToImei(bytes) {
  let imei = '';
  for (let i = 0; i < 8; i++) {
    imei += (bytes[i] >> 4).toString() + (bytes[i] & 0xF).toString();
  }
  return imei.slice(0, 15);
}

function parsePackets(buf) {
  const packets = [];
  let i = 0;
  while (i + 4 < buf.length) {
    if (buf[i] !== 0x78 || buf[i + 1] !== 0x78) { i++; continue; }
    const L = buf[i + 2];
    if (i + L + 5 > buf.length) break;
    const proto = buf[i + 3];
    const dataLen = Math.max(0, L - 5);
    const data = Buffer.from(buf.slice(i + 4, i + 4 + dataLen));
    packets.push({ proto, data, end: i + L + 5 });
    i += L + 5;
  }
  const consumed = packets.length ? packets[packets.length - 1].end : i;
  return { packets, remaining: buf.slice(consumed) };
}

function ack(proto) {
  const buf = Buffer.alloc(10);
  buf[0] = 0x78; buf[1] = 0x78; buf[2] = 0x05;
  buf[3] = proto;
  buf[4] = 0x00; buf[5] = 0x01;
  buf[6] = 0x00; buf[7] = 0x00;
  buf[8] = 0x0D; buf[9] = 0x0A;
  return buf;
}

async function handleLogin(data, socket) {
  const imei = bcdToImei(data);
  sessions.set(socket, { imei });
  console.log(`✅ Login: ${imei}`);
  const now = new Date().toISOString();
  await db('POST', 'devices', { imei, name: `Tracker ${imei.slice(-4)}`, status: 'online', last_seen: now }, '?on_conflict=imei');
  await db('PATCH', 'devices', { status: 'online', last_seen: now }, `?imei=eq.${imei}`);
  socket.write(ack(0x01));
}

async function handleGPS(data, socket) {
  const s = sessions.get(socket);
  if (!s || data.length < 18) return;
  const year = 2000 + data[0];
  const month = data[1];
  const day = data[2];
  const hour = data[3];
  const min = data[4];
  const sec = data[5];
  const satellites = data[6] & 0x0F;
  const latRaw = data.readUInt32BE(7);
  const lonRaw = data.readUInt32BE(11);
  let speed = data[15];
  const cs = data.readUInt16BE(16);
  const course = cs & 0x03FF;
  const northLat = (cs >> 10) & 1;
  const eastLon = (cs >> 11) & 1;
  let lat = latRaw / 1800000.0;
  let lon = lonRaw / 1800000.0;
  if (!northLat) lat = -lat;
  if (!eastLon) lon = -lon;
  const timestamp = new Date(Date.UTC(year, month - 1, day, hour, min, sec)).toISOString();
  const now = new Date().toISOString();
  console.log(`📍 ${s.imei}: ${lat.toFixed(5)}, ${lon.toFixed(5)} @ ${speed} km/h`);
  await Promise.all([
    db('POST', 'positions', { imei: s.imei, latitude: lat, longitude: lon, speed, course, satellites, timestamp }),
    db('PATCH', 'devices', { last_lat: lat, last_lon: lon, last_speed: speed, last_course: course, last_seen: now, status: 'online' }, `?imei=eq.${s.imei}`)
  ]);
  socket.write(ack(0x12));
}

async function handleHeartbeat(data, socket) {
  const s = sessions.get(socket);
  if (!s || data.length < 1) return;
  const voltage = data[0];
  const battery = Math.min(100, Math.round((voltage / 6) * 100));
  await db('PATCH', 'devices', { battery, status: 'online', last_seen: new Date().toISOString() }, `?imei=eq.${s.imei}`);
  socket.write(ack(0x13));
}

const server = net.createServer(socket => {
  let buf = Buffer.alloc(0);
  console.log(`🔌 Connecté: ${socket.remoteAddress}`);
  socket.on('data', async chunk => {
    buf = Buffer.concat([buf, chunk]);
    const { packets, remaining } = parsePackets(buf);
    buf = remaining;
    for (const { proto, data } of packets) {
      if (proto === 0x01) await handleLogin(data, socket);
      else if (proto === 0x12 || proto === 0x22) await handleGPS(data, socket);
      else if (proto === 0x13) await handleHeartbeat(data, socket);
    }
  });
  socket.on('close', () => {
    const s = sessions.get(socket);
    if (s) {
      db('PATCH', 'devices', { status: 'offline' }, `?imei=eq.${s.imei}`).catch(() => {});
      sessions.delete(socket);
    }
  });
  socket.on('error', err => { console.error('Erreur:', err.message); sessions.delete(socket); });
  socket.setTimeout(300000);
  socket.on('timeout', () => socket.destroy());
});

const PORT = process.env.PORT || 5023;
server.listen(PORT, '0.0.0.0', () => {
  console.log(`🚢 Serveur GT06 sur port ${PORT}`);
});
