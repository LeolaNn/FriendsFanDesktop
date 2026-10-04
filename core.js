// Передача файлу через data channel: хост віддає шматки, глядач віддає їх плеєру по локальному HTTP.
const fs = require('fs'), path = require('path'), http = require('http');
const FRAME = 64 * 1024, CHUNK = 4 * 1024 * 1024, AHEAD = 3;

function hostChannel(dc, filePath) {
  const size = fs.statSync(filePath).size, name = path.basename(filePath);
  const active = new Set();
  dc.binaryType = 'arraybuffer';
  try { dc.bufferedAmountLowThreshold = 2 * 1024 * 1024; } catch {}
  dc.onmessage = (e) => {
    if (typeof e.data !== 'string') return;
    const m = JSON.parse(e.data);
    if (m.t === 'meta') dc.send(JSON.stringify({ t: 'meta', size, name }));
    else if (m.t === 'cancel') active.delete(m.id);
    else if (m.t === 'get') serve(m.id, m.start, m.end).catch(() => active.delete(m.id));
  };
  async function serve(id, start, end) {
    active.add(id);
    const fh = await fs.promises.open(filePath, 'r');
    try {
      let pos = start;
      while (pos <= end && active.has(id)) {
        const n = Math.min(FRAME, end - pos + 1);
        const buf = Buffer.allocUnsafe(4 + n);
        buf.writeUInt32BE(id, 0);
        const { bytesRead } = await fh.read(buf, 4, n, pos);
        if (!bytesRead) break;
        while (dc.bufferedAmount > 8 * 1024 * 1024 && active.has(id))
          await new Promise(r => dc.addEventListener('bufferedamountlow', r, { once: true }));
        if (!active.has(id)) break;
        dc.send(buf.subarray(0, 4 + bytesRead));
        pos += bytesRead;
      }
    } finally { await fh.close(); }
    if (active.delete(id)) dc.send(JSON.stringify({ t: 'done', id }));
  }
}

const MIME = { mkv: 'video/x-matroska', mp4: 'video/mp4', webm: 'video/webm', avi: 'video/x-msvideo', mov: 'video/quicktime' };

function viewerChannel(dc) {
  dc.binaryType = 'arraybuffer';
  let meta = null, metaRes = null, nextId = 1, total = 0;
  const pending = new Map();
  dc.onmessage = (e) => {
    if (typeof e.data === 'string') {
      const m = JSON.parse(e.data);
      if (m.t === 'meta') { meta = m; if (metaRes) metaRes(m); }
      else if (m.t === 'done') { const p = pending.get(m.id); pending.delete(m.id); if (p) p.done(); }
    } else {
      const b = Buffer.from(e.data), p = pending.get(b.readUInt32BE(0)); total += b.length;
      if (p) p.data(b.subarray(4));
    }
  };
  const getMeta = () => new Promise(res => { metaRes = res; dc.send(JSON.stringify({ t: 'meta' })); });
  function fetchRange(start, end, onChunk) {
    const id = nextId++;
    const promise = new Promise(res => pending.set(id, { data: onChunk, done: res }));
    dc.send(JSON.stringify({ t: 'get', id, start, end }));
    return { promise, cancel: () => { if (pending.delete(id)) dc.send(JSON.stringify({ t: 'cancel', id })); } };
  }
  function makeServer() {
    return http.createServer(async (req, res) => {
      const size = meta.size; let start = 0, end = size - 1, code = 200;
      const r = /bytes=(\d*)-(\d*)/.exec(req.headers.range || '');
      if (r) {
        code = 206;
        if (r[1] === '') start = Math.max(0, size - parseInt(r[2], 10));
        else { start = parseInt(r[1], 10); if (r[2] !== '') end = Math.min(end, parseInt(r[2], 10)); }
      }
      if (start > end || start >= size) { res.writeHead(416, { 'Content-Range': 'bytes */' + size }); return res.end(); }
      const h = { 'Accept-Ranges': 'bytes', 'Content-Length': end - start + 1,
        'Content-Type': MIME[path.extname(meta.name).slice(1).toLowerCase()] || 'application/octet-stream' };
      if (code === 206) h['Content-Range'] = `bytes ${start}-${end}/${size}`;
      res.writeHead(code, h);
      if (req.method === 'HEAD') return res.end();
      let aborted = false; const jobs = []; let next = start;
      const launch = () => {
        while (jobs.length < AHEAD && next <= end && !aborted) {
          const e2 = Math.min(end, next + CHUNK - 1), job = { parts: [], head: false };
          job.cur = fetchRange(next, e2, b => { const buf = Buffer.from(b); if (job.head) res.write(buf); else job.parts.push(buf); });
          jobs.push(job); next = e2 + 1;
        }
      };
      res.on('close', () => { aborted = true; jobs.forEach(j => j.cur.cancel()); });
      launch();
      while (jobs.length && !aborted) {
        const job = jobs[0];
        job.head = true;
        if (job.parts.length) { res.write(Buffer.concat(job.parts)); job.parts = []; }
        await job.cur.promise;
        jobs.shift(); launch();
      }
      res.end();
    });
  }
  return { getMeta, makeServer, bytes: () => total };
}
module.exports = { hostChannel, viewerChannel };
