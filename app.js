const { ipcRenderer } = require('electron');
const { spawn } = require('child_process');
const net = require('net'), fs = require('fs'), path = require('path');
const core = require('./core');
const $ = id => document.getElementById(id);
const log = t => { const d = document.createElement('div'); d.textContent = new Date().toLocaleTimeString() + '  ' + t; $('log').prepend(d); };

window.addEventListener('error', e => log('Помилка: ' + e.message));
window.addEventListener('unhandledrejection', e => log('Помилка: ' + (e.reason && e.reason.message || e.reason)));
$('srv').value = localStorage.srv || ''; $('room').value = localStorage.room || '';
let ws = null, room = '', myId = null, isHost = false, filePath = null, retry = null, srvUrl = '';
let ICE = { iceServers: [{ urls: 'stun:stun.l.google.com:19302' }] };
const pcs = new Map(); let viewerSrv = null, curV = null, lastBytes = 0, lastT = Date.now();
const tx = o => { if (ws && ws.readyState === 1) ws.send(JSON.stringify(o)); };

/* ---------- mpv ---------- */
let mpv = null, paused = true, ignoreUntil = 0, remoteSeeks = 0, lastRemoteSeek = 0, localSeek = false, reqId = 1, hbTimer = null;
const cbs = new Map();
let volIgnoreUntil = 0, volTimer = null;
function mpvExe() {
  const saved = localStorage.mpvPath;
  if (saved && fs.existsSync(saved) && /mpv/i.test(path.basename(saved))) return saved;
  if (saved) log('Збережений шлях не схожий на mpv і ігнорується: ' + saved);
  const c = [path.join(process.resourcesPath || '', 'mpv', 'mpv.exe'), path.join(__dirname, 'mpv', 'mpv.exe')];
  for (const p of c) if (fs.existsSync(p)) return p;
  return 'mpv';
}
function killMpv() {
  clearInterval(hbTimer);
  if (mpv) { try { mpv.sock && mpv.sock.destroy(); } catch {} try { mpv.proc.kill(); } catch {} mpv = null; }
}
function ipc(cmd, cb) {
  if (!mpv || !mpv.sock) return;
  const id = reqId++; if (cb) cbs.set(id, cb);
  mpv.sock.write(JSON.stringify({ command: cmd, request_id: id }) + '\n');
}
const getTime = cb => ipc(['get_property', 'time-pos'], r => cb(typeof r.data === 'number' ? r.data : 0));
const broadcast = a => getTime(t => tx({ type: 'sync', a, t }));
const heartbeat = () => getTime(t => tx({ type: 'sync', a: 'hb', p: paused, t }));

function launchMpv(src) {
  killMpv();
  const pipe = process.platform === 'win32' ? '\\\\.\\pipe\\watchparty-' + process.pid + '-' + Date.now() : '/tmp/watchparty-' + process.pid + '.sock';
  const exe = mpvExe();
  log('Запускаю mpv: ' + exe);
  let proc;
  try {
    proc = spawn(exe, ['--input-ipc-server=' + pipe, '--pause', '--force-window=yes', '--keep-open=yes', '--hwdec=auto-safe',
      '--cache=yes', '--demuxer-max-bytes=300MiB', '--demuxer-readahead-secs=180', '--title=Спільний перегляд', src], { stdio: ['ignore', 'pipe', 'pipe'] });
  } catch (err) { return log('Помилка запуску mpv: ' + err.message); }
  mpv = { proc, sock: null }; paused = true;
  const out = d => String(d).split('\n').forEach(l => { if (/error|fail|cannot|unable|invalid/i.test(l)) log('mpv: ' + l.trim().slice(0, 200)); });
  proc.stdout.on('data', out); proc.stderr.on('data', out);
  proc.on('spawn', () => log('mpv запущено (pid ' + proc.pid + ')'));
  proc.on('error', e => log('Не вдалося запустити mpv (' + e.code + '). Натисніть «Шлях до mpv…» і вкажіть mpv.exe'));
  proc.on('exit', (code, sig) => { if (mpv && mpv.proc === proc) { mpv = null; clearInterval(hbTimer); log('mpv завершився (код ' + code + (sig ? ', ' + sig : '') + ')'); } });
  connectPipe(pipe, proc, 0);
}
function connectPipe(pipe, proc, tries) {
  const s = net.connect(pipe); let buf = '';
  s.on('connect', () => {
    if (!mpv || mpv.proc !== proc) return s.destroy();
    mpv.sock = s; ipc(['observe_property', 1, 'pause']); ipc(['observe_property', 2, 'volume']);
    volIgnoreUntil = Date.now() + 1000; ipc(['set_property', 'volume', +$('vol').value]);
    if (isHost) hbTimer = setInterval(heartbeat, 4000);
  });
  s.on('data', d => { buf += d; let i; while ((i = buf.indexOf('\n')) >= 0) { const l = buf.slice(0, i); buf = buf.slice(i + 1); try { onMpv(JSON.parse(l)); } catch {} } });
  s.on('error', () => { if (tries < 80 && mpv && mpv.proc === proc) setTimeout(() => connectPipe(pipe, proc, tries + 1), 250); });
}
function onMpv(e) {
  if (e.request_id && cbs.has(e.request_id)) { cbs.get(e.request_id)(e); cbs.delete(e.request_id); return; }
  if (e.event === 'property-change' && e.name === 'pause') { paused = e.data; if (Date.now() > ignoreUntil) broadcast(paused ? 'pause' : 'play'); }
  else if (e.event === 'property-change' && e.name === 'volume' && typeof e.data === 'number') {
    $('vol').value = e.data; $('volv').textContent = Math.round(e.data);
    if ($('volsync').checked && Date.now() > volIgnoreUntil) { clearTimeout(volTimer); volTimer = setTimeout(() => tx({ type: 'sync', a: 'vol', t: e.data }), 100); }
  }
  else if (e.event === 'seek') { if (remoteSeeks > 0 && Date.now() - lastRemoteSeek > 6000) remoteSeeks = 0; if (!remoteSeeks) localSeek = true; }
  else if (e.event === 'playback-restart') { if (remoteSeeks > 0) remoteSeeks--; else if (localSeek) { localSeek = false; broadcast('seek'); } }
}
function applySync(m) {
  if (m.a === 'vol') {
    if (!$('volsync').checked || !mpv || !mpv.sock) return;
    volIgnoreUntil = Date.now() + 600; ipc(['set_property', 'volume', m.t]); return;
  }
  if (!mpv || !mpv.sock || (m.a === 'hb' && isHost)) return;
  getTime(cur => {
    if (m.a !== 'seek') {
      const wantPause = m.a === 'pause' || (m.a === 'hb' && m.p);
      if (wantPause !== paused) { ignoreUntil = Date.now() + 800; ipc(['set_property', 'pause', wantPause]); }
    }
    if (m.a === 'seek' || Math.abs(cur - m.t) > (m.a === 'hb' ? 2.5 : 1)) {
      remoteSeeks++; lastRemoteSeek = Date.now(); ipc(['seek', m.t, 'absolute+exact']);
    }
  });
}

/* ---------- мережа ---------- */
async function statTick() {
  if (isHost || !curV) return;
  const now = Date.now(), b = curV.bytes();
  const mbps = ((b - lastBytes) * 8 / 1e6) / Math.max(0.5, (now - lastT) / 1000); lastBytes = b; lastT = now;
  let conn = '?';
  try {
    const pc = pcs.get('c'), st = await pc.getStats(); let sel;
    st.forEach(r => { if (r.type === 'transport' && r.selectedCandidatePairId) sel = st.get(r.selectedCandidatePairId); });
    if (!sel) st.forEach(r => { if (r.type === 'candidate-pair' && r.nominated && r.state === 'succeeded') sel = r; });
    if (sel) { const l = st.get(sel.localCandidateId), rm = st.get(sel.remoteCandidateId);
      conn = ((l && l.candidateType === 'relay') || (rm && rm.candidateType === 'relay')) ? 'через TURN (повільніше)' : 'напряму (P2P)'; }
  } catch {}
  const show = c => { $('stat').textContent = 'Швидкість: ' + mbps.toFixed(1) + ' Мбіт/с · Кеш: ' + c + ' с · З\'єднання: ' + conn; };
  if (!mpv || !mpv.sock) return show('?');
  ipc(['get_property', 'demuxer-cache-duration'], r => show(typeof r.data === 'number' ? r.data.toFixed(0) : '?'));
}
setInterval(statTick, 2000);
function closePeers() { pcs.forEach(p => p.close()); pcs.clear(); }
function stopAll() { closePeers(); killMpv(); if (viewerSrv) { viewerSrv.close(); viewerSrv = null; } }

$('join').onclick = async () => {
  srvUrl = $('srv').value.trim().replace(/\/+$/, '');
  room = $('room').value.trim().toLowerCase().replace(/[^a-z0-9_-]/g, '');
  if (!/^https?:\/\//.test(srvUrl)) return log('Вкажіть адресу сервера, наприклад https://назва.onrender.com');
  if (!room) return log('Вкажіть код кімнати (латиниця/цифри)');
  localStorage.srv = srvUrl; localStorage.room = room;
  log('Підключаюсь… (безкоштовний сервер може прокидатися до хвилини)');
  try { const r = await fetch(srvUrl + '/ice'); ICE = { iceServers: await r.json() }; log(JSON.stringify(ICE).includes('turn:') ? 'TURN підключено' : 'TURN не налаштовано (лише STUN)'); }
  catch { log('Не вдалося отримати налаштування ICE'); }
  connect();
};
function connect() {
  clearTimeout(retry);
  if (ws) { ws.onclose = null; ws.close(); }
  ws = new WebSocket(srvUrl.replace(/^http/, 'ws'));
  ws.onopen = () => { tx({ type: 'join', room }); $('status').textContent = 'Підключено: ' + room; };
  ws.onclose = () => { $('status').textContent = 'Перепідключення…'; retry = setTimeout(connect, 2000); };
  ws.onmessage = e => {
    let m; try { m = JSON.parse(e.data); } catch { return; }
    if (m.type === 'hello') myId = m.id;
    else if (m.type === 'peers') $('peers').textContent = 'У кімнаті: ' + m.n;
    else if (m.type === 'caster') {
      if (m.id && m.id !== myId) { isHost = false; stopAll(); log('У кімнаті з\'явився хост'); }
      else if (!m.id && !isHost) { stopAll(); log('Хост вийшов'); }
    }
    else if (m.type === 'viewer' && isHost) startPeer(m.id);
    else if (m.type === 'viewers' && isHost) m.ids.forEach(startPeer);
    else if (m.type === 'gone') { const p = pcs.get(m.id); if (p) { p.close(); pcs.delete(m.id); } }
    else if (m.type === 'signal') onSignal(m.from, m.data);
    else if (m.type === 'sync') applySync(m);
  };
}

$('host').onclick = async () => {
  if (!ws || ws.readyState !== 1) return log('Спершу увійдіть в кімнату');
  const p = await ipcRenderer.invoke('pick'); if (!p) return;
  stopAll(); filePath = p; isHost = true; tx({ type: 'cast' });
  log('Ви хост: ' + path.basename(p) + '. Натисніть Пробіл у mpv, коли всі підключаться');
  launchMpv(p);
};
$('vol').oninput = () => { $('volv').textContent = $('vol').value; ipc(['set_property', 'volume', +$('vol').value]); };
$('mpvp').onclick = async () => {
  const p = await ipcRenderer.invoke('pick', [{ name: 'mpv', extensions: ['exe'] }, { name: 'Усі файли', extensions: ['*'] }]);
  if (p) { localStorage.mpvPath = p; log('Шлях до mpv збережено'); }
};

function startPeer(id) {
  if (pcs.has(id)) pcs.get(id).close();
  const pc = new RTCPeerConnection(ICE); pcs.set(id, pc);
  core.hostChannel(pc.createDataChannel('f'), filePath);
  pc.onicecandidate = e => e.candidate && tx({ type: 'signal', to: id, data: { ice: e.candidate } });
  pc.onnegotiationneeded = async () => { await pc.setLocalDescription(); tx({ type: 'signal', to: id, data: { sdp: pc.localDescription } }); };
  pc.onconnectionstatechange = () => { if (pc.connectionState === 'failed') log('Не вдалося з\'єднатися з глядачем (мережа/NAT, потрібен TURN)'); };
}
async function onSignal(from, d) {
  let pc = pcs.get(isHost ? from : 'c');
  if (d.sdp) {
    if (d.sdp.type === 'offer') {
      if (!pc) {
        pc = new RTCPeerConnection(ICE); pcs.set('c', pc);
        pc.onicecandidate = e => e.candidate && tx({ type: 'signal', to: from, data: { ice: e.candidate } });
        pc.ondatachannel = e => {
          const dc = e.channel, v = core.viewerChannel(dc); curV = v; lastBytes = 0; lastT = Date.now();
          const go = async () => {
            try {
              const meta = await v.getMeta();
              if (viewerSrv) viewerSrv.close();
              viewerSrv = v.makeServer();
              await new Promise(r => viewerSrv.listen(0, '127.0.0.1', r));
              log('Відкриваю в mpv: ' + meta.name);
              launchMpv('http://127.0.0.1:' + viewerSrv.address().port + '/' + encodeURIComponent(meta.name));
            } catch (err) { log('Помилка: ' + err.message); }
          };
          dc.readyState === 'open' ? go() : dc.addEventListener('open', go);
        };
      }
      await pc.setRemoteDescription(d.sdp); await pc.setLocalDescription();
      tx({ type: 'signal', to: from, data: { sdp: pc.localDescription } });
    } else if (pc) await pc.setRemoteDescription(d.sdp);
  } else if (d.ice && pc) { try { await pc.addIceCandidate(d.ice); } catch {} }
}
