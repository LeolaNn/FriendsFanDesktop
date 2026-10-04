// Створення легших версій серії через ffmpeg. pct = % від розміру (бітрейту) оригіналу.
const { spawn } = require('child_process'), fs = require('fs'), path = require('path'), os = require('os');
const AB = { 90: 192, 75: 160, 50: 128, 10: 64 };
let current = null, cancelled = false;

function ffmpegExe() {
  const c = [path.join(process.resourcesPath || '', 'ffmpeg', 'ffmpeg.exe'), path.join(__dirname, 'ffmpeg', 'ffmpeg.exe')];
  for (const p of c) if (fs.existsSync(p)) return p;
  return 'ffmpeg';
}
function duration(file) {
  return new Promise(res => {
    let err = '';
    const p = spawn(ffmpegExe(), ['-hide_banner', '-i', file]);
    p.stderr.on('data', d => err += d);
    p.on('error', () => res(0));
    p.on('close', () => { const m = /Duration:\s*(\d+):(\d+):(\d+\.?\d*)/.exec(err); res(m ? (+m[1]) * 3600 + (+m[2]) * 60 + parseFloat(m[3]) : 0); });
  });
}
async function encode(file, pct, onProgress) {
  cancelled = false;
  const dur = await duration(file);
  if (!dur) throw new Error('не вдалося прочитати файл (ffmpeg не знайдено?)');
  const size = fs.statSync(file).size, totalBps = size * 8 / dur, ab = AB[pct] || 128;
  const vk = Math.max(120, Math.round((totalBps * pct / 100 - ab * 1000) / 1000));
  const dir = path.join(os.tmpdir(), 'watchparty-cache'); fs.mkdirSync(dir, { recursive: true });
  const base = path.basename(file).replace(/\.[^.]+$/, '').replace(/[^\w\-. ]+/g, '_');
  const out = path.join(dir, `${base}.${size}.p${pct}.mkv`), part = out + '.part.mkv';
  if (fs.existsSync(out) && fs.statSync(out).size > 0) return out;
  const scale = pct <= 10 ? ['-vf', 'scale=-2:min(480\\,ih)'] : pct <= 50 ? ['-vf', 'scale=-2:min(1080\\,ih)'] : [];
  const run = (full) => new Promise((resolve, reject) => {
    const args = ['-y', '-hide_banner', '-loglevel', 'error', '-i', file, '-map', '0:v:0', '-map', '0:a?',
      ...(full ? ['-map', '0:s?', '-map', '0:t?'] : []),
      '-c:v', 'libx264', '-preset', 'veryfast', '-b:v', vk + 'k', '-maxrate', Math.round(vk * 1.3) + 'k', '-bufsize', vk * 3 + 'k',
      '-pix_fmt', 'yuv420p', ...scale, '-c:a', 'aac', '-b:a', ab + 'k',
      ...(full ? ['-c:s', 'copy', '-c:t', 'copy'] : []), '-progress', 'pipe:1', '-nostats', part];
    const p = spawn(ffmpegExe(), args); current = p; let err = '';
    p.stdout.on('data', d => {
      const l = String(d).split('\n').filter(x => x.startsWith('out_time_us=') || x.startsWith('out_time_ms=')).pop();
      if (l) onProgress(Math.min(99, Math.round(parseInt(l.split('=')[1], 10) / 1e6 / dur * 100)));
    });
    p.stderr.on('data', d => err += d);
    p.on('error', reject);
    p.on('close', c => c === 0 ? resolve() : reject(new Error(cancelled ? 'скасовано' : (err.trim().split('\n').pop() || 'код ' + c))));
  });
  try { await run(true); } catch (e) { if (cancelled) throw e; await run(false); }
  fs.renameSync(part, out);
  return out;
}
function cancel() { cancelled = true; if (current) try { current.kill(); } catch {} }
module.exports = { encode, cancel };
