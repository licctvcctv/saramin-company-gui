const REGIONS = [
  { code: '101000', name: '首尔' },
  { code: '108000', name: '仁川' },
  { code: '104000', name: '大邱' },
  { code: '104100', name: '釜山' },
  { code: '105000', name: '江原道' },
  { code: '107000', name: '光州' },
  { code: '116000', name: '大田' },
  { code: '103000', name: '京畿道' },
  { code: '109000', name: '蔚山' },
];

const BASE_URL =
  'https://www.saramin.co.kr/zf_user/jobs/list/domestic?loc_mcd=101000&panel_type=&search_optional_item=n&search_done=y&panel_count=y&preview=y';

const form = document.getElementById('collectForm');
const regionWrap = document.getElementById('regionWrap');
const startBtn = document.getElementById('startBtn');
const stopBtn = document.getElementById('stopBtn');
const statusText = document.getElementById('statusText');
const logArea = document.getElementById('logArea');
const links = document.getElementById('links');
const jitterRange = document.getElementById('jitterRange');
const jitterValue = document.getElementById('jitterValue');

const DEFAULTS = {
  source: 'saramin',
  start_page: 1,
  max_pages: 120,
  page_size: 50,
  workers: 6,
  max_items: 1000,
  sleep: 0.08,
  jitter: 0.15,
  save_every: 200,
  save_interval: 3,
  fsync_every: 50,
  split_every: 20000,
};

function toNumber(v, fallback) {
  const n = Number(v);
  return Number.isFinite(n) ? n : fallback;
}

function toNonNegativeInt(v, fallback) {
  const n = Math.trunc(toNumber(v, fallback));
  return Math.max(0, n);
}

function toPositiveInt(v, fallback) {
  const n = Math.trunc(toNumber(v, fallback));
  return Math.max(1, n);
}

function renderRegionCards() {
  regionWrap.innerHTML = REGIONS.map((item) => {
    const safeName = item.name.replace(/[&<>"']/g, '');
    return `
      <label class="region-item">
        <input type="checkbox" name="locations" value="${item.code}" />
        <span>${safeName}</span>
      </label>
    `;
  }).join('');
}

function getSelectedLocations() {
  const values = [...document.querySelectorAll('input[name="locations"]')]
    .filter((el) => el.checked)
    .map((el) => el.value);

  if (!values.length) {
    return ['101000'];
  }
  return values;
}

function getPayload() {
  const formData = new FormData(form);
  const rawLocations = getSelectedLocations();
  const jitter = Math.max(0, toNumber(formData.get('jitter'), DEFAULTS.jitter));

  const payload = {
    source: DEFAULTS.source,
    start_page: DEFAULTS.start_page,
    max_pages: toPositiveInt(formData.get('max_pages'), DEFAULTS.max_pages),
    max_items: toNonNegativeInt(formData.get('max_items'), DEFAULTS.max_items),
    page_size: DEFAULTS.page_size,
    workers: toPositiveInt(formData.get('workers'), DEFAULTS.workers),
    sleep: DEFAULTS.sleep,
    jitter,
    save_every: DEFAULTS.save_every,
    save_interval: DEFAULTS.save_interval,
    fsync_every: DEFAULTS.fsync_every,
    split_every: DEFAULTS.split_every,
    verbose: false,
    url: BASE_URL,
    locations: rawLocations,
  };

  const ts = new Date().toISOString().replace(/[^0-9]/g, '').slice(0, 14);
  payload.output_csv = `outputs/saramin_jobs_${ts}.csv`;
  payload.output_xlsx = `outputs/saramin_jobs_${ts}.xlsx`;
  return payload;
}

async function startJob(event) {
  event.preventDefault();
  const payload = getPayload();

  startBtn.disabled = true;
  stopBtn.disabled = false;
  statusText.textContent = '启动中...';
  logArea.textContent = '提交参数中，请等待服务器返回...';
  links.innerHTML = '';

  try {
    const res = await fetch('/api/start', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
    const data = await res.json();
    if (!data.ok) {
      statusText.textContent = `启动失败: ${data.error || '未知原因'}`;
      startBtn.disabled = false;
      stopBtn.disabled = false;
      return;
    }
    statusText.textContent = '任务已启动';
  } catch (err) {
    statusText.textContent = '请求失败，请检查本地服务后重试';
    startBtn.disabled = false;
    stopBtn.disabled = false;
  }
}

async function stopJob() {
  statusText.textContent = '停止请求中...';
  try {
    const res = await fetch('/api/stop', { method: 'POST' });
    const data = await res.json();
    if (!data.ok) {
      statusText.textContent = data.error || '当前无运行任务';
      return;
    }
    statusText.textContent = '已提交停止';
  } catch (err) {
    statusText.textContent = `停止失败: ${String(err)}`;
  }
}

function applyStatus(data) {
  if (!data.job) {
    statusText.textContent = '闲置';
    startBtn.disabled = false;
    stopBtn.disabled = false;
    return;
  }

  const job = data.job;
  const running = data.running;
  const total = job.total ? ` / ${job.total}` : '';
  statusText.textContent = `状态: ${job.status} 进度: ${job.progress}${total}`;
  if (job.error) {
    statusText.textContent += ` | 错误: ${job.error}`;
  }

  logArea.textContent = (job.log_lines || []).slice(-240).join('\n');
  startBtn.disabled = running;
  links.innerHTML = '';
  if (job.output_csv) {
    links.innerHTML = `
      <a href="/api/download/csv" target="_blank">下载 CSV</a>
      <a href="/api/download/xlsx" target="_blank">下载 Excel</a>
    `;
  }

  if (!running && job.status !== 'idle' && job.status !== 'running') {
    stopBtn.disabled = false;
  }
}

async function refresh() {
  try {
    const res = await fetch('/api/status');
    const data = await res.json();
    applyStatus(data);
  } catch (err) {
    statusText.textContent = '状态刷新失败，请稍后重试';
  }
}

function updateJitterValue() {
  if (!jitterRange || !jitterValue) {
    return;
  }
  const value = Math.max(0, toNumber(jitterRange.value, DEFAULTS.jitter));
  jitterValue.textContent = `${value.toFixed(2)} 秒`;
}

form.addEventListener('submit', startJob);
stopBtn.addEventListener('click', stopJob);
if (jitterRange) {
  jitterRange.addEventListener('input', updateJitterValue);
}
renderRegionCards();
updateJitterValue();
setInterval(refresh, 1500);
refresh();
