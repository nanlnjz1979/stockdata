// 简易事件工具
function qs(sel) { return document.querySelector(sel); }
function qsa(sel) { return Array.from(document.querySelectorAll(sel)); }
function toast(msg, timeout = 2200) {
  const el = qs('#toast');
  el.textContent = msg;
  el.style.display = 'block';
  setTimeout(() => { el.style.display = 'none'; }, timeout);
}
const API_BASE = 'http://127.0.0.1:8000';

// 主题切换（暗色/亮色）
(function initThemeToggle(){
  const btn = qs('#themeToggle');
  if (!btn) return;
  let dark = false;
  const apply = () => {
    document.documentElement.setAttribute('data-theme', dark ? 'dark' : 'light');
    btn.innerHTML = dark ? '<i class="ri-sun-line"></i> 亮色' : '<i class="ri-moon-line"></i> 夜间';
  };
  btn.addEventListener('click', () => { dark = !dark; apply(); });
  apply();
})();

// 登录（无需验证）
(function initLogin(){
  const login = qs('#login');
  const btn = qs('#loginBtn');
  const nameInput = qs('#nickname');
  btn.addEventListener('click', () => {
    const name = (nameInput.value || '').trim();
    if (!name) return toast('请输入昵称');
    login.style.display = 'none';
    qs('#userStatus').textContent = `欢迎，${name}`;
    toast('登录成功');
  });
})();

// 标签导航与标题更新
(function initTabs(){
  const tabs = qsa('.sidebar .tab');
  function showTab(target){
    qsa('.section').forEach(s => s.classList.toggle('active', s.id === target));
    tabs.forEach(t => t.classList.toggle('active', t.dataset.target === target));
    const titleMap = { quotes: '行情', query: '查询', analysis: '分析', profile: '个人中心', status: '数据状态', update: '数据更新', tasks: '计划任务', config: '参数配置' };
    const title = titleMap[target] || '模块';
    qs('#pageTitle').textContent = title;
  }
  tabs.forEach(t => t.addEventListener('click', () => showTab(t.dataset.target)));
  showTab('quotes');
})();

// 行情图初始化（ECharts，占位数据）
(function initChart(){
  const el = qs('#chart');
  if (!el) return;
  const chart = echarts.init(el);
  const baseData = Array.from({length: 60}, (_, i) => [i, 100 + Math.sin(i/4)*8 + (i%7)]);
  chart.setOption({
    grid: { left: 36, right: 16, top: 24, bottom: 28 },
    tooltip: { trigger: 'axis' },
    xAxis: { type: 'category' },
    yAxis: { type: 'value' },
    series: [{ type: 'line', data: baseData.map(d => d[1]), smooth: true, lineStyle: { width: 2 }, areaStyle: { opacity: 0.08 } }],
    color: ['#1677ff']
  });
  const subBtn = qs('#subBtn');
  const subCode = qs('#subCode');
  const subsList = qs('#subsList');
  if (subBtn) {
    subBtn.addEventListener('click', () => {
      const code = (subCode.value || '').trim();
      if (!code) return toast('请输入订阅代码');
      const li = document.createElement('li');
      li.textContent = code;
      subsList.appendChild(li);
      toast(`已订阅 ${code}`);
      baseData.push([baseData.length, baseData[baseData.length-1][1] + (Math.random()-0.5)*2]);
      baseData.shift();
      chart.setOption({ series: [{ data: baseData.map(d => d[1]) }] });
    });
  }
})();

// 查询接口占位
(function initQuery(){
  const btn = qs('#queryBtn');
  const codeEl = qs('#queryCode');
  const dateEl = qs('#queryDate');
  const tbody = qs('#queryTable');
  if (!btn) return;
  btn.addEventListener('click', async () => {
    const code = (codeEl.value || 'TEST').trim();
    const url = `${API_BASE}/api/stocks/quotes?code=${encodeURIComponent(code)}`;
    try {
      const res = await fetch(url);
      const data = await res.json();
      tbody.innerHTML = '';
      const rows = Array.isArray(data) ? data : (data.items || []);
      const src = Array.isArray(data.quotes) ? data.quotes : rows;
      src.slice(0, 20).forEach(r => {
        const tr = document.createElement('tr');
        tr.innerHTML = `<td>${r.timestamp||r.date||'-'}</td><td>${r.open_price||r.open||'-'}</td><td>${r.close_price||r.close||'-'}</td><td>${r.high_price||r.high||'-'}</td><td>${r.low_price||r.low||'-'}</td><td>${r.volume||'-'}</td>`;
        tbody.appendChild(tr);
      });
      toast(`查询完成：${(src||[]).length} 条`);
    } catch (e) { toast('查询失败（后端未迁移或接口错误）'); }
  });
})();

// MA(5) 示例
(function initMA(){
  const btn = qs('#ma5Btn');
  const input = qs('#ma5Input');
  const out = qs('#ma5Output');
  if (!btn) return;
  btn.addEventListener('click', () => {
    const arr = (input.value || '1,2,3,4,5,6,7,8,9').split(',').map(v => parseFloat(v.trim())).filter(v => !isNaN(v));
    const ma5 = arr.map((_, i) => {
      if (i < 4) return null;
      const s = arr.slice(i-4, i+1).reduce((a,b)=>a+b,0);
      return +(s/5).toFixed(2);
    });
    out.textContent = JSON.stringify(ma5, null, 2);
  });
})();

// 关注列表（本地存储回退）
(function initFav(){
  const key = 'favList';
  const list = qs('#favList');
  const addBtn = qs('#favAdd');
  const codeEl = qs('#favCode');
  if (!addBtn) return;
  const save = () => localStorage.setItem(key, JSON.stringify(Array.from(list.querySelectorAll('li')).map(li => li.textContent)));
  const load = () => {
    list.innerHTML = '';
    try {
      (JSON.parse(localStorage.getItem(key)||'[]')||[]).forEach(c => { const li = document.createElement('li'); li.textContent = c; list.appendChild(li); });
    } catch {}
  };
  addBtn.addEventListener('click', () => { const c = (codeEl.value||'').trim(); if(!c) return; const li=document.createElement('li'); li.textContent=c; list.appendChild(li); save(); });
  load();
})();

// 预警占位
(function initAlert(){
  const list = qs('#alertList');
  const saveBtn = qs('#alertSave');
  if (!saveBtn) return;
  saveBtn.addEventListener('click', () => {
    const code = (qs('#alertCode').value||'').trim();
    const price = (qs('#alertPrice').value||'').trim();
    if (!code || !price) return toast('请填写代码与价格');
    const li = document.createElement('li');
    li.textContent = `${code} 触发价 ${price}`;
    list.appendChild(li);
    toast('已保存预警（占位）');
  });
})();

// 数据状态模块（筛选+趋势+备份健康）
(function initStatus(){
  const overview = qs('#statusOverview');
  const marketsBody = qs('#statusMarkets');
  const listing = qs('#statusListing');
  const refreshBtn = qs('#statusRefresh');
  const marketSel = qs('#statusMarket');
  const startEl = qs('#financeStart');
  const endEl = qs('#financeEnd');
  const applyBtn = qs('#statusApply');
  const backupEl = qs('#statusBackup');
  const trendEl = qs('#statusTrend');
  let trendChart = null;

  function buildUrl(){
    const p = new URLSearchParams();
    const m = marketSel && marketSel.value || '';
    const s = startEl && startEl.value || '';
    const e = endEl && endEl.value || '';
    if (m) p.set('market', m);
    if (s) p.set('finance_start', s);
    if (e) p.set('finance_end', e);
    const qsStr = p.toString();
    return `${API_BASE}/api/stocks/status${qsStr ? ('?' + qsStr) : ''}`;
  }

  function renderTrend(trends){
    if (!trendEl) return;
    trendChart = trendChart || echarts.init(trendEl);
    const labels = (trends||[]).map(t => t.month);
    const counts = (trends||[]).map(t => t.count);
    trendChart.setOption({
      grid: { left: 36, right: 16, top: 24, bottom: 28 },
      tooltip: { trigger: 'axis' },
      xAxis: { type: 'category', data: labels },
      yAxis: { type: 'value' },
      series: [{ type: 'bar', data: counts, barMaxWidth: 24 }],
      color: ['#53c1de']
    });
  }

  async function reload(){
    // 状态模块不使用任务列表的加载遮罩
    try {
      const res = await fetch(buildUrl());
      const data = await res.json();
      // 总览
      const ov = data.overview || {};
      overview.innerHTML = `基础股票：<b>${ov.stock_basic_count||0}</b>，财务记录：<b>${ov.finance_count||0}</b>，关注记录：<b>${ov.follow_count||0}</b>，最新财报日期：<b>${ov.latest_finance_date||'-'}</b>`;
      // 备份健康
      const bk = data.backup || {};
      const sizeMB = bk.size_bytes ? (bk.size_bytes/1e6).toFixed(2) : '0.00';
      backupEl.innerHTML = `数据库：<code>${bk.db_path||'-'}</code>；大小：<b>${sizeMB} MB</b>；最后更新：<b>${bk.last_modified||'-'}</b>；健康度：<b>${bk.health_score||0}</b>/100`;
      // 市场分布
      marketsBody.innerHTML = '';
      (data.markets||[]).forEach(m => { const tr = document.createElement('tr'); tr.innerHTML = `<td>${m.market||'-'}</td><td>${m.count||0}</td>`; marketsBody.appendChild(tr); });
      // 上市日期范围
      const rng = data.listing_range || {};
      listing.textContent = `最早：${rng.min||'-'}，最晚：${rng.max||'-'}`;
      // 趋势图
      renderTrend(data.trends || []);
      // 选项填充（仅首次或空时）
      if (marketSel && marketSel.options.length <= 1) {
        (data.options?.markets||[]).forEach(v => { const opt = document.createElement('option'); opt.value = v; opt.textContent = v; marketSel.appendChild(opt); });
      }
    } catch(e) {
      overview.textContent = '加载失败（请确认后端运行且数据已迁移）';
      listing.textContent = '-';
    }
  }

  if (refreshBtn) refreshBtn.addEventListener('click', reload);
  if (applyBtn) applyBtn.addEventListener('click', reload);
  const statusTab = document.querySelector('.sidebar .tab[data-target="status"]');
  if (statusTab) statusTab.addEventListener('click', reload);
  reload();
})();

// 导出（图表PNG + 表格CSV）
(function initExport(){
  const btn = qs('#exportBtn');
  if (!btn) return;
  btn.addEventListener('click', () => {
    try {
      const chartEl = qs('#chart');
      const chart = echarts.getInstanceByDom(chartEl);
      const url = chart.getDataURL({ type: 'png', pixelRatio: 2, backgroundColor: '#fff' });
      const a = document.createElement('a'); a.href = url; a.download = 'chart.png'; a.click();
    } catch {}
    const rows = [['日期','开盘','收盘','最高','最低','成交量']].concat(Array.from(qs('#queryTable').querySelectorAll('tr')).map(tr => Array.from(tr.children).map(td => td.textContent)));
    const csv = rows.map(r => r.map(v => '"'+String(v).replace(/"/g,'\"')+'"').join(',')).join('\n');
    const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' });
    const a2 = document.createElement('a'); a2.href = URL.createObjectURL(blob); a2.download = 'query.csv'; a2.click();
    toast('已导出图表与表格');
  });
})();

// 数据更新板块
(function initUpdate(){
  const qdbEl = document.getElementById('updateQdb');
  const paramsEl = document.getElementById('updateQdbParams');
  const basicEl = document.getElementById('updateBasicCount');
  const financeEl = document.getElementById('updateFinanceCount');
  const latestFinanceEl = document.getElementById('updateLatestFinance');
  const latestFollowEl = document.getElementById('updateLatestFollow');
  const runBtn = document.getElementById('updateRun');
  const runStatusEl = document.getElementById('updateRunStatus');
  const fullBtn = document.getElementById('updateFull');
  const fullStatusEl = document.getElementById('updateFullStatus');
  const pauseBtn = document.getElementById('updatePause');
  const stopBtn = document.getElementById('updateStop');

  const queueStartBtn = document.getElementById('queueUpdateStart');
  const queueToggleBtn = document.getElementById('queueUpdateToggle');
  const queueStatusEl = document.getElementById('queueUpdateStatus');
   let paused = false;
   let queuePaused = false;

  fetch(`${API_BASE}/api/stocks/update/status`)
    .then(r => r.json())
    .then(d => {
      const ctrl = d.controller || {};
      const qctrl = d.queue_controller || {};
      paused = !!ctrl.paused;
      queuePaused = !!qctrl.paused;
      qdbEl.textContent = d.questdb?.connected ? '已连接' : '未连接';
      paramsEl.textContent = `${d.questdb?.host || '-'}:${d.questdb?.port || '-'} ${d.questdb?.user || '-'}/${d.questdb?.dbname || '-'}`;
      basicEl.textContent = d.stock_basic_count || 0;
      financeEl.textContent = d.finance_count || 0;
      latestFinanceEl.textContent = d.latest_finance_date || '-';
      latestFollowEl.textContent = d.latest_follow_time || '-';
      // 初始化按钮文案
      if (pauseBtn) pauseBtn.textContent = paused ? '继续' : '暂停';
      if (queueToggleBtn) queueToggleBtn.textContent = queuePaused ? '继续' : '暂停';
    })
    .catch(err => {
      qdbEl.textContent = '异常';
      paramsEl.textContent = String(err);
    });

  runBtn?.addEventListener('click', () => {
    runStatusEl.textContent = '增量更新触发中...';
    fetch(`${API_BASE}/api/stocks/update/run`, { method: 'POST' })
      .then(r => r.json())
      .then(d => {
        runStatusEl.textContent = `增量更新 started=${d.started} at ${d.started_at || ''}`;
      })
      .catch(err => {
        runStatusEl.textContent = `失败：${err}`;
      });
  });

  fullBtn?.addEventListener('click', () => {
    fullStatusEl.textContent = '触发全量中...';
    fetch(`${API_BASE}/api/stocks/update/full`, { method: 'POST' })
      .then(async (r) => {
        let payload = {};
        try { payload = await r.json(); } catch {}
        if (!r.ok) {
          const msg = payload.error || payload.detail || `HTTP ${r.status}`;
          throw new Error(msg);
        }
        return payload;
      })
      .then(d => {
        fullStatusEl.textContent = `started=${d.started} at ${d.started_at || ''} total=${d.total_codes || ''}`;
        if (!d.started) {
          toast(d.error || '启动失败');
        }
      })
      .catch(err => {
        fullStatusEl.textContent = `失败：${err.message}`;
        toast(`全量更新失败：${err.message}`);
      });
  });

  pauseBtn?.addEventListener('click', async () => {
    try {
      const url = paused ? `${API_BASE}/api/stocks/update/resume` : `${API_BASE}/api/stocks/update/pause`;
      const res = await fetch(url, { method: 'POST' });
      const data = await res.json();
      paused = !!data.paused;
      pauseBtn.textContent = paused ? '继续' : '暂停';
      toast(paused ? '已暂停全量更新' : '已继续全量更新');
    } catch(e) {
      toast('操作失败');
    }
  });

  stopBtn?.addEventListener('click', async () => {
    try {
      const res = await fetch(`${API_BASE}/api/stocks/update/stop`, { method: 'POST' });
      const data = await res.json();
      toast('已请求停止，全量更新将尽快退出');
    } catch(e) {
      toast('停止失败');
    }
  });

  // 任务队列：启动 + 暂停/继续
  queueStartBtn?.addEventListener('click', async () => {
    try {
      queueStatusEl.textContent = '启动任务队列中...';
      const r = await fetch(`${API_BASE}/api/stocks/update/queue/start`, { method: 'POST' });
      let d = {};
      try { d = await r.json(); } catch {}
      if (!r.ok || !d.started) {
        const msg = d.error || d.detail || `HTTP ${r.status}`;
        queueStatusEl.textContent = `启动失败：${msg}`;
        toast(`任务队列启动失败：${msg}`);
        return;
      }
      queueStatusEl.textContent = `已启动，待处理 ${d.total_codes || 0} 个任务`;
      if (queueToggleBtn) {
        queueToggleBtn.disabled = false;
        queueToggleBtn.textContent = '暂停';
      }
      queuePaused = false;
    } catch(e) {
      queueStatusEl.textContent = `异常：${e.message || e}`;
    }
  });

  queueToggleBtn?.addEventListener('click', async () => {
    try {
      const url = queuePaused ? `${API_BASE}/api/stocks/update/queue/resume` : `${API_BASE}/api/stocks/update/queue/pause`;
      const r = await fetch(url, { method: 'POST' });
      let d = {};
      try { d = await r.json(); } catch {}
      queuePaused = !!d.paused;
      if (queueToggleBtn) queueToggleBtn.textContent = queuePaused ? '继续' : '暂停';
      queueStatusEl.textContent = queuePaused ? '已暂停队列' : '已继续队列';
      toast(queuePaused ? '已暂停任务队列' : '已继续任务队列');
    } catch(e) {
      toast('操作失败');
    }
  });

  const updateTab = document.querySelector('.sidebar .tab[data-target="update"]');

  async function loadStatus(){
    try {
      const res = await fetch(`${API_BASE}/api/stocks/update/status`);
      const data = await res.json();
      const qdb = data.questdb || {};
      const ctrl = data.controller || {};
      const qctrl = data.queue_controller || {};
      paused = !!ctrl.paused;
      queuePaused = !!qctrl.paused;
      if (pauseBtn) pauseBtn.textContent = paused ? '继续' : '暂停';
      if (queueToggleBtn) queueToggleBtn.textContent = queuePaused ? '继续' : '暂停';
      qdbEl.textContent = qdb.connected ? '已连接' : '未连接';
      paramsEl.textContent = `${qdb.host||'-'}:${qdb.port||'-'} ${qdb.user||'-'}/${qdb.dbname||'-'}`;
      basicEl.textContent = data.stock_basic_count ?? 0;
      financeEl.textContent = data.finance_count ?? 0;
      latestFinanceEl.textContent = data.latest_finance_date || '-';
      latestFollowEl.textContent = data.latest_follow_time || '-';
    } catch(e) {
      qdbEl.textContent = '加载失败';
      paramsEl.textContent = '-';
    }
  }

  async function pollProgress(){
    try {
      const res = await fetch(`${API_BASE}/api/stocks/update/status`);
      const data = await res.json();
      const total = (data.controller?.total_codes ?? data.total_codes) || 0;
      const updated = (data.controller?.updated_count ?? data.updated_count) || 0;
      const percent = total ? Math.round(updated / total * 100) : 0;
      const cur = data.controller?.current_code || '-';
      const runFlag = data.controller?.running ? '运行中' : (data.controller?.stopped ? '已停止' : '空闲');
      const recent = (data.recent_updates || []).map(r => r.code).slice(0,5).join(', ');
      // 原有全量状态
      fullStatusEl.textContent = `状态：${runFlag} | 进度：${updated}/${total} (${percent}%) 当前：${cur} 最近：${recent || '-'}`;
      // 新增任务队列状态（从 queue_controller 取状态）
      if (queueStatusEl) {
        const qtotal = (data.queue_controller?.total_codes ?? 0) || 0;
        const qupdated = (data.queue_controller?.updated_count ?? 0) || 0;
        const qpercent = qtotal ? Math.round(qupdated / qtotal * 100) : 0;
        const qcur = data.queue_controller?.current_code || '-';
        const qrunFlag = data.queue_controller?.running ? '运行中' : (data.queue_controller?.stopped ? '已停止' : '空闲');
        queueStatusEl.textContent = `状态：${qrunFlag} | 进度：${qupdated}/${qtotal} (${qpercent}%) 当前：${qcur}`;
      }
      // 切换按钮可用性与文案
      const running = !!(data.queue_controller?.running);
      queuePaused = !!(data.queue_controller?.paused);
      if (queueToggleBtn) {
        queueToggleBtn.disabled = !running;
        queueToggleBtn.textContent = queuePaused ? '继续' : '暂停';
      }
      if (queueStartBtn) {
        queueStartBtn.disabled = running; // 运行中不可再次启动
      }
    } catch(e) {}
  }

  if (runBtn) runBtn.addEventListener('click', () => {});
  if (updateTab) updateTab.addEventListener('click', loadStatus);
  loadStatus();
  setInterval(pollProgress, 3000);
})();

// 计划任务模块（列表 + 过滤）
(function initTasks(){
  const typeSel = qs('#taskTypeFilter');
  const statusSel = qs('#taskStatusFilter');
  const applyBtn = qs('#taskApply');
  const tbody = qs('#taskListTable');
  const tasksTab = document.querySelector('.sidebar .tab[data-target="tasks"]');
  if (!tbody) return;
  const prevBtn = qs('#taskPrev');
  const nextBtn = qs('#taskNext');
  const pageInfo = qs('#taskPageInfo');
  let page = 1;
  const pageSize = 50;
  // 固定状态列表，保证筛选项始终可用
  const STATUS_OPTIONS = ['待处理','处理中','成功','失败','重试中','已取消'];
  if (statusSel && statusSel.options.length <= 1) {
    STATUS_OPTIONS.forEach(v => { const opt = document.createElement('option'); opt.value = v; opt.textContent = v; statusSel.appendChild(opt); });
  }
  const paramSearch = qs('#taskParamSearch');
  function buildUrl(){
    const p = new URLSearchParams();
    const t = (typeSel && typeSel.value) || '';
    const s = (statusSel && statusSel.value) || '';
    const q = (paramSearch && paramSearch.value) || '';
    if (t) p.set('task_type', t);
    if (s) p.set('status', s);
    if (q) p.set('param_contains', q);
    p.set('page', String(page));
    p.set('page_size', String(pageSize));
    const qsStr = p.toString();
    return `${API_BASE}/api/stocks/tasks${qsStr ? ('?' + qsStr) : ''}`;
  }

  function render(items){
    tbody.innerHTML = '';
    const fmtTs = (v) => {
      if (!v) return '-';
      try {
        const d = new Date(v);
        if (!isNaN(d.getTime())) return d.toLocaleString();
        return String(v);
      } catch(e) { return String(v); }
    };
    (items || []).forEach(it => {
      const tr = document.createElement('tr');
      const desc = it.task_desc || '';
      const params = (it.task_params && typeof it.task_params === 'object') ? JSON.stringify(it.task_params) : (it.task_params || '');
      tr.innerHTML = `<td>${it.task_type||'-'}</td><td title="${desc}">${desc||'-'}</td><td title="${params}">${params||'-'}</td><td>${it.status||'-'}</td><td>${it.priority ?? 0}</td><td>${fmtTs(it.created_at)}</td><td>${fmtTs(it.started_at)}</td><td>${fmtTs(it.ended_at)}</td>`;
      tbody.appendChild(tr);
    });
  }

  async function reload(){
    try {
      const res = await fetch(buildUrl());
      const data = await res.json();
      render(data.items || []);
      if (typeSel && typeSel.options.length <= 1) {
        (data.options?.types || []).forEach(v => { const opt = document.createElement('option'); opt.value = v; opt.textContent = v; typeSel.appendChild(opt); });
      }
      const meta = {
        total: data.total ?? 0,
        page: data.page ?? page,
        page_size: data.page_size ?? pageSize,
        total_pages: data.total_pages ?? Math.max(1, Math.ceil(((data.total ?? 0) / (data.page_size ?? pageSize))))
      };
      page = meta.page;
      if (pageInfo) pageInfo.textContent = `第 ${meta.page} / ${meta.total_pages} 页`;
      if (prevBtn) prevBtn.disabled = !(data.has_prev ?? (meta.page > 1));
      if (nextBtn) nextBtn.disabled = !(data.has_next ?? (meta.page < meta.total_pages));
      // 状态下拉已固定填充，无需依赖后端；如需要可同步后端返回但不覆盖
    } catch(e) {
      tbody.innerHTML = '<tr><td colspan="8">加载失败</td></tr>';
    }
  }

  applyBtn?.addEventListener('click', () => { page = 1; reload(); });
  paramSearch?.addEventListener('keydown', (ev) => { if (ev.key === 'Enter') { page = 1; reload(); } });
  tasksTab?.addEventListener('click', () => { page = 1; reload(); });
  prevBtn?.addEventListener('click', () => { if (page > 1) { page--; reload(); }});
  nextBtn?.addEventListener('click', () => { page++; reload(); });
  reload();
})();

// 参数配置模块（列表编辑 + 保存）
(function initConfig(){
  const tbody = qs('#configTable');
  const reloadBtn = qs('#configReload');
  const saveBtn = qs('#configSave');
  const addBtn = qs('#configAdd');
  if (!tbody) return;

  function render(items){
    tbody.innerHTML = '';
    (items||[]).forEach(it => {
      const tr = document.createElement('tr');
      const id = (it.id||'').trim();
      const name = it.name||'';
      const desc = it.task_desc||'';
      const params = it.params||'';
      const stime = it.schedule_time||'';
      const enabled = (it.enabled===1 || it.enabled===true || String(it.enabled).toLowerCase() in { '1':1, 'true':1, 't':1, 'yes':1, 'y':1 });
      tr.innerHTML = `
        <td><input class="input" value="${id}" data-field="id" /></td>
        <td><input class="input" value="${name}" data-field="name" /></td>
        <td><input class="input" value="${desc}" data-field="task_desc" /></td>
        <td><input class="input" value='${params.replace(/'/g, "&#39;")}' data-field="params" /></td>
        <td><input class="input" value="${stime}" data-field="schedule_time" placeholder="16:30 或 1970-01-01 16:30:00" /></td>
        <td style="text-align:center;"><input type="checkbox" ${enabled? 'checked': ''} data-field="enabled" /></td>
        <td><button class="btn outline danger row-del">删除</button></td>
      `;
      tbody.appendChild(tr);
    });
  }

  function addRow(){
    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td><input class="input" value="" data-field="id" placeholder="唯一ID" /></td>
      <td><input class="input" value="" data-field="name" placeholder="名称" /></td>
      <td><input class="input" value="" data-field="task_desc" placeholder="描述" /></td>
      <td><input class="input" value='{"market":"CN","adjust":"hfq"}' data-field="params" /></td>
      <td><input class="input" value="16:30" data-field="schedule_time" placeholder="16:30 或 1970-01-01 16:30:00" /></td>
      <td style="text-align:center;"><input type="checkbox" checked data-field="enabled" /></td>
      <td><button class="btn outline danger row-del">删除</button></td>
    `;
    tbody.appendChild(tr);
  }

  // 事件委托：删除行
  tbody.addEventListener('click', (e) => {
    const t = e.target;
    if (t && t.classList && t.classList.contains('row-del')) {
      const tr = t.closest('tr');
      if (tr) {
        tr.remove();
        toast('已删除一行，点击保存同步到数据库');
      }
    }
  });

  // 时间选择器弹窗
  let tpEl = null, tpDate = null, tpTime = null, tpActiveInput = null;
  function ensureTimePicker(){
    if (tpEl) return;
    tpEl = document.createElement('div');
    tpEl.id = 'timePicker';
    tpEl.style.position = 'absolute';
    tpEl.style.background = '#fff';
    tpEl.style.border = '1px solid #ddd';
    tpEl.style.boxShadow = '0 4px 16px rgba(0,0,0,0.12)';
    tpEl.style.padding = '12px';
    tpEl.style.borderRadius = '8px';
    tpEl.style.zIndex = '9999';
    tpEl.style.minWidth = '280px';
    tpEl.style.display = 'none';
    tpEl.innerHTML = `
      <div style="font-weight:600;margin-bottom:8px;">选择时间</div>
      <div style="display:flex;gap:8px;align-items:center;margin-bottom:8px;">
        <label style="width:56px">日期</label>
        <input type="date" id="tpDate" class="input" style="flex:1;" />
      </div>
      <div style="display:flex;gap:8px;align-items:center;margin-bottom:12px;">
        <label style="width:56px">时间</label>
        <input type="time" id="tpTime" class="input" step="60" style="flex:1;" />
      </div>
      <div style="display:flex;gap:8px;justify-content:flex-end;">
        <button class="btn" data-action="now">现在</button>
        <button class="btn outline" data-action="clear">清空</button>
        <button class="btn primary" data-action="ok">确定</button>
        <button class="btn outline" data-action="cancel">取消</button>
      </div>
    `;
    document.body.appendChild(tpEl);
    tpDate = tpEl.querySelector('#tpDate');
    tpTime = tpEl.querySelector('#tpTime');
    tpEl.addEventListener('click', (ev) => {
      const btn = ev.target.closest('button[data-action]');
      if (!btn) return;
      const act = btn.dataset.action;
      if (act === 'now') {
        const d = new Date();
        tpDate.value = d.toISOString().slice(0,10);
        const hh = String(d.getHours()).padStart(2,'0');
        const mm = String(d.getMinutes()).padStart(2,'0');
        tpTime.value = `${hh}:${mm}`;
      } else if (act === 'clear') {
        tpDate.value = '';
        tpTime.value = '';
      } else if (act === 'ok') {
        if (tpActiveInput) {
          const dv = tpDate.value.trim();
          let tv = (tpTime.value||'').trim();
          if (tv && tv.length === 5) tv = tv + ':00';
          if (dv && tv) tpActiveInput.value = `${dv} ${tv}`;
          else if (!dv && tv) tpActiveInput.value = tv.slice(0,5);
          else tpActiveInput.value = '';
        }
        hideTimePicker();
      } else if (act === 'cancel') {
        hideTimePicker();
      }
    });
    document.addEventListener('mousedown', (ev) => {
      if (!tpEl || tpEl.style.display === 'none') return;
      const withinPicker = tpEl.contains(ev.target);
      const onInput = tpActiveInput && (ev.target === tpActiveInput);
      if (!withinPicker && !onInput) hideTimePicker();
    });
  }
  function showTimePicker(input){
    ensureTimePicker();
    tpActiveInput = input;
    const v = (input.value||'').trim();
    tpDate.value = '';
    tpTime.value = '';
    // 解析现有值
    const m1 = v.match(/^(\d{2}:\d{2})(?::\d{2})?$/);
    const m2 = v.match(/^(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2})(?::\d{2})?$/);
    if (m2) {
      tpDate.value = m2[1];
      tpTime.value = m2[2];
    } else if (m1) {
      tpTime.value = m1[1];
    }
    // 定位到输入框下方
    const rect = input.getBoundingClientRect();
    tpEl.style.left = (window.scrollX + rect.left) + 'px';
    tpEl.style.top = (window.scrollY + rect.bottom + 4) + 'px';
    tpEl.style.display = 'block';
  }
  function hideTimePicker(){
    if (tpEl) tpEl.style.display = 'none';
    tpActiveInput = null;
  }

  // 点击时间输入时弹窗
  tbody.addEventListener('click', (e) => {
    const input = e.target.closest('input[data-field="schedule_time"]');
    if (input) {
      showTimePicker(input);
    }
  });

  async function reload(){
    try {
      const res = await fetch(`${API_BASE}/api/configs/schedule`);
      const data = await res.json();
      render(data.items||[]);
      toast('已加载参数配置');
    } catch(e) {
      toast('加载配置失败');
    }
  }

  function collect(){
    const rows = Array.from(tbody.querySelectorAll('tr'));
    return rows.map(tr => {
      const get = f => tr.querySelector(`[data-field="${f}"]`);
      const id = (get('id').value||'').trim();
      const name = (get('name').value||'').trim();
      const task_desc = (get('task_desc').value||'').trim();
      const params = (get('params').value||'').trim();
      const schedule_time = (get('schedule_time').value||'').trim();
      const enabled = !!get('enabled').checked;
      return { id, name, task_desc, params, schedule_time, enabled };
    }).filter(it => it.id);
  }

  async function save(){
    const items = collect();
    try {
      const res = await fetch(`${API_BASE}/api/configs/schedule`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ items })
      });
      const data = await res.json();
      if (data.saved) toast(`已保存 ${data.count||items.length} 条配置`); else throw new Error(data.error||'未知错误');
    } catch(e) { toast('保存失败：' + (e.message||'')); }
  }

  if (reloadBtn) reloadBtn.addEventListener('click', reload);
  if (saveBtn) saveBtn.addEventListener('click', save);
  if (addBtn) addBtn.addEventListener('click', addRow);
  const cfgTab = document.querySelector('.sidebar .tab[data-target="config"]');
  if (cfgTab) cfgTab.addEventListener('click', reload);
})();
