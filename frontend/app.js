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
  // 直接隐藏登录窗口，不再要求用户输入
  login.style.display = 'none';
  // 设置默认用户名称
  qs('#userStatus').textContent = `欢迎，管理员`;
  toast('已自动登录');
})();

// 标签导航与标题更新
(function initTabs(){
  const tabs = qsa('.sidebar .tab');
  function showTab(target){
    qsa('.section').forEach(s => s.classList.toggle('active', s.id === target));
    tabs.forEach(t => t.classList.toggle('active', t.dataset.target === target));
    const titleMap = { query: '查询', analysis: '分析', profile: '个人中心', status: '数据状态', update: '数据更新', 'task-monitor': '任务状态', tasks: '计划任务', config: '参数配置' };
    const title = titleMap[target] || '模块';
    qs('#pageTitle').textContent = title;
  }
  tabs.forEach(t => t.addEventListener('click', () => showTab(t.dataset.target)));
  showTab('query');
})();

// 行情图初始化（ECharts，占位数据）
// 实时行情和图表功能已移除

// 任务监控模块
(function initTaskMonitor() {
  const monitorSection = qs('#task-monitor');
  if (!monitorSection) return;

  const refreshBtn = qs('#taskMonitorRefresh');
  const statsSection = qs('#taskMonitorStats');
  const recentTasksTable = qs('#recentTasksTable');
  const schedulesTable = qs('#schedulesTable');
  
  // 加载所有数据
  function loadAllData() {
    loadStatistics();
    loadRecentTasks();
    loadSchedules();
  }
  
  // 添加刷新按钮事件监听
  if (refreshBtn) {
    refreshBtn.addEventListener('click', () => {
      refreshBtn.disabled = true;
      refreshBtn.textContent = '刷新中...';
      
      loadAllData().finally(() => {
        setTimeout(() => {
          refreshBtn.disabled = false;
          refreshBtn.textContent = '刷新';
        }, 500);
      });
    });
  }
  
  // 当任务监控页面激活时自动加载数据
  const tabs = qsa('.sidebar .tab');
  tabs.forEach(tab => {
    tab.addEventListener('click', () => {
      if (tab.dataset.target === 'task-monitor') {
        loadAllData();
      }
    });
  });
  
  // 初始加载（如果当前就是任务监控页面）
  if (monitorSection.classList.contains('active')) {
    loadAllData();
  }

  // 格式化时间
  function formatDateTime(dateTime) {
    if (!dateTime) return '-';
    const date = new Date(dateTime);
    return date.toLocaleString('zh-CN', {
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit'
    });
  }

  // 获取状态样式类名
  function getStatusClass(status) {
    const statusMap = {
      '成功': 'status-success',
      '失败': 'status-failed',
      '运行中': 'status-started',
      '排队中': 'status-queued'
    };
    return statusMap[status] || '';
  }

  // 加载任务统计
  async function loadStatistics() {
    try {
      const response = await fetch(`${API_BASE}/api/tasks/monitor`);
      const data = await response.json();
      
      if (data.success && data.statistics) {
        const stats = data.statistics;
        qs('#statTotal').textContent = stats.total || 0;
        qs('#statQueued').textContent = stats.queued || 0;
        qs('#statStarted').textContent = stats.started || 0;
        qs('#statSuccess').textContent = stats.success || 0;
        qs('#statFailed').textContent = stats.failed || 0;
        qs('#statSchedules').textContent = stats.schedules || 0;
      }
    } catch (error) {
      console.error('加载任务统计失败:', error);
    }
  }

  // 加载最近任务
  async function loadRecentTasks() {
    try {
      const response = await fetch(`${API_BASE}/api/tasks/recent?limit=20`);
      const data = await response.json();
      
      if (data.success && data.tasks) {
        recentTasksTable.innerHTML = '';
        
        data.tasks.forEach(task => {
          const tr = document.createElement('tr');
          const statusClass = getStatusClass(task.status);
          
          tr.innerHTML = `
            <td>${task.name || '未命名任务'}</td>
            <td>${task.func || '-'}</td>
            <td class="${statusClass}">${task.status}</td>
            <td>${formatDateTime(task.started)}</td>
            <td>${formatDateTime(task.stopped)}</td>
            <td>${task.result || '-'}</td>
          `;
          
          recentTasksTable.appendChild(tr);
        });
      }
    } catch (error) {
      console.error('加载最近任务失败:', error);
      recentTasksTable.innerHTML = '<tr><td colspan="5" style="text-align: center; color: #dc3545;">加载失败</td></tr>';
    }
  }

  // 加载调度任务
  async function loadSchedules() {
    try {
      const response = await fetch(`${API_BASE}/api/tasks/schedules`);
      const data = await response.json();
      
      if (data.success && data.schedules) {
        schedulesTable.innerHTML = '';
        
        data.schedules.forEach(schedule => {
          const tr = document.createElement('tr');
          // 根据repeats_status设置状态样式
          let statusClass = '';
          if (schedule.repeats_status === '已禁用') {
            statusClass = 'status-disabled';
          } else if (schedule.repeats_status === '无限重复') {
            statusClass = 'status-infinite';
          } else {
            statusClass = 'status-limited';
          }
          
          // 根据是否有错误日志决定名称颜色
          const nameClass = schedule.err_log ? 'status-failed' : 'status-success';
          
          tr.innerHTML = `
            <td class="${nameClass}">${schedule.name}</td>
            <td>${schedule.schedule_type}</td>
            <td>${schedule.func || '-'}</td>
            <td>${formatDateTime(schedule.next_run)}</td>
            <td class="${statusClass}">${schedule.repeats_status || '未知'}</td>
          `;
          
          schedulesTable.appendChild(tr);
        });
      }
    } catch (error) {
      console.error('加载调度任务失败:', error);
      schedulesTable.innerHTML = '<tr><td colspan="5" style="text-align: center; color: #dc3545;">加载失败</td></tr>';
    }
  }

  // 刷新所有数据
  async function refreshAll() {
    refreshBtn.disabled = true;
    refreshBtn.textContent = '刷新中...';
    
    try {
      await Promise.all([
        loadStatistics(),
        loadRecentTasks(),
        loadSchedules()
      ]);
    } finally {
      refreshBtn.disabled = false;
      refreshBtn.textContent = '刷新';
    }
  }

  // 绑定刷新按钮事件
  refreshBtn.addEventListener('click', refreshAll);

  // 初始加载数据
  refreshAll();

  // 自动刷新（每30秒）
  setInterval(() => {
    refreshAll();
  }, 30000);

})();

// 历史行情查询
// 查询界面选项卡切换功能
(function initQueryTabs() {
  const tabBtns = document.querySelectorAll('.tab-btn');
  
  tabBtns.forEach(btn => {
    btn.addEventListener('click', function() {
      const tab = this.getAttribute('data-tab');
      
      // 移除所有激活状态
      document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
      document.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));
      
      // 添加当前激活状态
      this.classList.add('active');
      document.getElementById(tab).classList.add('active');
    });
  });
})();

// 历史行情查询
(function initQuery() {
  console.log('初始化历史行情查询功能');
  const btn = qs('#queryBtn');
  const codeEl = qs('#queryCode');
  const startDateEl = qs('#startDate');
  const endDateEl = qs('#endDate');
  const tbody = qs('#queryTable');
  const adjustTypeEls = document.getElementsByName('adjustType');
  
  // 分页相关元素
  const prevPageBtn = qs('#prevPage');
  const nextPageBtn = qs('#nextPage');
  const currentPageEl = qs('#currentPage');
  const totalPagesEl = qs('#totalPages');
  const pageSizeEl = qs('#pageSize');
  
  // 全局变量用于分页
  let allData = []; // 存储完整查询结果
  let currentPage = 1;
  let pageSize = 20;
  
  console.log('DOM元素状态:', { btn, codeEl, startDateEl, endDateEl, tbody, adjustTypeEls });
  
  if (!btn || !codeEl || !startDateEl || !endDateEl || !tbody || adjustTypeEls.length === 0) {
    console.warn('查询相关DOM元素未找到');
    return;
  }
  
  // 获取选中的复权类型
  function getAdjustType() {
    for (const el of adjustTypeEls) {
      if (el.checked) {
        // 根据用户要求处理复权类型
        if (el.value === 'qfq') return 'qfq'; // 前复权
        if (el.value === 'hfq') return 'hfq'; // 后复权
        return ''; // 不复权，返回空字符串
      }
    }
    return 'qfq'; // 默认前复权
  }
  
  // 设置默认日期范围（最近7天）
  function setDefaultDates() {
    const endDate = new Date();
    const startDate = new Date();
    startDate.setDate(startDate.getDate() - 7);
    
    startDateEl.valueAsDate = startDate;
    endDateEl.valueAsDate = endDate;
  }
  
  // 初始化时设置默认日期
  setDefaultDates();
  
  // 移除可能存在的旧事件监听器，防止重复绑定
  const newBtn = btn.cloneNode(true);
  btn.parentNode.replaceChild(newBtn, btn);
  
  // 渲染表格数据（支持分页）
  function renderTableData(page = 1) {
    if (allData.length === 0) {
      tbody.innerHTML = '<tr><td colspan="9" style="text-align:center;">暂无数据</td></tr>';
      return;
    }
    
    // 计算分页参数
    const totalPages = Math.ceil(allData.length / pageSize);
    const currentPageNum = Math.max(1, Math.min(page, totalPages));
    const startIndex = (currentPageNum - 1) * pageSize;
    const endIndex = Math.min(startIndex + pageSize, allData.length);
    const currentData = allData.slice(startIndex, endIndex);
    
    // 更新页码信息
    currentPage = currentPageNum;
    currentPageEl.textContent = currentPageNum;
    totalPagesEl.textContent = totalPages;
    
    // 更新按钮状态
    prevPageBtn.disabled = currentPageNum <= 1;
    nextPageBtn.disabled = currentPageNum >= totalPages;
    
    // 渲染当前页数据
    tbody.innerHTML = '';
    currentData.forEach(function(row) {
      // 根据后端返回的数组格式解析数据：[trade_date, open, close, high, low, volume, amount, outstanding_share, turnover]
      const date = row[0] || '-';
      const open = row[1] || '-';
      const close = row[4] || '-'; // 注意索引位置，根据后端代码调整
      const high = row[2] || '-';
      const low = row[3] || '-';
      const volume = row[5] || '-';
      // 新增字段
      const amount = row[6] || '-';
      const outstandingShare = row[8] || '-';
      const turnover = row[7] || '-';
      
      const tr = document.createElement('tr');
      tr.innerHTML = `
        <td>${date}</td>
        <td>${open}</td>
        <td>${close}</td>
        <td>${high}</td>
        <td>${low}</td>
        <td>${volume}</td>
        <td>${amount}</td>
        <td>${outstandingShare}</td>
        <td>${turnover}</td>
      `;
      tbody.appendChild(tr);
    });
  }
  
  // 绑定分页事件
  function bindPaginationEvents() {
    // 上一页
    prevPageBtn.addEventListener('click', function() {
      if (currentPage > 1) {
        renderTableData(currentPage - 1);
      }
    });
    
    // 下一页
    nextPageBtn.addEventListener('click', function() {
      const totalPages = Math.ceil(allData.length / pageSize);
      if (currentPage < totalPages) {
        renderTableData(currentPage + 1);
      }
    });
    
    // 每页条数变化
    pageSizeEl.addEventListener('change', function() {
      pageSize = parseInt(this.value);
      renderTableData(1); // 重置到第一页
    });
  }
  
  // 初始化分页功能
  bindPaginationEvents();
  
  newBtn.addEventListener('click', async function() {
    console.log('查询按钮被点击');
    const code = (codeEl.value || '').trim();
    console.log('查询股票代码:', code);
    if (!code) {
      toast('请输入股票代码');
      return;
    }
    
    // 获取日期并格式化
    const startDate = startDateEl.value ? startDateEl.value.replace(/-/g, '') : '';
    const endDate = endDateEl.value ? endDateEl.value.replace(/-/g, '') : '';
    
    // 获取复权类型
    const adjustType = getAdjustType();
    console.log('复权类型:', adjustType);
    
    // 构建API请求URL
    console.log('使用API_BASE:', API_BASE);
    let url = `${API_BASE}/api/stocks/data/daily?code=${encodeURIComponent(code)}&adjust=${adjustType}`;
    if (startDate) url += `&start_date=${startDate}`;
    if (endDate) url += `&end_date=${endDate}`;
    console.log('最终请求URL:', url);
    
    // 设置加载状态
    newBtn.disabled = true;
    newBtn.textContent = '查询中...';
    tbody.innerHTML = '<tr><td colspan="9" style="text-align:center;">查询中...</td></tr>';
    
    try {
      // 发送请求
      const res = await fetch(url);
      
      if (!res.ok) {
        throw new Error(`HTTP错误，状态码: ${res.status}`);
      }
      
      const data = await res.json();
      
      // 检查响应数据格式
      if (Array.isArray(data)) {
        // 保存完整数据用于分页
        allData = data;
        
        // 重置到第一页并渲染
        renderTableData(1);
        
        toast(`查询完成：${data.length} 条记录`);
      } else {
        // 如果不是预期的数组格式，尝试转换为JSON字符串以便调试
        console.log('返回的数据格式:', data);
        tbody.innerHTML = '<tr><td colspan="9" style="text-align:center;">数据格式错误</td></tr>';
        toast('后端返回的数据格式不正确');
        allData = [];
        renderTableData(1); // 重置分页状态
      }
    } catch (e) {
      console.error('查询错误:', e);
      tbody.innerHTML = '<tr><td colspan="9" style="text-align:center;">查询失败</td></tr>';
      toast('查询失败：' + (e.message || '未知错误'));
      allData = [];
      renderTableData(1); // 重置分页状态
    } finally {
      // 恢复按钮状态
      newBtn.disabled = false;
      newBtn.textContent = '查询';
    }
  });
})();

// 龙虎榜机构追踪
(function initLHBQuery() {
  console.log('初始化龙虎榜机构追踪功能');
  const btn = qs('#lhbBtn');
  const dateEl = qs('#lhbDate');
  const queryTypeEl = qs('#lhbQueryType');
  const tbody = qs('#lhbTable');
  const loadingEl = qs('#lhbLoading');
  const noDataEl = qs('#lhbNoData');
  
  // 排序相关变量
  let currentData = []; // 存储原始数据
  let sortColumn = null; // 当前排序列索引
  let sortDirection = 0; // 0: 不排序, 1: 升序, -1: 降序
  
  // 可排序的列索引（直接对应数组索引位置）
  const sortableColumns = {
    2: 2, // 买入金额（数组索引2）
    3: 3, // 买入次数（数组索引3）
    4: 4, // 卖出金额（数组索引4）
    5: 5, // 卖出次数（数组索引5）
    6: 6   // 净额（数组索引6）
  };
  
  console.log('龙虎榜DOM元素状态:', { btn, dateEl, queryTypeEl, tbody, loadingEl, noDataEl });
  
  if (!btn || !dateEl || !queryTypeEl || !tbody || !loadingEl || !noDataEl) {
    console.warn('龙虎榜相关DOM元素未找到');
    return;
  }
  
  // 设置默认日期为当天
  const today = new Date();
  const formattedToday = today.toISOString().split('T')[0];
  dateEl.value = formattedToday;
  
  // 更新表头样式，添加排序图标
    function updateTableHeader() {
      const thead = tbody.parentNode.querySelector('thead tr');
      if (thead) {
        // 重置所有表头样式
        Array.from(thead.querySelectorAll('th')).forEach((th, index) => {
          // 清除之前的排序图标和样式
          th.innerHTML = th.textContent.replace(/\s*↑|↓$/g, '').trim();
          th.style.cursor = sortableColumns[index] ? 'pointer' : 'default';
          // 重置颜色为默认值
          th.style.color = '';
          
          // 添加排序图标和样式
          if (sortableColumns[index] && sortColumn === index) {
            if (sortDirection === 1) {
              th.innerHTML += ' ↑';
              th.style.color = '#f56c6c';
            } else if (sortDirection === -1) {
              th.innerHTML += ' ↓';
              th.style.color = '#67c23a';
            }
            // 不排序状态不设置颜色
          }
        });
      }
    }
  
  // 排序函数
  function sortData() {
    if (!sortColumn || sortDirection === 0) {
      return currentData.slice();
    }
    
    const index = sortableColumns[sortColumn];
    if (index === undefined) return currentData.slice();
    
    return currentData.slice().sort((a, b) => {
      const valA = parseFloat(a[index] || 0);
      const valB = parseFloat(b[index] || 0);
      
      if (sortDirection === 1) {
        return valA - valB;
      } else {
        return valB - valA;
      }
    });
  }
  
  // 渲染表格数据
  function renderTable(data) {
    tbody.innerHTML = '';
    
    // 限制显示最多200条记录
    data.slice(0, 200).forEach(function(row) {
      // 解析数据，从数组索引获取数据
      // 数组索引对应关系：0-code, 1-name, 2-buy_amount, 3-buy_times, 4-sell_amount, 5-sell_times, 6-net_amount, 7-query_type
      const stockCode = row[0] || '-';
      const stockName = row[1] || '-';
      const buyAmount = row[2] || 0;
      const buyTimes = row[3] || 0;
      const sellAmount = row[4] || 0;
      const sellTimes = row[5] || 0;
      const netAmount = row[6] || (parseFloat(buyAmount) - parseFloat(sellAmount)).toFixed(2);
      const queryType = row[7] || queryTypeEl.value;
      
      const tr = document.createElement('tr');
      tr.innerHTML = `
        <td>${stockCode}</td>
        <td>${stockName}</td>
        <td>${buyAmount}</td>
        <td>${buyTimes}</td>
        <td>${sellAmount}</td>
        <td>${sellTimes}</td>
        <td>${netAmount}</td>
        <td>${queryType}</td>
      `;
      
      // 设置净额的样式
      const netTd = tr.querySelector('td:nth-child(7)');
      if (parseFloat(netAmount) > 0) {
        netTd.style.color = '#f56c6c';
      } else if (parseFloat(netAmount) < 0) {
        netTd.style.color = '#67c23a';
      }
      
      tbody.appendChild(tr);
    });
  }
  
  // 初始化表头点击事件
  function initTableSorting() {
    const thead = tbody.parentNode.querySelector('thead tr');
    if (thead) {
      Array.from(thead.querySelectorAll('th')).forEach((th, index) => {
        if (sortableColumns[index]) {
          th.style.cursor = 'pointer';
          th.addEventListener('click', function() {
            // 切换排序状态：0 -> 1 -> -1 -> 0
            if (sortColumn === index) {
              // 当前列已经排序，切换到下一个状态
              if (sortDirection === 1) {
                sortDirection = -1; // 升序 -> 降序
              } else if (sortDirection === -1) {
                sortDirection = 0; // 降序 -> 不排序
              } else {
                sortDirection = 1; // 不排序 -> 升序
              }
            } else {
              sortColumn = index;
              sortDirection = 1; // 新列默认升序
            }
            
            // 更新表头样式
            updateTableHeader();
            
            // 重新渲染排序后的数据
            const sortedData = sortData();
            renderTable(sortedData);
          });
        }
      });
    }
  }
  
  // 通用查询函数
  async function queryDragonTiger(isDetail = false) {
    const ingestDate = (dateEl.value || '').trim();
    const queryType = queryTypeEl.value;
    
    console.log('查询参数:', { ingestDate, queryType, isDetail });
    
    // 构建API请求URL
    const apiPath = isDetail ? '/api/stocks/data/dragon_tiger/detail' : '/api/stocks/data/dragon_tiger';
    let url = `${API_BASE}/${apiPath}?date=${encodeURIComponent(ingestDate)}&symbol=${encodeURIComponent(queryType)}`;
    console.log('最终请求URL:', url);
    
    // 重置排序状态
    sortColumn = null;
    sortDirection = 0;
    
    // 设置加载状态
    btn.disabled = true;
    btn.textContent = '查询中...';
    tbody.innerHTML = '';
    loadingEl.style.display = 'block';
    noDataEl.style.display = 'none';
    
    try {
      // 发送请求
      const res = await fetch(url);
      
      if (!res.ok) {
        throw new Error(`HTTP错误，状态码: ${res.status}`);
      }
      
      const data = await res.json();
      
      // 检查响应数据格式
      if (Array.isArray(data)) {
        // 保存原始数据
        currentData = data;
        
        if (data.length === 0) {
          noDataEl.style.display = 'block';
          toast('未找到符合条件的龙虎榜数据');
          return;
        }
        
        // 更新表头样式
        updateTableHeader();
        
        // 渲染表格
        renderTable(data);
        
        toast(`查询完成：${data.length} 条龙虎榜记录${isDetail ? '（详细）' : ''}`);
      } else {
        // 如果不是预期的数组格式，尝试转换为JSON字符串以便调试
        console.log('返回的数据格式:', data);
        noDataEl.style.display = 'block';
        toast('后端返回的数据格式不正确');
      }
    } catch (e) {
      console.error('龙虎榜查询错误:', e);
      noDataEl.style.display = 'block';
      toast('查询失败：' + (e.message || '未知错误'));
    } finally {
      // 恢复按钮状态
      btn.disabled = false;
      btn.textContent = '查询';
      loadingEl.style.display = 'none';
    }
  }
  
  // 初始化表格排序
  initTableSorting();
  
  // 移除可能存在的旧事件监听器，防止重复绑定
  const newBtn = btn.cloneNode(true);
  btn.parentNode.replaceChild(newBtn, btn);
  
  // 查询按钮事件
  newBtn.addEventListener('click', function() {
    queryDragonTiger(false);
  });
  
  // 详情按钮已移除
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

// 关注和预警功能已移除

// 数据状态模块（筛选+趋势+备份健康）
(function initStatus(){
  // 总览和市场分布功能已移除
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
      // 总览和市场分布功能已移除
      // 备份健康
      const bk = data.backup || {};
      const sizeMB = bk.size_bytes ? (bk.size_bytes/1e6).toFixed(2) : '0.00';
      backupEl.innerHTML = `数据库：<code>${bk.db_path||'-'}</code>；大小：<b>${sizeMB} MB</b>；最后更新：<b>${bk.last_modified||'-'}</b>；健康度：<b>${bk.health_score||0}</b>/100`;
      // 上市日期范围功能已移除
      // 趋势图
      renderTrend(data.trends || []);
      // 选项填充（仅首次或空时）
      if (marketSel && marketSel.options.length <= 1) {
        (data.options?.markets||[]).forEach(v => { const opt = document.createElement('option'); opt.value = v; opt.textContent = v; marketSel.appendChild(opt); });
      }
    } catch(e) {
      // 异常处理 - 总览功能已移除
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
  const applyBtn = qs('#configApply');
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

  // 立即生效按钮点击事件
  applyBtn.addEventListener('click', async () => {
    try {
      // 显示加载状态
      applyBtn.disabled = true;
      applyBtn.textContent = '处理中...';
      
      // 调用API#const 
      const response = await fetch(`${API_BASE}/api/configs/schedule/apply`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        }
      });
      
      const data = await response.json();
      
      // 显示结果提示
      if (data.success) {
        toast(`定时任务配置已更新，共配置 ${data.count || 0} 个任务`);
      } else {
        toast(`操作失败: ${data.error || '未知错误'}`, 'error');
      }
    } catch (error) {
      toast(`网络错误: ${error.message}`, 'error');
    } finally {
      // 恢复按钮状态
      applyBtn.disabled = false;
      applyBtn.textContent = '立即生效';
    }
  });

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
