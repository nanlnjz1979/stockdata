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

// 热力图分析模块
(function initHeatmap() {
  let heatmapChart = null;
  const API_BASE = 'http://127.0.0.1:8000';
  
  // 初始化热力图
  function initChart() {
    const container = document.getElementById('heatmapContainer');
    if (!container) return;
    
    // 强制设置容器样式以确保它能完全适应父容器
    container.style.width = '100%';
    container.style.height = '60vh';
    container.style.minHeight = '400px';
    container.style.boxSizing = 'border-box';
    container.style.display = 'block';
    container.style.margin = '0';
    container.style.padding = '0';
    
    // 立即刷新容器的布局计算
    container.offsetWidth; // 触发重排
    
    heatmapChart = echarts.init(container);
    
    // 响应式处理 - 使用防抖处理resize事件
    let resizeTimer;
    window.addEventListener('resize', function() {
      clearTimeout(resizeTimer);
      resizeTimer = setTimeout(handleResize, 50); // 50ms防抖
    });
    
    // 立即执行一次调整大小，确保初始加载时的布局正确
    handleResize();
    
    // 处理热力图调整大小
    function handleResize() {
      if (heatmapChart) {
        // 强制更新容器尺寸
        container.style.width = '100%';
        
        // 触发重排以获取准确的容器宽度
        container.offsetWidth;
        
        // 获取更新后的容器宽度
        const containerWidth = container.clientWidth;
        
        // 更激进地计算每行显示的股票数量，确保充分利用容器宽度
        const newStocksPerRow = Math.max(2, Math.min(30, Math.floor(containerWidth / 50)));
        
        // 调整热力图大小
        heatmapChart.resize();
        
        // 如果热力图已经有数据，重新渲染以适应新的布局
        if (dataCache) {
          // 重新计算布局
          const updatedData = transformToHeatmapFormat(dataCache.raw, dataCache.period, newStocksPerRow);
          renderHeatmap(updatedData);
        }
      }
    }
  }
  
  // 缓存最近的数据，用于响应式调整
  let dataCache = null;
  
  // 获取热力图数据
  async function fetchHeatmapData() {
    const period = document.getElementById('heatmapPeriod')?.value || '30';
    console.log('获取热力图数据，周期:', period);
    
    try {
      // 不设置limit参数，让后端返回所有股票数据
      const url = `${API_BASE}/api/stocks/data/heatmap?period=${period}&limit=50000`;
      console.log('API请求URL:', url);
      const response = await fetch(url);
      console.log('API响应状态:', response.status);
      
      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }
      const backendData = await response.json();
      console.log('后端返回原始数据:', backendData);
      
      // 转换后端返回的真实数据格式为前端需要的热力图格式
      const rawData = backendData.data || [];
      const transformedData = transformToHeatmapFormat(rawData, parseInt(period));
      console.log('转换后的热力图数据:', transformedData);
      
      // 缓存原始数据用于响应式调整
      dataCache = {
        raw: rawData,
        period: parseInt(period)
      };
      
      return transformedData;
    } catch (error) {
      console.error('获取热力图数据失败:', error);
      toast('获取热力图数据失败，请稍后重试');
      // 只返回空数据，不使用模拟数据
      return {
        stockLabels: [],
        data: [],
        dates: []
      };
    }
  }
  
  // 判断是否为交易日（周一到周五为交易日，周末不是交易日）
  function isTradingDay(date) {
    // 参数验证
    if (!date) return false;
    
    const d = new Date(date);
    // 检查日期是否有效
    if (isNaN(d.getTime())) return false;
    
    const day = d.getDay();
    // 周一到周五为交易日（0是周日，6是周六）
    return day >= 1 && day <= 5;
  }
  
  // 计算两个日期之间的交易日数量（去除非交易日如周末）
  function getTradingDaysBetween(startDate, endDate) {
    // 参数验证
    if (!startDate || !endDate) return 0;
    
    let start = new Date(startDate);
    let end = new Date(endDate);
    
    // 检查日期是否有效
    if (isNaN(start.getTime()) || isNaN(end.getTime())) return 0;
    
    // 重置时间部分，只比较日期
    start.setHours(0, 0, 0, 0);
    end.setHours(0, 0, 0, 0);
    
    // 确保start <= end
    if (start > end) {
      [start, end] = [end, start];
    }
    
    let tradingDays = 0;
    
    // 计算两个日期之间的交易日数量，只统计周一到周五
    const current = new Date(start);
    while (current <= end) {
      if (isTradingDay(current)) {
        tradingDays++;
      }
      // 前进一天
      current.setDate(current.getDate() + 1);
    }
    
    // 返回两个日期之间的交易日数量（不包括开始日期本身）
    return tradingDays - 1;
  }
  
  // 根据最后更新日期与今天的交易日差计算更新状态值
  function calculateUpdateStatus(lastUpdateDateStr) {
    if (!lastUpdateDateStr) return 0;
    
    const now = new Date();
    const currentHour = now.getHours();
    
    // 创建今天的日期对象
    const today = new Date();
    
    // 判断当前时间是否超过3点
    // 如果超过3点，今天算作一个完整的交易日
    // 如果没有超过3点，则使用昨天的日期作为结束日期
    const endDate = currentHour >= 15 ? today : new Date(today);
    if (currentHour < 15) {
      endDate.setDate(endDate.getDate() - 1);
    }
    
    // 重置时间部分，只比较日期
    endDate.setHours(0, 0, 0, 0);
    
    const lastUpdate = new Date(lastUpdateDateStr);
    lastUpdate.setHours(0, 0, 0, 0);
    
    // 计算交易日差
    const tradingDaysDiff = getTradingDaysBetween(lastUpdate, endDate);
    
    // 根据交易日差计算更新状态值
    if (tradingDaysDiff === 0) {
      return 1.0; // 今天更新，状态良好
    } else if (tradingDaysDiff === 1) {
      return 0.8; // 昨天更新，状态良好
    } else if (tradingDaysDiff === 2) {
      return 0.6; // 前天更新，状态一般
    } else if (tradingDaysDiff <= 5) {
      return 0.3; // 5个交易日内更新，状态较差
    } else {
      return 0.0; // 超过5个交易日未更新，状态差
    }
  }
  
  // 将后端数据转换为热力图需要的格式 - 每个股票一个格子
  function transformToHeatmapFormat(backendData, period, customStocksPerRow = null) {
    console.log('开始转换数据，后端数据数量:', backendData.length, '周期:', period);
    
    // 获取热力图容器宽度以动态计算每行股票数量
    const container = document.getElementById('heatmapContainer');
    const containerWidth = container ? container.clientWidth : window.innerWidth;
    
    // 更激进地计算每行显示的股票数量，确保充分利用容器宽度
    // 降低每行最小宽度要求，提高最大股票数，以确保更好地适应不同屏幕尺寸
    const stocksPerRow = customStocksPerRow || Math.max(2, Math.min(30, Math.floor(containerWidth / 50)));
    
    console.log(`容器宽度: ${containerWidth}px, 每行股票数: ${stocksPerRow}`);
    
    // 获取今天的日期字符串
    const today = new Date();
    const todayStr = today.toISOString().split('T')[0];
    
    // 获取昨天的日期字符串
    const yesterday = new Date(today);
    yesterday.setDate(yesterday.getDate() - 1);
    const yesterdayStr = yesterday.toISOString().split('T')[0];
    
    // 获取前天的日期字符串
    const dayBeforeYesterday = new Date(today);
    dayBeforeYesterday.setDate(dayBeforeYesterday.getDate() - 2);
    const dayBeforeYesterdayStr = dayBeforeYesterday.toISOString().split('T')[0];
    
    // 只使用真实的后端数据，不使用模拟数据
    const displayData = backendData;
    
    // 不限制显示的股票数量，显示所有股票
    const limitedData = displayData;
    
    // 为每个股票创建一个格子的数据点
    const data = [];
    const stockLabels = [];
    
    limitedData.forEach((stock, index) => {
      // 计算每个股票的坐标位置（x为列，y为行）
      const x = index % stocksPerRow;
      const y = Math.floor(index / stocksPerRow);
      
      // 根据最后更新日期计算更新状态值
      const updateStatus = calculateUpdateStatus(stock.last_update);
      
      // 确定更新状态描述
      let statusText;
      if (updateStatus === 1.0) {
        statusText = '今日已更新';
      } else if (updateStatus === 0.8) {
        statusText = '昨日已更新';
      } else if (updateStatus >= 0.6) {
        statusText = '近期已更新';
      } else if (updateStatus > 0) {
        statusText = '需要更新';
      } else {
        statusText = '严重滞后';
      }
      
      // 存储股票信息
      stockLabels.push({
        x: x,
        y: y,
        code: stock.code,
        name: stock.name,
        update_status: updateStatus,
        last_update: stock.last_update || '未知'
      });
      
      // 计算交易日差，确保只计算实际的交易日
      // 使用endDate而不是today以保持与calculateUpdateStatus函数一致
      const now = new Date();
      const currentHour = now.getHours();
      const today = new Date();
      const endDate = currentHour >= 15 ? today : new Date(today);
      if (currentHour < 15) {
        endDate.setDate(endDate.getDate() - 1);
      }
      endDate.setHours(0, 0, 0, 0);
      
      const tradingDaysDiff = getTradingDaysBetween(stock.last_update, endDate);
      
      // 添加热力图数据点，值使用计算出的更新状态
      data.push([
        x,  // x坐标
        y,  // y坐标
        updateStatus,  // 计算出的更新状态值（0-1）
        {
          code: stock.code,
          name: stock.name,
          update_status: updateStatus,
          status: statusText,
          last_update: stock.last_update || '未知',
          trading_days_diff: tradingDaysDiff || 0
        }
      ]);
    });
    
    console.log('数据转换完成，股票标签数量:', stockLabels.length, '数据点数量:', data.length);
    
    return {
      stockLabels: stockLabels,
      data: data,
      stocksPerRow: stocksPerRow,
      totalRows: Math.ceil(stockLabels.length / stocksPerRow),
      totalStockCount: displayData.length // 保存真实的总股票数量
    };
  }
  
  // 生成模拟数据（用于测试）

  
  // 计算统计数据
  function calculateStatistics(data, totalCount) {
    const stats = {
      total: totalCount || data.length, // 使用传入的总数或默认使用数据长度
      today: 0,
      yesterday: 0,
      recent: 0,
      needUpdate: 0,
      severelyLagged: 0
    };
    
    data.forEach(item => {
      const status = item[2]; // 更新状态值
      if (status === 1.0) stats.today++;
      else if (status === 0.8) stats.yesterday++;
      else if (status >= 0.6) stats.recent++;
      else if (status > 0) stats.needUpdate++;
      else stats.severelyLagged++;
    });
    
    return stats;
  }
  
  // 创建并管理加载动画
  function getLoadingSpinner() {
    let spinner = document.getElementById('loadingSpinner');
    if (!spinner) {
      // 创建加载动画元素
      spinner = document.createElement('div');
      spinner.id = 'loadingSpinner';
      spinner.style.cssText = `
        position: absolute;
        top: 50%;
        left: 50%;
        transform: translate(-50%, -50%);
        width: 50px;
        height: 50px;
        border: 4px solid #f3f3f3;
        border-top: 4px solid #3498db;
        border-radius: 50%;
        animation: spin 1s linear infinite;
        z-index: 1000;
        display: none;
      `;
      
      // 添加动画样式
      const style = document.createElement('style');
      style.textContent = `
        @keyframes spin {
          0% { transform: translate(-50%, -50%) rotate(0deg); }
          100% { transform: translate(-50%, -50%) rotate(360deg); }
        }
      `;
      document.head.appendChild(style);
      
      // 添加到热力图容器中
      const container = document.getElementById('heatmapContainer');
      if (container) {
        container.appendChild(spinner);
      }
    }
    return spinner;
  }
  
  // 显示加载动画
  function showLoading() {
    const spinner = getLoadingSpinner();
    if (spinner) {
      spinner.style.display = 'block';
    }
  }
  
  // 隐藏加载动画
  function hideLoading() {
    const spinner = document.getElementById('loadingSpinner');
    if (spinner) {
      spinner.style.display = 'none';
    }
  }
  
  // 渲染右侧统计框
  function renderStatistics(stats) {
    let statsContainer = document.getElementById('heatmapStats');
    if (!statsContainer) {
      // 创建统计框元素
      statsContainer = document.createElement('div');
      statsContainer.id = 'heatmapStats';
      statsContainer.style.cssText = `
        width: 200px;
        height: 100%;
        background: white;
        border: 2px solid #ddd;
        border-radius: 5px;
        padding: 15px;
        box-shadow: 0 2px 8px rgba(0,0,0,0.1);
        box-sizing: border-box;
        overflow-y: auto;
      `;
      
      // 获取或创建父容器
      let parentContainer = document.getElementById('heatmapWrapper');
      if (!parentContainer) {
        parentContainer = document.createElement('div');
        parentContainer.id = 'heatmapWrapper';
        parentContainer.style.cssText = `
          display: flex;
          width: 100%;
          height: 600px;
          gap: 10px;
          align-items: flex-start;
        `;
        
        // 将现有热力图容器放入包装容器
        const existingContainer = document.getElementById('heatmapContainer');
        if (existingContainer) {
          const parent = existingContainer.parentNode;
          parent.appendChild(parentContainer);
          parentContainer.appendChild(existingContainer);
          parentContainer.appendChild(statsContainer);
        }
      } else {
        parentContainer.appendChild(statsContainer);
      }
    }
    
    // 更新统计内容
    statsContainer.innerHTML = `
      <h4 style="margin-top: 0; color: #333; border-bottom: 1px solid #eee; padding-bottom: 8px;">统计数据</h4>
      <div style="font-size: 14px; line-height: 1.8;">
        <div>总股票数: <strong>${stats.total}</strong></div>
        <div>今日已更新: <strong style="color: #5cb85c;">${stats.today}</strong> (${Math.round(stats.today/stats.total*100)}%)</div>
        <div>昨日已更新: <strong style="color: #90ee90;">${stats.yesterday}</strong> (${Math.round(stats.yesterday/stats.total*100)}%)</div>
        <div>近期已更新: <strong style="color: #ffd700;">${stats.recent}</strong> (${Math.round(stats.recent/stats.total*100)}%)</div>
        <div>需要更新: <strong style="color: #f0ad4e;">${stats.needUpdate}</strong> (${Math.round(stats.needUpdate/stats.total*100)}%)</div>
        <div>严重滞后: <strong style="color: #d9534f;">${stats.severelyLagged}</strong> (${Math.round(stats.severelyLagged/stats.total*100)}%)</div>
      </div>
    `;
  }
  
  // 渲染热力图 - 每个股票一个格子
  function renderHeatmap(data) {
    console.log('渲染热力图数据:', data);
    if (!heatmapChart || !data || !data.stockLabels || !data.data) {
      console.error('热力图数据不完整或图表未初始化');
      hideLoading(); // 确保即使数据不完整也隐藏加载动画
      return;
    }
    
    console.log('股票标签数量:', data.stockLabels.length);
    console.log('数据点数量:', data.data.length);
    console.log('每行股票数:', data.stocksPerRow);
    console.log('总行数:', data.totalRows);
    
    // 计算统计数据 - 传入真实的总股票数（如果有），否则使用显示的数据长度
    const totalStockCount = data.totalStockCount || data.data.length;
    const stats = calculateStatistics(data.data, totalStockCount);
    // 渲染统计框
    renderStatistics(stats);
    
    // 计算合适的网格范围
    const xAxisData = Array.from({length: data.stocksPerRow}, (_, i) => i.toString());
    const yAxisData = Array.from({length: data.totalRows}, (_, i) => i.toString());
    
    const option = {
      title: {
        text: '股票数据更新状态热力图 - 基于交易日对比',
        left: 'center'
      },
      tooltip: {
        position: 'top',
        formatter: function(params) {
          if (params.data && params.data[3]) {
            const data = params.data[3];
            return `
              <div style="padding: 8px;">
                <div><strong>股票代码:</strong> ${data.code}</div>
                <div><strong>股票名称:</strong> ${data.name}</div>
                <div><strong>更新状态:</strong> ${data.status}</div>
                <div><strong>最后更新:</strong> ${data.last_update}</div>
                <div><strong>交易日差:</strong> ${data.trading_days_diff} 天</div>
              </div>
            `;
          }
          return '无数据';
        }
      },
      grid: {
        height: '85%',
        top: '8%',
        left: '1%',
        right: '1%', // 不需要再为统计框留空间，已经通过外部布局处理
        bottom: '6%',
        containLabel: true,
        borderColor: '#ddd',
        borderWidth: 2
      },
      xAxis: {
        type: 'category',
        data: xAxisData,
        splitArea: {
          show: true
        },
        axisLabel: {
          show: false // 不显示x轴标签
        },
        axisLine: {
          show: false
        },
        axisTick: {
          show: false
        }
      },
      yAxis: {
        type: 'category',
        data: yAxisData,
        splitArea: {
          show: true
        },
        axisLabel: {
          show: false // 不显示y轴标签
        },
        axisLine: {
          show: false
        },
        axisTick: {
          show: false
        }
      },
      visualMap: {
        min: 0,
        max: 1,
        calculable: false,
        orient: 'horizontal',
        left: 'center',
        bottom: '0%', // 进一步降低到底部
        // 确保颜色映射方向与数据值范围一致，数值高的显示绿色，数值低的显示红色
        inRange: {
          color: [
            '#d9534f',  // 红色 - 超过5个交易日未更新 (低值)
            '#f0ad4e',  // 橙色 - 3-5个交易日未更新
            '#ffd700',  // 黄色 - 2个交易日未更新
            '#90ee90',  // 浅绿色 - 1个交易日未更新
            '#5cb85c'   // 深绿色 - 今日已更新 (高值)
          ]
        },
        // 明确绑定到数据值
        dimension: 2,
        text: ['更新及时', '需要更新'],
        formatter: function(value) {
          if (value === 1.0) return '今日已更新';
          if (value === 0.8) return '昨日已更新';
          if (value >= 0.6) return '近期已更新';
          if (value > 0) return '需要更新';
          return '严重滞后';
        },
        textStyle: {
          fontSize: 10 // 减小字体大小以节省空间
        },
        // 设置分段式颜色显示
        pieces: [
          {min: 0.9, max: 1.0, label: '今日已更新'},
          {min: 0.7, max: 0.9, label: '昨日已更新'},
          {min: 0.5, max: 0.7, label: '近期已更新'},
          {min: 0.1, max: 0.5, label: '需要更新'},
          {min: 0, max: 0.1, label: '严重滞后'}
        ],
        // 紧凑布局
        itemSymbol: 'circle',
        itemWidth: 10,
        itemHeight: 10
      },
      series: [
        {
          name: '股票更新状态',
          type: 'heatmap',
          data: data.data,
          label: {
            show: true,
            formatter: function(params) {
              // 在格子中显示股票代码（后4位）
              if (params.data && params.data[3]) {
                return params.data[3].code.slice(-4);
              }
              return '';
            },
            fontSize: 11,
            color: function(params) {
              // 根据背景色自动选择文字颜色（深色背景用白色文字，浅色背景用黑色文字）
              const value = params.data[2];
              return value < 0.5 ? '#fff' : '#000'; // 状态值小于0.5用白色文字，否则用黑色
            }
          },
          emphasis: {
            label: {
              show: true,
              fontSize: 14,
              fontWeight: 'bold',
              color: '#fff' // 悬停时始终用白色文字以提高可读性
            }
          },
          // 设置单元格大小
          symbolSize: function() {
            // 根据容器大小动态调整单元格大小
            const container = document.getElementById('heatmapContainer');
            if (!container) return [50, 50];
            
            // 强制获取容器的最新宽度
            container.offsetWidth;
            
            const width = container.clientWidth;
            const height = container.clientHeight;
            
            // 最大化利用容器空间，减少边距
            const cellWidth = Math.floor(width / data.stocksPerRow * 0.98);
            // 保持单元格为正方形
            const cellHeight = cellWidth;
            
            // 再次放宽单元格大小限制，确保在小屏幕上也能显示更多格子
            return [
              Math.min(Math.max(cellWidth, 20), 150),
              Math.min(Math.max(cellHeight, 20), 150)
            ];
          },
          // 配置热力图的布局
          progressive: 1000,
          progressiveThreshold: 1000,
          // 调整单元格间距和样式
          itemStyle: {
            borderColor: '#ddd', // 修改为更明显的边框颜色
            borderWidth: 2,
            borderRadius: 2,
            emphasis: {
              shadowBlur: 10,
              shadowColor: 'rgba(0, 0, 0, 0.5)'
            }
          }
        }
      ]
    };
    
    heatmapChart.setOption(option);
  }
  
  // 刷新热力图
  async function refreshHeatmap() {
    console.log('开始刷新热力图');
    // 显示加载动画
    showLoading();
    
    try {
      const data = await fetchHeatmapData();
      console.log('获取热力图数据成功，准备渲染');
      
      // 确保容器尺寸正确后再渲染
      const container = document.getElementById('heatmapContainer');
      if (container) {
        // 强制更新容器尺寸并触发重排
        container.style.width = 'calc(100% - 210px)'; // 减去统计框宽度和间隙
        container.style.height = '100%';
        container.style.border = '2px solid #ddd'; // 为热力图容器添加边框
        container.style.borderRadius = '5px';
        container.style.boxSizing = 'border-box';
        container.style.position = 'relative'; // 确保加载动画可以正确定位
        container.offsetWidth;
        
        // 渲染热力图
        renderHeatmap(data);
        
        // 渲染完成后再次调整大小，确保完全适应
        setTimeout(() => {
          if (heatmapChart) {
            heatmapChart.resize();
          }
        }, 100);
      }
    } catch (error) {
      console.error('刷新热力图失败:', error);
    } finally {
      // 无论成功失败都隐藏加载动画
      hideLoading();
    }
  }
  
  // 绑定事件
  function bindEvents() {
    const refreshBtn = document.getElementById('heatmapRefresh');
    if (refreshBtn) {
      refreshBtn.addEventListener('click', refreshHeatmap);
    }
    
    // 周期改变时自动刷新
    const periodSelect = document.getElementById('heatmapPeriod');
    if (periodSelect) {
      periodSelect.addEventListener('change', refreshHeatmap);
    }
  }
  
  // 初始化
  function initialize() {
    // 等待DOM完全加载
    if (document.readyState === 'loading') {
      document.addEventListener('DOMContentLoaded', finishInit);
    } else {
      finishInit();
    }
    
    function finishInit() {
      initChart();
      bindEvents();
      
      // 当分析页面被显示时自动加载数据
      const analysisSection = document.getElementById('analysis');
      const observer = new MutationObserver((mutations) => {
        mutations.forEach((mutation) => {
          if (mutation.attributeName === 'style') {
            const displayStyle = analysisSection.style.display;
            if (!displayStyle || displayStyle !== 'none') {
              // 短暂延迟以确保DOM已渲染完成
              setTimeout(() => {
                // 确保容器尺寸正确
                const container = document.getElementById('heatmapContainer');
                if (container) {
                  container.style.width = '100%';
                  container.offsetWidth;
                  refreshHeatmap();
                }
              }, 150); // 增加延迟以确保DOM完全渲染
            }
          }
        });
      });
      
      observer.observe(analysisSection, {
        attributes: true
      });
      
      // 如果当前已经在分析页面，立即加载数据
      if (analysisSection && (!analysisSection.style.display || analysisSection.style.display !== 'none')) {
        setTimeout(() => {
          const container = document.getElementById('heatmapContainer');
          if (container) {
            container.style.width = '100%';
            container.offsetWidth;
            refreshHeatmap();
          }
        }, 200);
      }
    }
  }
  
  // 导出功能支持
  window.exportHeatmap = function() {
    if (heatmapChart) {
      const link = document.createElement('a');
      link.download = 'heatmap.png';
      link.href = heatmapChart.getDataURL({
        type: 'png',
        pixelRatio: 2,
        backgroundColor: '#fff'
      });
      link.click();
      toast('热力图导出成功');
    }
  };
  
  // 启动初始化
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initialize);
  } else {
    initialize();
  }
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

// 数据状态模块（筛选+备份健康）
(function initStatus(){
  
  // 选项卡切换功能
  function initTabs() {
    const tabBtns = document.querySelectorAll('.tab-btn');
    const tabContents = document.querySelectorAll('.tab-content');
    
    tabBtns.forEach(btn => {
      btn.addEventListener('click', () => {
        const tabId = btn.getAttribute('data-tab');
        
        // 更新按钮状态
        tabBtns.forEach(b => b.classList.remove('active'));
        btn.classList.add('active');
        
        // 更新内容显示
        tabContents.forEach(content => {
          content.classList.remove('active');
          if (content.id === tabId) {
            content.classList.add('active');
          }
        });
      });
    });
  }

  // 进度条动画函数
  function animateProgress(progressBar, progressText, targetPercent, message = '处理中...') {
    let current = 0;
    const interval = setInterval(() => {
      current += 1;
      progressBar.style.width = `${current}%`;
      progressText.textContent = `${message} ${current}%`;
      
      if (current >= targetPercent) {
        clearInterval(interval);
        setTimeout(() => {
          progressBar.parentElement.style.display = 'none';
        }, 1000);
      }
    }, 30);
  }

  // 数据完整性检查
  function initIntegrityCheck() {
    // 初始化筛选功能
    initFilter();
    const checkBtn = document.getElementById('checkIntegrityBtn');
    const progressEl = document.getElementById('integrityProgress');
    const progressBar = document.getElementById('integrityProgressBar');
    const progressText = document.getElementById('integrityProgressText');
    
    // 获取结果容器
    function getResultsContainer() {
      return document.getElementById('integrityResultsContainer');
    }
    
    // 存储所有结果记录，用于筛选
    let allResults = [];
    
    // 创建问题详情模态框
      function createIssueModal() {
        let modal = document.getElementById('issueDetailModal');
        if (!modal) {
          modal = document.createElement('div');
          modal.id = 'issueDetailModal';
          modal.className = 'modal';
          modal.style.display = 'none';
          modal.style.position = 'fixed';
          modal.style.zIndex = '1000';
          modal.style.left = '0';
          modal.style.top = '0';
          modal.style.width = '100%';
          modal.style.height = '100%';
          modal.style.overflow = 'auto';
          modal.style.backgroundColor = 'rgba(0,0,0,0.4)';
          modal.innerHTML = `
            <div style="background-color: #fefefe; margin: 15% auto; padding: 20px; border: 1px solid #888; width: 80%; max-width: 800px; max-height: 70vh; overflow-y: auto; border-radius: 8px;">
              <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px;">
                <h2 style="margin: 0;">股票数据问题详情</h2>
                <button id="closeModalBtn" style="background: none; border: none; font-size: 28px; cursor: pointer; color: #aaa;">&times;</button>
              </div>
              <div id="modalContent"></div>
            </div>
          `;
          document.body.appendChild(modal);
          
          // 添加关闭按钮事件
          document.getElementById('closeModalBtn').addEventListener('click', function() {
            modal.style.display = 'none';
          });
          
          // 点击模态框外部关闭
          window.addEventListener('click', function(event) {
            if (event.target === modal) {
              modal.style.display = 'none';
            }
          });
        }
        return modal;
      }
      
      // 显示问题详情
      function showIssueDetails(stockCode, stockName) {
        // 查找对应的结果记录
        const result = allResults.find(r => r.stock_code === stockCode);
        if (!result || !result.details) return;
        
        // 创建并显示模态框
        const modal = createIssueModal();
        const content = document.getElementById('modalContent');
        
        // 构建详细内容
        let detailHtml = `<h3>${stockCode} ${stockName}</h3>`;
        detailHtml += '<div style="margin-top: 15px;">';
        
        // 显示详细问题记录
        if (result.details.date_gaps && result.details.date_gaps.length > 0) {
          detailHtml += '<div style="margin-bottom: 15px;">';
          detailHtml += '<h4>缺失的日期:</h4>';
          detailHtml += '<p>' + result.details.date_gaps.join(', ') + '</p>';
          detailHtml += '</div>';
        }
        
        if (result.details.missing_adjustment && result.details.missing_adjustment.length > 0) {
          detailHtml += '<div style="margin-bottom: 15px;">';
          detailHtml += '<h4>缺失的复权数据:</h4>';
          detailHtml += '<p>' + result.details.missing_adjustment.join(', ') + '</p>';
          detailHtml += '</div>';
        }
        
        if (result.details.old_data_dates && result.details.old_data_dates.length > 0) {
          detailHtml += '<div style="margin-bottom: 15px;">';
          detailHtml += '<h4>需要更新的数据日期:</h4>';
          detailHtml += '<p>' + result.details.old_data_dates.join(', ') + '</p>';
          detailHtml += '</div>';
        }
        
        // 如果没有详细信息
        if (!result.details.date_gaps && !result.details.missing_adjustment && !result.details.old_data_dates) {
          detailHtml += '<p>暂无详细问题记录</p>';
        }
        
        detailHtml += '</div>';
        content.innerHTML = detailHtml;
        modal.style.display = 'block';
      }
      
      // 添加单条结果到表格开头
      function addResultToTable(result) {
        const tbody = document.getElementById('integrityResultsTable');
        if (!tbody) return;
        
        // 保存结果到数组
        allResults.push(result);
        
        // 检查是否需要显示该记录
        if (!shouldShowRecord(result)) {
          return; // 如果不符合筛选条件，不添加到表格
        }
        
        const row = document.createElement('tr');
        // 存储结果状态，用于筛选
        row.dataset.status = result.status;
        
        // 设置状态样式
        let statusClass = 'status-complete';
        let statusText = '完整';
        if (result.status === 'partial') {
          statusClass = 'status-partial';
          statusText = '部分完整';
        } else if (result.status === 'missing') {
          statusClass = 'status-missing';
          statusText = '缺失';
        }
        
        row.innerHTML = `
          <td>${result.stock_code}</td>
          <td>${result.stock_name}</td>
          <td>${result.total_records}</td>
          <td><span class="status-badge ${statusClass}" data-stock-code="${result.stock_code}" data-stock-name="${result.stock_name}">${statusText}</span></td>
          <td>${result.issues && result.issues.length > 0 ? result.issues.join(', ') : '-'}</td>
        `;
        
        // 将新行插入到表格开头
        if (tbody.firstChild) {
          tbody.insertBefore(row, tbody.firstChild);
        } else {
          tbody.appendChild(row);
        }
        
        // 添加点击事件到部分完整状态标签
        if (result.status === 'partial') {
          const statusBadge = row.querySelector('.status-badge.status-partial');
          if (statusBadge) {
            statusBadge.style.cursor = 'pointer';
            statusBadge.addEventListener('click', function() {
              const stockCode = this.getAttribute('data-stock-code');
              const stockName = this.getAttribute('data-stock-name');
              showIssueDetails(stockCode, stockName);
            });
          }
        }
      }
    
    // 检查记录是否应该显示
    function shouldShowRecord(result) {
      const selectedOption = document.querySelector('input[name="showComplete"]:checked').value;
      if (selectedOption === 'all') {
        return true; // 显示所有记录
      } else if (selectedOption === 'onlyProblem') {
        // 仅显示有问题的记录（状态不是ok/complete）
        return result.status !== 'ok' && result.status !== 'complete';
      }
      return true;
    }
    
    // 筛选并重新显示所有记录
      function filterRecords() {
        const tbody = document.getElementById('integrityResultsTable');
        if (!tbody) return;
        
        // 清空表格
        tbody.innerHTML = '';
        
        // 重新添加符合条件的记录
        allResults.forEach(result => {
          if (shouldShowRecord(result)) {
            const row = document.createElement('tr');
            row.dataset.status = result.status;
            
            // 设置状态样式
            let statusClass = 'status-complete';
            let statusText = '完整';
            if (result.status === 'partial') {
              statusClass = 'status-partial';
              statusText = '部分完整';
            } else if (result.status === 'missing') {
              statusClass = 'status-missing';
              statusText = '缺失';
            }
            
            row.innerHTML = `
              <td>${result.stock_code}</td>
              <td>${result.stock_name}</td>
              <td>${result.total_records}</td>
              <td><span class="status-badge ${statusClass}" data-stock-code="${result.stock_code}" data-stock-name="${result.stock_name}">${statusText}</span></td>
              <td>${result.issues && result.issues.length > 0 ? result.issues.join(', ') : '-'}</td>
            `;
            
            // 添加到表格（保持原有顺序）
            tbody.appendChild(row);
            
            // 添加点击事件到部分完整状态标签
            if (result.status === 'partial') {
              const statusBadge = row.querySelector('.status-badge.status-partial');
              if (statusBadge) {
                statusBadge.style.cursor = 'pointer';
                statusBadge.addEventListener('click', function() {
                  const stockCode = this.getAttribute('data-stock-code');
                  const stockName = this.getAttribute('data-stock-name');
                  showIssueDetails(stockCode, stockName);
                });
              }
            }
          }
        });
      }
    
    // 初始化筛选功能
    function initFilter() {
      const radioButtons = document.querySelectorAll('input[name="showComplete"]');
      radioButtons.forEach(radio => {
        radio.addEventListener('change', filterRecords);
      });
    }
    
    checkBtn.addEventListener('click', async () => {
      // 显示进度条
      progressEl.style.display = 'block';
      progressBar.style.width = '0%';
      checkBtn.disabled = true;
      
      // 获取结果容器并清空表格
      const container = getResultsContainer();
      if (container) {
        container.style.display = 'block'; // 显示结果容器
      }
      const tbody = document.getElementById('integrityResultsTable');
      if (tbody) {
        tbody.innerHTML = '';
      }
      
      // 重置结果数组
      allResults = [];
      
      try {
        // 第一步：获取统计数据
        const statsResponse = await fetch(`${API_BASE}/api/stocks/integrity/check`);
        const statsData = await statsResponse.json();
        
        // 更新统计显示
        document.getElementById('integrityTotal').textContent = statsData.total;
        document.getElementById('integrityMissing').textContent = statsData.missing;
        document.getElementById('integrityComplete').textContent = statsData.complete;
        
        // 第二步：使用stocks_with_data逐个检查每个股票代码
        const stockCodes = statsData.stocks_with_data || [];
        let problemCount = 0;
        
        for (let i = 0; i < stockCodes.length; i++) {
          const stockCode = stockCodes[i];
          
          // 调用API检查单个股票
          const stockResponse = await fetch(`${API_BASE}/api/stocks/integrity/check`, {
            method: 'POST',
            headers: {
              'Content-Type': 'application/json'
            },
            body: JSON.stringify({ stock_code: stockCode })
          });
          
          const stockData = await stockResponse.json();
          
          // 添加结果到表格（新记录显示在最前面）
          if (stockData) {
            addResultToTable(stockData);
            if (stockData.status !== 'ok') {
              problemCount++;
            }
          }
          
          // 更新进度
          const progress = ((i + 1) / stockCodes.length) * 100;
          progressBar.style.width = `${progress}%`;
          progressText.textContent = `完整性检查中 ${progress.toFixed(1)}% - ${stockCode}`;
          
          // 短暂延迟以允许UI更新
          await new Promise(resolve => setTimeout(resolve, 100));
        }
        
        toast(`数据完整性检查完成：共检查${stockCodes.length}只股票，发现${problemCount}只股票有问题`);
        
      } catch (error) {
        progressText.textContent = '检查失败';
        toast('检查失败：' + error.message);
        console.error('Integrity check error:', error);
      } finally {
        checkBtn.disabled = false;
      }
    });
  }

  // 数据格式标准化
  function initFormatStandardization() {
    const standardizeBtn = document.getElementById('standardizeFormatBtn');
    const progressEl = document.getElementById('formatProgress');
    const progressBar = document.getElementById('formatProgressBar');
    const progressText = document.getElementById('formatProgressText');
    const resultsContainer = document.getElementById('formatResultsContainer');
    const resultsTable = document.getElementById('formatResultsTable');
    
    // 添加错误处理，防止元素不存在时出错
    if (!standardizeBtn || !progressEl || !progressBar || !progressText) {
      console.warn('格式标准化相关元素未找到，跳过初始化');
      return;
    }
    
    standardizeBtn.addEventListener('click', async () => {
      // 显示进度条
      progressEl.style.display = 'block';
      progressBar.style.width = '0%';
      standardizeBtn.disabled = true;
      
      // 清空并隐藏结果列表
      if (resultsTable) resultsTable.innerHTML = '';
      if (resultsContainer) resultsContainer.style.display = 'none';
      
      try {
        // 第一步：获取股票代码列表（调用stocks/integrity/check接口的GET方法）
        progressText.textContent = '获取股票列表...';
        const stocksResponse = await fetch(`${API_BASE}/api/stocks/integrity/check`);
        
        if (!stocksResponse.ok) {
          throw new Error(`获取股票列表失败: ${stocksResponse.status}`);
        }
        
        const stocksData = await stocksResponse.json();
        // 从响应中提取有数据的股票列表
        const stockCodes = stocksData?.stocks_with_data || [];
        
        if (stockCodes.length === 0) {
          toast('没有可处理的股票数据');
          return;
        }
        
        progressText.textContent = `开始格式标准化检查，共${stockCodes.length}只股票`;
        
        // 初始化统计数据
        let totalProcessed = 0;
        let standardizedCount = 0;
        let nonStandardCount = 0;
        let allCheckResults = [];
        
        // 第二步：对每个股票进行格式标准化检查
        for (let i = 0; i < stockCodes.length; i++) {
          const stockCode = stockCodes[i];
          
          try {
            // 调用格式标准化检查API
            progressText.textContent = `格式标准化检查中 ${i+1}/${stockCodes.length} - ${stockCode}`;
            const standardizationResponse = await fetch(
              `${API_BASE}/api/stocks/format/standardization`,
              {
                method: 'POST',
                headers: {
                  'Content-Type': 'application/json'
                },
                body: JSON.stringify({ stock_code: stockCode })
              }
            );
            
            if (!standardizationResponse.ok) {
              throw new Error(`股票${standardizationResponse.statusText}`);
            }
            
            const checkResult = await standardizationResponse.json();
            allCheckResults.push(checkResult);
            
            // 显示结果列表
              if (resultsContainer) resultsContainer.style.display = 'block';
              
              // 添加结果到表格顶部（确保最新的在上面）
              if (resultsTable) addResultToTable(checkResult, new Date());
            
            // 更新统计数据
            totalProcessed++;
            if (checkResult.overall_status === 'pass') {
              standardizedCount++;
            } else {
              nonStandardCount++;
            }
            
          } catch (err) {
            console.error(`处理股票${stockCode}时出错:`, err);
            nonStandardCount++;
            totalProcessed++;
            
            // 添加错误结果到表格
              if (resultsContainer) resultsContainer.style.display = 'block';
              if (resultsTable) {
                addResultToTable({
                  stock_code: stockCode,
                  stock_name: '未知',
                  overall_status: 'error',
                  details: { error: err.message }
                }, new Date());
              }
          }
          
          // 更新进度
          const progress = ((i + 1) / stockCodes.length) * 100;
          progressBar.style.width = `${progress}%`;
          
          // 短暂延迟以允许UI更新
          await new Promise(resolve => setTimeout(resolve, 100));
        }
        
        // 更新统计数据显示
        document.getElementById('formatTotal').textContent = totalProcessed;
        document.getElementById('formatStandardized').textContent = standardizedCount;
        document.getElementById('formatNonStandard').textContent = nonStandardCount;
        
        // 显示详细结果（可以根据需要扩展）
        console.log('格式标准化检查结果:', allCheckResults);
        
        toast(`格式标准化检查完成：共处理${totalProcessed}只股票，${standardizedCount}只格式正常，${nonStandardCount}只有格式问题`);
        
      } catch (error) {
        console.error('格式标准化检查失败:', error);
        toast('格式标准化检查失败: ' + error.message);
      } finally {
        standardizeBtn.disabled = false;
      }
    });
    
    // 添加结果到表格的函数
    function addResultToTable(result, checkTime) {
      const formattedTime = checkTime.toLocaleString('zh-CN', {
        year: 'numeric',
        month: '2-digit',
        day: '2-digit',
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit'
      });
      
      // 处理四个检查项的结果
      const checkItems = {
        accuracy_check: result?.accuracy_check || { status: 'unknown' },
        logical_check: result?.logical_check || { status: 'unknown' },
        format_check: result?.format_check || { status: 'unknown' },
        suspension_check: result?.suspension_check || { status: 'unknown' }
      };
      
      // 构建问题描述
      let issues = [];
      if (result.overall_status === 'error') {
        issues.push(result?.error || '检查过程发生错误');
      } else {
        // 收集各检查项的问题
        Object.entries(checkItems).forEach(([key, check]) => {
          if (check.status && check.status !== 'pass' && check.issues && check.issues.length > 0) {
            const checkNames = {
              accuracy_check: '准确性',
              logical_check: '逻辑性',
              format_check: '格式',
              suspension_check: '停牌日'
            };
            issues.push(`${checkNames[key]}问题: ${check.issues.join('; ')}`);
          }
        });
      }
      
      const issuesText = issues.length > 0 ? issues.join(', ') : '无问题';
      
      // 设置状态样式
      let statusText, statusClass;
      switch (result.overall_status) {
        case 'pass':
          statusText = '通过';
          statusClass = 'success';
          break;
        case 'warning':
          statusText = '警告';
          statusClass = 'warning';
          break;
        case 'error':
          statusText = '错误';
          statusClass = 'error';
          break;
        default:
          statusText = '未知';
          statusClass = '';
      }
      
      // 检查项状态样式函数
      const getCheckStatusHTML = (checkName, checkData) => {
        let checkStatusText, checkStatusClass;
        switch (checkData.status) {
          case 'pass':
            checkStatusText = '通过';
            checkStatusClass = 'success';
            break;
          case 'warn':
          case 'warning':
            checkStatusText = '警告';
            checkStatusClass = 'warning';
            break;
          case 'fail':
          case 'error':
            checkStatusText = '失败';
            checkStatusClass = 'error';
            break;
          default:
            checkStatusText = '未知';
            checkStatusClass = '';
        }
        
        // 是否有详细信息
        const hasDetails = checkData.issues && checkData.issues.length > 0 || 
                          (checkData.details && checkData.details.suspension_days && checkData.details.suspension_days.length > 0);
        const titleText = hasDetails ? `点击查看${checkName}详细问题` : `${checkName}检查结果`;
        
        return `
          <span 
            class="check-item ${checkStatusClass}" 
            style="
              cursor: ${hasDetails ? 'pointer' : 'default'};
              padding: 2px 6px;
              border-radius: 8px;
              font-size: 11px;
              ${checkStatusClass === 'success' ? 'background-color: #e8f5e8; color: #388e3c;' : 
                checkStatusClass === 'warning' ? 'background-color: #fff3cd; color: #856404;' : 
                checkStatusClass === 'error' ? 'background-color: #f8d7da; color: #721c24;' : 
                'background-color: #e9ecef; color: #6c757d;'}
              ${hasDetails ? 'border: 1px dashed #ccc;' : ''}
            "
            title="${titleText}"
            data-check-data='${JSON.stringify(checkData)}'
          >
            ${checkStatusText}
            ${hasDetails ? '<small style="margin-left: 2px; color: #666;">(点击查看)</small>' : ''}
          </span>
        `;
      };
      
      // 创建行元素并添加到表格顶部
      const row = document.createElement('tr');
      row.className = statusClass;
      row.innerHTML = `
        <td style="padding: 8px 10px; border-bottom: 1px solid #e0e0e0;">${formattedTime}</td>
        <td style="padding: 8px 10px; border-bottom: 1px solid #e0e0e0;">${result.stock_code || '未知'}</td>
        <td style="padding: 8px 10px; border-bottom: 1px solid #e0e0e0;">${result.stock_name || '未知'}</td>
        <td style="padding: 8px 10px; border-bottom: 1px solid #e0e0e0;">
          <span style="
            padding: 2px 8px;
            border-radius: 12px;
            font-size: 12px;
            ${statusClass === 'success' ? 'background-color: #e8f5e8; color: #388e3c;' : 
              statusClass === 'warning' ? 'background-color: #fff3cd; color: #856404;' : 
              statusClass === 'error' ? 'background-color: #f8d7da; color: #721c24;' : 
              'background-color: #e9ecef; color: #6c757d;'}
          ">
            ${statusText}
          </span>
        </td>
        <td style="padding: 8px 10px; border-bottom: 1px solid #e0e0e0;">
          <div style="display: flex; gap: 6px; flex-wrap: wrap;">
            <div style="display: flex; flex-direction: column; align-items: center; gap: 4px;">
              <span style="font-size: 11px; color: #666;">准确性</span>
              ${getCheckStatusHTML('准确性', checkItems.accuracy_check)}
            </div>
            <div style="display: flex; flex-direction: column; align-items: center; gap: 4px;">
              <span style="font-size: 11px; color: #666;">逻辑性</span>
              ${getCheckStatusHTML('逻辑性', checkItems.logical_check)}
            </div>
            <div style="display: flex; flex-direction: column; align-items: center; gap: 4px;">
              <span style="font-size: 11px; color: #666;">格式</span>
              ${getCheckStatusHTML('格式', checkItems.format_check)}
            </div>
            <div style="display: flex; flex-direction: column; align-items: center; gap: 4px;">
              <span style="font-size: 11px; color: #666;">停牌日</span>
              ${getCheckStatusHTML('停牌日', checkItems.suspension_check)}
            </div>
          </div>
        </td>
        <td style="padding: 8px 10px; border-bottom: 1px solid #e0e0e0; max-width: 300px; word-wrap: break-word;">${issuesText}</td>
      `;
      
      // 添加点击事件处理程序，用于显示详细问题
      row.querySelectorAll('.check-item[data-check-data]').forEach(item => {
        item.addEventListener('click', function() {
          const checkData = JSON.parse(this.getAttribute('data-check-data'));
          const issues = checkData.issues || [];
          const checkName = this.parentElement.querySelector('span:first-child').textContent;
          
          // 创建并显示模态框
          let modal = document.getElementById('issueDetailModal');
          if (!modal) {
            modal = document.createElement('div');
            modal.id = 'issueDetailModal';
            modal.className = 'dialog-overlay';
            modal.style.display = 'none';
            modal.innerHTML = `
              <div class="dialog">
                <div class="dialog-header">
                  <h3>${checkName}检查详细问题</h3>
                  <button class="dialog-close">&times;</button>
                </div>
                <div class="dialog-content" id="modalIssuesContent"></div>
              </div>
            `;
            document.body.appendChild(modal);
            
            // 添加关闭事件
            modal.querySelector('.dialog-close').addEventListener('click', function() {
              modal.style.display = 'none';
            });
            
            // 点击外部关闭
            modal.addEventListener('click', function(event) {
              if (event.target === modal) {
                modal.style.display = 'none';
              }
            });
          }
          
          // 更新模态框内容
          modal.querySelector('h3').textContent = `${checkName}检查详细问题`;
          const content = modal.querySelector('#modalIssuesContent');
          
          // 构建详细内容
          let issuesHtml = `<div style="margin-bottom: 16px;">
            <strong>股票代码:</strong> ${result.stock_code || '未知'}<br>
            <strong>股票名称:</strong> ${result.stock_name || '未知'}<br>
            <strong>检查时间:</strong> ${formattedTime}
          </div>`;
          
          // 特殊处理停牌日检查，尝试显示具体的停牌日期列表
          if (checkName === '停牌日') {
            issuesHtml += `<div style="margin-top: 16px;">
              <h4 style="margin-top: 0; margin-bottom: 12px; color: #333;">停牌日期列表:</h4>`;
            
            // 尝试从checkItems中获取详细的停牌日期信息
            const suspensionData = checkItems.suspension_check;
            if (suspensionData && suspensionData.details && suspensionData.details.suspension_days) {
              const suspensionDays = suspensionData.details.suspension_days;
              if (suspensionDays.length > 0) {
                // 显示总个数
                issuesHtml += `<div style="margin-bottom: 12px; padding: 8px; background-color: #f8f9fa; border-radius: 4px;">
                  <strong>停牌日总个数:</strong> <span style="color: #d32f2f; font-weight: bold;">${suspensionDays.length}</span> 天
                </div>`;
                
                // 按月份分组显示停牌日期
                const groupedDays = {};
                suspensionDays.forEach(day => {
                  // 提取年月作为分组键
                  const monthKey = day.substring(0, 7); // 格式如 "2023-01"
                  if (!groupedDays[monthKey]) {
                    groupedDays[monthKey] = [];
                  }
                  groupedDays[monthKey].push(day);
                });
                
                issuesHtml += `<div style="margin-left: 10px;">`;
                // 遍历所有月份分组
                let monthIndex = 0;
                Object.entries(groupedDays).sort().forEach(([month, days]) => {
                  const monthName = month.substring(0, 4) + '年' + month.substring(5) + '月';
                  const monthId = `month_${monthIndex}`;
                  monthIndex++;
                  
                  issuesHtml += `<div style="margin-bottom: 8px;">
                    <div style="cursor: pointer; padding: 6px; background-color: #e9ecef; border-radius: 4px; display: flex; justify-content: space-between; align-items: center;">
                      <span style="color: #333; font-weight: bold;">${monthName}</span>
                      <span style="color: #666; display: flex; align-items: center;">
                        <span style="margin-right: 8px;">(${days.length}个)</span>
                        <span class="toggle-icon" data-target="${monthId}" style="font-size: 12px; transition: transform 0.2s;">▼</span>
                      </span>
                    </div>
                    <div id="${monthId}" style="display: none; margin-top: 6px; margin-left: 15px;">
                      <ul style="margin: 0; padding-left: 20px;">`;
                  days.forEach(day => {
                    issuesHtml += `<li style="margin-bottom: 4px; line-height: 1.3; color: #666;">${day}</li>`;
                  });
                  issuesHtml += `</ul>
                    </div>
                  </div>`;
                });
                
                // 模态框内容加载后添加事件处理
                setTimeout(() => {
                  // 为所有切换图标添加点击事件
                  document.querySelectorAll('.toggle-icon').forEach(icon => {
                    // 先移除可能存在的旧事件监听器
                    const newIcon = icon.cloneNode(true);
                    icon.parentNode.replaceChild(newIcon, icon);
                    
                    newIcon.addEventListener('click', function(e) {
                      e.stopPropagation();
                      const targetId = this.getAttribute('data-target');
                      const targetElement = document.getElementById(targetId);
                      if (targetElement.style.display === 'none') {
                        targetElement.style.display = 'block';
                        this.textContent = '▲';
                      } else {
                        targetElement.style.display = 'none';
                        this.textContent = '▼';
                      }
                    });
                  });
                  
                  // 点击整个月份标题也可以展开/折叠
                  document.querySelectorAll('.toggle-icon').forEach(icon => {
                    const parentDiv = icon.closest('div[style*="cursor: pointer"]');
                    if (parentDiv) {
                      parentDiv.addEventListener('click', function() {
                        const icon = this.querySelector('.toggle-icon');
                        icon.click();
                      });
                    }
                  });
                }, 100);
                
                issuesHtml += `</div>`;
              } else {
                issuesHtml += `<p style="color: #666;">未找到具体的停牌日期记录</p>`;
              }
            } else if (issues.length > 0) {
              // 如果没有详细的停牌日期列表，则显示通用问题信息
              issuesHtml += `<ul style="margin: 0; padding-left: 20px;">`;
              issues.forEach(issue => {
                issuesHtml += `<li style="margin-bottom: 8px; line-height: 1.4; color: #666;">${issue}</li>`;
              });
              issuesHtml += `</ul>`;
            } else {
              issuesHtml += `<p style="color: #666;">未找到详细的停牌日信息</p>`;
            }
            
            issuesHtml += `</div>`;
          } else {
            // 其他检查项按类型分组显示
            // 定义问题类型分组规则
            const issueTypePatterns = {
              '缺失数据': [/缺失/, /不存在/, /空值/],
              '格式错误': [/格式/, /类型/, /字符/, /无效/],
              '逻辑错误': [/逻辑/, /矛盾/, /不一致/, /冲突/],
              '数值异常': [/数值/, /异常/, /超出/, /范围/]
            };
            
            // 分组问题
            const groupedIssues = {
              '其他问题': []
            };
            
            issues.forEach(issue => {
              let assigned = false;
              // 尝试匹配已知类型
              Object.entries(issueTypePatterns).forEach(([type, patterns]) => {
                if (!groupedIssues[type]) {
                  groupedIssues[type] = [];
                }
                
                for (const pattern of patterns) {
                  if (pattern.test(issue)) {
                    groupedIssues[type].push(issue);
                    assigned = true;
                    break;
                  }
                }
              });
              
              // 未匹配到类型的问题放入其他类别
              if (!assigned) {
                groupedIssues['其他问题'].push(issue);
              }
            });
            
            issuesHtml += `<div style="margin-top: 16px;">`;
            
            // 遍历所有分组显示问题
            let hasIssues = false;
            Object.entries(groupedIssues).forEach(([type, typeIssues]) => {
              if (typeIssues.length > 0) {
                hasIssues = true;
                issuesHtml += `<div style="margin-bottom: 16px;">
                  <h4 style="margin-top: 0; margin-bottom: 8px; color: #333; font-size: 14px;">${type} (${typeIssues.length}个):</h4>
                  <ul style="margin: 0; padding-left: 20px;">`;
                typeIssues.forEach(issue => {
                  issuesHtml += `<li style="margin-bottom: 6px; line-height: 1.4; color: #666;">${issue}</li>`;
                });
                issuesHtml += `</ul>
                </div>`;
              }
            });
            
            // 如果没有问题
            if (!hasIssues) {
              issuesHtml += `<p style="color: #666;">未发现具体问题</p>`;
            }
            
            issuesHtml += `</div>`;
          }
          
          content.innerHTML = issuesHtml;
          
          // 显示模态框
          modal.style.display = 'flex';
        });
      });
      
      // 添加到表格顶部
      resultsTable.insertBefore(row, resultsTable.firstChild);
    }
  }

  // 异常值处理
  function initOutlierHandling() {
    const detectBtn = document.getElementById('detectOutliersBtn');
    const handleBtn = document.getElementById('handleOutliersBtn');
    const progressEl = document.getElementById('outlierProgress');
    const progressBar = document.getElementById('outlierProgressBar');
    const progressText = document.getElementById('outlierProgressText');
    
    detectBtn.addEventListener('click', () => {
      // 显示进度条
      progressEl.style.display = 'block';
      progressBar.style.width = '0%';
      
      // 模拟异步检测
      setTimeout(() => {
        animateProgress(progressBar, progressText, 100, '异常值检测中');
        
        // 更新统计数据
        setTimeout(() => {
          const detected = 89;
          const handled = 45;
          const remaining = 44;
          
          document.getElementById('outlierDetected').textContent = detected;
          document.getElementById('outlierHandled').textContent = handled;
          document.getElementById('outlierRemaining').textContent = remaining;
          
          toast(`异常值检测完成：共检测到${detected}个异常值，其中${handled}个已处理，${remaining}个待处理`);
        }, 3000);
      }, 500);
    });
    
    handleBtn.addEventListener('click', () => {
      const remaining = parseInt(document.getElementById('outlierRemaining').textContent) || 0;
      if (remaining === 0) {
        toast('没有需要处理的异常值');
        return;
      }
      
      // 显示进度条
      progressEl.style.display = 'block';
      progressBar.style.width = '0%';
      
      // 模拟异步处理
      setTimeout(() => {
        animateProgress(progressBar, progressText, 100, '异常值处理中');
        
        // 更新统计数据
        setTimeout(() => {
          const detected = parseInt(document.getElementById('outlierDetected').textContent) || 0;
          const handled = detected; // 全部处理完成
          const remaining = 0;
          
          document.getElementById('outlierHandled').textContent = handled;
          document.getElementById('outlierRemaining').textContent = remaining;
          
          toast(`异常值处理完成：已处理所有${handled}个异常值`);
        }, 3000);
      }, 500);
    });
  }

  function buildUrl(){
    return `${API_BASE}/api/stocks/status`;
  }

  async function reload(){
    try {
      const res = await fetch(buildUrl());
      const data = await res.json();
      
      // 初始化统计数据为模拟值
      setTimeout(() => {
        // 完整性检查
        document.getElementById('integrityTotal').textContent = '0';
        document.getElementById('integrityMissing').textContent = '0';
        document.getElementById('integrityComplete').textContent = '0%';
        
        // 格式标准化
        document.getElementById('formatTotal').textContent = '0';
        document.getElementById('formatStandardized').textContent = '0';
        document.getElementById('formatNonStandard').textContent = '0';
        
        // 异常值处理
        document.getElementById('outlierDetected').textContent = '0';
        document.getElementById('outlierHandled').textContent = '0';
        document.getElementById('outlierRemaining').textContent = '0';
      }, 100);
    } catch(e) {
      console.error('加载状态数据失败:', e);
    }
  }

  // 初始化各功能
  initTabs();
  initIntegrityCheck();
  initFormatStandardization();
  initOutlierHandling();
  
  const statusTab = document.querySelector('.sidebar .tab[data-target="status"]');
  if (statusTab) statusTab.addEventListener('click', reload);
  reload();
})();

// 导出（图表PNG + 表格CSV）
(function initExport(){
  const btn = qs('#exportBtn');
  if (!btn) return;
  btn.addEventListener('click', () => {
    // 检查是否在分析页面且有热力图
    const analysisSection = document.getElementById('analysis');
    const heatmapContainer = document.getElementById('heatmapContainer');
    
    if (analysisSection && !analysisSection.style.display && heatmapContainer && window.exportHeatmap) {
      // 在热力图页面，调用热力图导出
      window.exportHeatmap();
    } else {
      // 常规导出逻辑
      try {
        const chartEl = qs('#chart');
        if (chartEl) {
          const chart = echarts.getInstanceByDom(chartEl);
          if (chart) {
            const url = chart.getDataURL({ type: 'png', pixelRatio: 2, backgroundColor: '#fff' });
            const a = document.createElement('a'); a.href = url; a.download = 'chart.png'; a.click();
          }
        }
      } catch {}
      
      try {
        const queryTable = qs('#queryTable');
        if (queryTable) {
          const rows = [['日期','开盘','收盘','最高','最低','成交量']].concat(Array.from(queryTable.querySelectorAll('tr')).map(tr => Array.from(tr.children).map(td => td.textContent)));
          const csv = rows.map(r => r.map(v => '"'+String(v).replace(/"/g,'\"')+'"').join(',')).join('\n');
          const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' });
          const a2 = document.createElement('a'); a2.href = URL.createObjectURL(blob); a2.download = 'query.csv'; a2.click();
          toast('已导出图表与表格');
        }
      } catch {}
    }
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

