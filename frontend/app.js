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

// 申万数据功能实现
(function initSwData() {
  console.log('初始化申万数据功能');
  
  // 选项卡切换逻辑
  const swTabs = document.querySelectorAll('#sw_data .tab-btn');
  swTabs.forEach(tab => {
    tab.addEventListener('click', () => {
      const tabId = tab.getAttribute('data-tab');
      
      // 移除所有选项卡和内容的active类
      swTabs.forEach(t => t.classList.remove('active'));
      document.querySelectorAll('#sw_data .tab-content').forEach(content => {
        content.classList.remove('active');
      });
      
      // 添加当前选项卡和内容的active类
      tab.classList.add('active');
      const contentEl = document.getElementById(tabId);
      if (contentEl) {
        contentEl.classList.add('active');
        console.log(`切换到选项卡: ${tabId}`);
      } else {
        console.error(`未找到选项卡内容: ${tabId}`);
      }
    });
  });
  
  // 生成申万分类数据
  qs('#generateSwClassification')?.addEventListener('click', async () => {
    try {
      const resultEl = qs('#swClassificationResult');
      resultEl.textContent = '正在生成分类数据...';
      
      const response = await fetch(`${API_BASE}/api/stocks/sw/generate`, {
        method: 'POST'
      });
      
      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }
      
      const data = await response.json();
      resultEl.textContent = `生成完成！${data.message || '已成功生成申万分类数据'}`;
    } catch (error) {
      console.error('生成分类数据失败:', error);
      qs('#swClassificationResult').textContent = `生成失败: ${error.message}`;
    }
  });
  
  // 查询申万分类数据
  qs('#querySwClassification')?.addEventListener('click', async () => {
    try {
      const resultEl = qs('#swClassificationResult');
      resultEl.textContent = '正在查询分类数据...';
      
      const response = await fetch(`${API_BASE}/api/stocks/sw/classification`);
      
      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }
      
      const responseData = await response.json();
      // 处理数据并渲染树形结构
      if (responseData && Array.isArray(responseData.data)) {
        console.log('查询到分类数据:', responseData.data.length, '条');
        // 创建树形结构
        const treeData = buildSwClassificationTree(responseData.data);
          
        // 提取三级行业编号代码
        const thirdLevelIndustries = extractThirdLevelIndustries(treeData);
        console.log('提取的三级行业编号:', thirdLevelIndustries);
          
          // 渲染树形结构
          resultEl.innerHTML = renderSwClassificationTree(treeData);
          
          // 添加树形节点的展开/折叠功能
          addTreeToggleFunctionality();
          
          // 显示三级行业编号并添加调用API功能
          showThirdLevelIndustries(thirdLevelIndustries);
      } else {
        resultEl.textContent = JSON.stringify(responseData, null, 2);
      }
    } catch (error) {
      console.error('查询分类数据失败:', error);
      qs('#swClassificationResult').textContent = `查询失败: ${error.message}`;
    }
  });

  // 提取三级行业编号代码
  function extractThirdLevelIndustries(treeData) {
    const thirdLevelCodes = [];
    
    // 遍历树形结构，找出三级行业
    function traverse(node, level = 0) {
      // 申万分类通常为三级结构，这里假设没有子节点的节点是三级行业
      // 或者可以通过行业代码格式来判断
      const isThirdLevel = !node.children || node.children.length === 0;
      
      if (isThirdLevel && level === 2) {
        thirdLevelCodes.push(node.id);
      }
      
      // 继续遍历子节点
      if (node.children && node.children.length > 0) {
        node.children.forEach(child => traverse(child, level + 1));
      }
    }
    
    // 从根节点开始遍历
    treeData.forEach(node => traverse(node));
    return thirdLevelCodes;
  }
  
  // 显示三级行业编号
  function showThirdLevelIndustries(industryCodes) {
    const resultEl = qs('#swClassificationResult');
    
    // 创建一个容器来显示三级行业编号
    const thirdLevelContainer = document.createElement('div');
    thirdLevelContainer.className = 'third-level-industries';
    thirdLevelContainer.style.marginTop = '20px';
    thirdLevelContainer.style.padding = '15px';
    thirdLevelContainer.style.backgroundColor = '#f5f5f5';
    thirdLevelContainer.style.borderRadius = '4px';
    
    // 添加标题
    const titleEl = document.createElement('h3');
    titleEl.textContent = `三级行业编号代码（共${industryCodes.length}个）：`;
    titleEl.style.marginTop = '0';
    titleEl.style.color = '#333';
    thirdLevelContainer.appendChild(titleEl);
    
    // 添加编号列表
    const codesListEl = document.createElement('div');
    codesListEl.style.wordWrap = 'break-word';
    codesListEl.style.lineHeight = '1.5';
    codesListEl.textContent = industryCodes.join(', ');
    thirdLevelContainer.appendChild(codesListEl);
    
    // 添加调用API按钮
    const callApiBtn = document.createElement('button');
    callApiBtn.className = 'btn primary';
    callApiBtn.textContent = '开始调用API获取详细信息';
    callApiBtn.style.marginTop = '15px';
    callApiBtn.onclick = async () => {
      // 禁用按钮防止重复点击
      callApiBtn.disabled = true;
      callApiBtn.textContent = '调用中...';
      
      // 创建结果显示区域
      const resultsContainer = document.createElement('div');
      resultsContainer.className = 'api-results';
      resultsContainer.style.marginTop = '15px';
      resultsContainer.style.padding = '10px';
      resultsContainer.style.backgroundColor = '#fff';
      resultsContainer.style.borderRadius = '4px';
      resultsContainer.style.border = '1px solid #ddd';
      
      // 添加进度条容器
      const progressContainer = createProgressBar();
      resultsContainer.appendChild(progressContainer);
      
      // 添加结果标题
      const resultsTitle = document.createElement('h4');
      resultsTitle.textContent = 'API调用结果：';
      resultsTitle.style.marginTop = '15px';
      resultsContainer.appendChild(resultsTitle);
      
      // 添加结果列表容器
      const resultsList = document.createElement('div');
      resultsList.style.maxHeight = '300px';
      resultsList.style.overflowY = 'auto';
      resultsContainer.appendChild(resultsList);
      
      // 添加到容器
      thirdLevelContainer.appendChild(resultsContainer);
      
      try {
        // 逐个调用API
        await callIndustryCodeAPI(industryCodes, resultsList, progressContainer);
        
        // 调用完成后更新按钮状态
        callApiBtn.textContent = 'API调用完成';
        callApiBtn.style.backgroundColor = '#4CAF50';
      } catch (error) {
        console.error('API调用失败:', error);
        callApiBtn.textContent = '调用失败，请重试';
        callApiBtn.style.backgroundColor = '#f44336';
        
        // 显示错误信息
        const errorEl = document.createElement('div');
        errorEl.style.color = 'red';
        errorEl.textContent = `错误: ${error.message}`;
        resultsList.appendChild(errorEl);
      } finally {
        // 允许再次点击
        setTimeout(() => {
          callApiBtn.disabled = false;
          callApiBtn.textContent = '重新调用API';
          callApiBtn.style.backgroundColor = '';
        }, 1000);
      }
    };
    thirdLevelContainer.appendChild(callApiBtn);
    
    // 添加到结果区域
    resultEl.appendChild(thirdLevelContainer);
  }
  
  // 创建进度条
  function createProgressBar() {
    const container = document.createElement('div');
    container.className = 'progress-container';
    container.style.width = '100%';
    container.style.backgroundColor = '#f0f0f0';
    container.style.borderRadius = '4px';
    container.style.overflow = 'hidden';
    
    const progressBar = document.createElement('div');
    progressBar.className = 'progress-bar';
    progressBar.style.width = '0%';
    progressBar.style.height = '20px';
    progressBar.style.backgroundColor = '#2196F3';
    progressBar.style.transition = 'width 0.3s ease';
    
    const progressText = document.createElement('div');
    progressText.className = 'progress-text';
    progressText.style.position = 'absolute';
    progressText.style.width = '100%';
    progressText.style.textAlign = 'center';
    progressText.style.lineHeight = '20px';
    progressText.style.fontSize = '12px';
    progressText.style.color = '#333';
    progressText.textContent = '0%';
    
    container.style.position = 'relative';
    container.appendChild(progressBar);
    container.appendChild(progressText);
    
    // 保存引用以便后续更新
    container.progressBar = progressBar;
    container.progressText = progressText;
    
    return container;
  }
  
  // 更新进度条
  function updateProgressBar(progressContainer, current, total) {
    const percent = Math.round((current / total) * 100);
    progressContainer.progressBar.style.width = `${percent}%`;
    progressContainer.progressText.textContent = `${percent}% (${current}/${total})`;
  }
  
  // 逐个调用行业代码API
  async function callIndustryCodeAPI(industryCodes, resultsList, progressContainer) {
    const total = industryCodes.length;
    const results = [];
    
    // 逐个处理每个行业代码
    for (let i = 0; i < total; i++) {
      const code = industryCodes[i];
      
      try {
        // 创建当前代码的结果元素
        const codeResultEl = document.createElement('div');
        codeResultEl.style.marginBottom = '8px';
        codeResultEl.style.padding = '8px';
        codeResultEl.style.backgroundColor = '#f9f9f9';
        codeResultEl.style.borderRadius = '4px';
        codeResultEl.innerHTML = `<strong>处理行业代码: ${code}</strong> - 处理中...`;
        resultsList.appendChild(codeResultEl);
        
        // 调用API
        const response = await fetch(`${API_BASE}/api/stocks/sw/third_level_industry_codes?code=${code}`);
        
        if (!response.ok) {
          throw new Error(`API调用失败，状态码: ${response.status}`);
        }
        
        const data = await response.json();
        results.push({ code, data });
        
        // 更新显示结果
        codeResultEl.innerHTML = `<strong>行业代码: ${code}</strong> - 处理完成`;
        codeResultEl.style.backgroundColor = '#e8f5e8';
        
        // 添加详细结果
        const dataEl = document.createElement('pre');
        dataEl.style.marginTop = '5px';
        dataEl.style.padding = '5px';
        dataEl.style.backgroundColor = '#fff';
        dataEl.style.borderRadius = '3px';
        dataEl.style.fontSize = '12px';
        dataEl.style.overflow = 'auto';
        dataEl.textContent = JSON.stringify(data, null, 2);
        codeResultEl.appendChild(dataEl);
        
      } catch (error) {
        console.error(`处理代码 ${code} 失败:`, error);
        
        // 更新错误显示
        const errorEl = document.createElement('div');
        errorEl.style.marginBottom = '8px';
        errorEl.style.padding = '8px';
        errorEl.style.backgroundColor = '#ffebee';
        errorEl.style.borderRadius = '4px';
        errorEl.innerHTML = `<strong>行业代码: ${code}</strong> - 处理失败: ${error.message}`;
        resultsList.appendChild(errorEl);
      } finally {
                // 更新进度条
                updateProgressBar(progressContainer, i + 1, total);
                
                // 可选：添加短暂延迟以避免请求过快
                await new Promise(resolve => setTimeout(resolve, 100));
              }
    }
    
    return results;
  }
  
  // 构建申万分类树结构
  function buildSwClassificationTree(data) {
    // 健壮性检查
    if (!Array.isArray(data) || data.length === 0) {
      console.warn('无效的分类数据:', data);
      return [];
    }
    
    const root = [];
    const nodeMap = {};
    
    // 首先创建所有节点的映射
    data.forEach(item => {
      const node = {
        id: item.industry_code,
        name: item.industry_name,
        children: [],
        data: item // 保存原始数据
      };
      nodeMap[node.id] = node;
      
      // 如果没有上级行业或上级行业为空字符串，认为是一级节点
      if (!item.parent_industry || item.parent_industry === '') {
        root.push(node);
      }
    });
    
    // 然后构建父子关系
    data.forEach(item => {
      const parentName = item.parent_industry;
      if (parentName && parentName !== '') {
        // 查找父节点（通过行业名称匹配）
        const parentNode = Object.values(nodeMap).find(node => node.name === parentName);
        const currentNode = nodeMap[item.industry_code];
        
        if (parentNode && currentNode) {
          parentNode.children.push(currentNode);
        }
      }
    });
    
    return root;
  }
  
  // 渲染申万分类树结构
  function renderSwClassificationTree(treeData) {
    const treeContainer = document.createElement('div');
    treeContainer.className = 'sw-classification-tree';
    
    // 处理空数据情况
    if (!treeData || treeData.length === 0) {
      treeContainer.innerHTML = '<div class="tree-empty">暂无分类数据</div>';
      return treeContainer.outerHTML;
    }
    
    function renderNode(node, level = 0) {
      const nodeEl = document.createElement('div');
      nodeEl.className = `tree-node level-${level}`;
      nodeEl.style.marginLeft = `${level * 24}px`;
      
      // 创建节点内容
      const contentEl = document.createElement('div');
      contentEl.className = 'tree-node-content';
      
      // 如果有子节点，添加展开/折叠按钮
      if (node.children && node.children.length > 0) {
        const toggleBtn = document.createElement('span');
        toggleBtn.className = 'tree-toggle-btn';
        toggleBtn.innerHTML = '▼';
        toggleBtn.setAttribute('data-level', level);
        contentEl.appendChild(toggleBtn);
      } else {
        // 没有子节点时添加占位符
        const placeholder = document.createElement('span');
        placeholder.style.display = 'inline-block';
        placeholder.style.width = '20px';
        contentEl.appendChild(placeholder);
      }
      
      // 添加节点文本和统计信息
      const textEl = document.createElement('span');
      textEl.className = 'tree-node-text';
      
      // 构建显示文本，包含行业名称和所有统计信息
      let nodeText = node.name;
      
      // 行业代码
      nodeText += ` <span class="node-metric">${node.id}</span>`;
      
      // 计算下一级分类个数
      const subCategoryCount = node.children ? node.children.length : 0;
      if (subCategoryCount > 0) {
        nodeText += ` <span class="sub-category-count">(子分类:${subCategoryCount})</span>`;
      }
      
      // 统计信息
      const stats = [];
      
      // 显示成份个数
      if (node.data.component_count) {
        stats.push(`成份: ${node.data.component_count}`);
      }
      
      // 显示TTM市盈率
      const pe = node.data.ttm_pe;
      if (pe) {
        const peClass = parseFloat(pe) > 0 && parseFloat(pe) < 30 ? 'positive' : 'negative';
        stats.push(`市盈率: <span class="node-metric ${peClass}">${pe}</span>`);
      }
      
      // 显示市净率
      if (node.data.pb_ratio) {
        stats.push(`市净率: ${node.data.pb_ratio}`);
      }
      
      // 显示股息率
      const dividend = node.data.static_dividend_yield;
      if (dividend) {
        const dividendClass = parseFloat(dividend) > 2 ? 'positive' : '';
        stats.push(`股息率: <span class="node-metric ${dividendClass}">${dividend}%</span>`);
      }
      
      // 将统计信息添加到文本中
      if (stats.length > 0) {
        nodeText += ` ${stats.join(' ')}`;
      }
      
      textEl.innerHTML = nodeText;
      contentEl.appendChild(textEl);
      nodeEl.appendChild(contentEl);
      // 移除单独的infoEl元素
      
      // 渲染子节点
      if (node.children && node.children.length > 0) {
        const childrenEl = document.createElement('div');
        childrenEl.className = 'tree-children';
        
        // 对子节点进行排序
        const sortedChildren = [...node.children].sort((a, b) => {
          // 优先按成份个数排序
          const countA = a.data.component_count || 0;
          const countB = b.data.component_count || 0;
          return countB - countA;
        });
        
        sortedChildren.forEach(child => {
          childrenEl.appendChild(renderNode(child, level + 1));
        });
        
        nodeEl.appendChild(childrenEl);
      }
      
      return nodeEl;
    }
    
    // 渲染所有根节点，并按成份个数排序
    const sortedRootNodes = [...treeData].sort((a, b) => {
      const countA = a.data.component_count || 0;
      const countB = b.data.component_count || 0;
      return countB - countA;
    });
    
    sortedRootNodes.forEach(node => {
      treeContainer.appendChild(renderNode(node));
    });
    
    return treeContainer.outerHTML;
  }
  
  // 添加树形结构展开/折叠功能
  function addTreeToggleFunctionality() {
    const toggleButtons = document.querySelectorAll('.tree-toggle-btn');
    
    // 添加展开/折叠按钮事件
    toggleButtons.forEach(btn => {
      btn.addEventListener('click', function(event) {
        event.stopPropagation(); // 防止事件冒泡
        
        const nodeEl = this.closest('.tree-node');
        const childrenEl = nodeEl.querySelector('.tree-children');
        
        if (childrenEl) {
          const isExpanded = childrenEl.style.display !== 'none';
          
          if (isExpanded) {
            // 折叠节点
            childrenEl.style.display = 'none';
            this.innerHTML = '▶';
            this.setAttribute('aria-expanded', 'false');
          } else {
            // 展开节点
            childrenEl.style.display = 'block';
            this.innerHTML = '▼';
            this.setAttribute('aria-expanded', 'true');
          }
          
          // 添加动画效果
          childrenEl.style.transition = 'all 0.3s ease';
        }
      });
    });
    
    // 添加节点点击事件，可点击节点文本展开/折叠
    document.querySelectorAll('.tree-node-content').forEach(content => {
      content.addEventListener('click', function(event) {
        // 如果点击的是展开/折叠按钮，不处理
        if (event.target.closest('.tree-toggle-btn')) {
          return;
        }
        
        const toggleBtn = this.querySelector('.tree-toggle-btn');
        if (toggleBtn) {
          toggleBtn.click(); // 触发展开/折叠按钮的点击事件
        }
      });
    });
    
    // 添加键盘导航支持
    toggleButtons.forEach(btn => {
      btn.setAttribute('tabindex', '0');
      btn.setAttribute('role', 'button');
      btn.setAttribute('aria-label', '展开/折叠节点');
      
      btn.addEventListener('keydown', function(event) {
        if (event.key === 'Enter' || event.key === ' ') {
          event.preventDefault();
          this.click();
        }
      });
    });
  }
  

  

  
  // 模拟进度更新
  function simulateProgress(progressEl, statusEl, type) {
    let progress = 0;
    const interval = setInterval(() => {
      progress += Math.random() * 10;
      if (progress >= 100) {
        progress = 100;
        clearInterval(interval);
        statusEl.textContent = `${type}完成`;
      } else {
        statusEl.textContent = `${type}中... ${Math.round(progress)}%`;
      }
      progressEl.style.width = `${progress}%`;
    }, 500);
  }
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
    const titleMap = { query: '查询', analysis: '分析', profile: '个人中心', status: '数据状态', update: '数据更新', 'task-monitor': '任务状态', tasks: '计划任务', config: '参数配置', sw_data: '申万数据' };
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
  const autoRefreshToggle = qs('#autoRefreshToggle');
  const statsSection = qs('#taskMonitorStats');
  const recentTasksTable = qs('#recentTasksTable');
  const schedulesTable = qs('#schedulesTable');
  
  // 自动刷新相关变量
  let autoRefreshTimer = null;
  const autoRefreshInterval = 5000; // 5秒刷新一次
  
  // 加载所有数据
  async function loadAllData() {
    await Promise.all([
      loadStatistics(),
      loadRecentTasks(),
      loadSchedules()
    ]);
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
  
  // 添加自动刷新切换事件监听
  if (autoRefreshToggle) {
    autoRefreshToggle.addEventListener('change', function() {
      if (this.checked) {
        // 开启自动刷新
        startAutoRefresh();
      } else {
        // 关闭自动刷新
        stopAutoRefresh();
      }
    });
  }
  
  // 开始自动刷新
  function startAutoRefresh() {
    // 先清除可能存在的定时器
    stopAutoRefresh();
    
    // 设置新的定时器
    autoRefreshTimer = setInterval(() => {
      loadAllData();
    }, autoRefreshInterval);
  }
  
  // 停止自动刷新
  function stopAutoRefresh() {
    if (autoRefreshTimer) {
      clearInterval(autoRefreshTimer);
      autoRefreshTimer = null;
    }
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
  
  // 页面卸载时清理定时器
  window.addEventListener('unload', stopAutoRefresh);

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

  // CSV异常值处理
  function initOutlierHandling() {
    const detectBtn = document.getElementById('detectOutliersBtn');
    const handleBtn = document.getElementById('handleOutliersBtn');
    const progressEl = document.getElementById('outlierProgress');
    const progressBar = document.getElementById('outlierProgressBar');
    const progressText = document.getElementById('outlierProgressText');
    
    // 逐条检测股票函数
      async function detectStocksOneByOne(stockList, progressBar, progressText) {
        let processedCount = 0;
        const totalCount = stockList.length;
        let successCount = 0;
        let failedCount = 0;
        
        // 创建一个显示检测结果的区域
        const resultContainer = document.getElementById('stockListContent');
        let resultHtml = '<div style="font-size: 14px;">检测进度:<br></div>';
        resultContainer.innerHTML = resultHtml;
        
        // 遍历股票列表进行检测
        for (const stockCode of stockList) {
          try {
            processedCount++;
            
            // 更新进度信息
            const progressPercentage = Math.floor((processedCount / totalCount) * 100);
            progressBar.style.width = `${progressPercentage}%`;
            progressText.textContent = `正在检测 ${stockCode} (${processedCount}/${totalCount})`;
            
            // 调用post方法检测单个股票
            const checkResponse = await fetch(`${API_BASE}/api/stocks/integrity/csv_check`, {
              method: 'POST',
              headers: {
                'Content-Type': 'application/json'
              },
              body: JSON.stringify({ stock_code: stockCode })
            });
            
            const checkResult = await checkResponse.json();
            
            // 更新检测结果显示
            if (checkResponse.ok) {
              successCount++;
              
              // 获取检测结果
              const accuracyCheck = checkResult.accuracy_check || { status: 'unknown' };
              const logicalCheck = checkResult.logical_check || { status: 'unknown' };
              const formatCheck = checkResult.format_check || { status: 'unknown' };
              const suspensionCheck = checkResult.suspension_check || { status: 'unknown' };
              const enhancedCheck = checkResult.enhanced_check || { status: 'unknown' };
              
              // 格式化时间（只显示时分秒）
              const timestamp = checkResult.timestamp ? new Date(checkResult.timestamp).toLocaleTimeString('zh-CN', {hour: '2-digit', minute:'2-digit', second:'2-digit'}) : '未知时间';
              
              // 构建单行7列的检测结果HTML（带颜色标识和完整错误详情提示）
              // 处理每个检测结果的错误信息，包含issues字段
              const getErrorDetail = (check) => {
                if (check.status !== 'fail') return `${check.status === 'pass' ? '通过' : '未知'}`;
                
                let details = [];
                // 添加错误消息
                if (check.error_message || check.message) {
                  details.push(check.error_message || check.message);
                }
                
                // 添加issues信息（如果存在）
                if (check.issues && Array.isArray(check.issues) && check.issues.length > 0) {
                  check.issues.forEach((issue, index) => {
                    if (typeof issue === 'string') {
                      details.push(`问题${index + 1}: ${issue}`);
                    } else if (typeof issue === 'object') {
                      // 尝试提取对象中的有价值信息
                      const issueStr = Object.entries(issue)
                        .map(([key, value]) => `${key}: ${value}`)
                        .join(', ');
                      details.push(`问题${index + 1}: ${issueStr}`);
                    }
                  });
                }
                
                return details.length > 0 ? details.join('\n') : '检测失败';
              };
              
              resultHtml += `
                <div class="stock-result-item" style="border-bottom: 1px solid #eee; padding: 8px 0; display: flex; align-items: center; font-size: 12px;">
                  <div style="width: 10%; font-weight: bold;">${checkResult.stock_code}</div>
                  <div style="width: 15%;">${checkResult.stock_name}</div>
                  <div style="width: 10%; text-align: center; padding: 2px; background-color: ${accuracyCheck.status === 'pass' ? '#e8f5e9' : '#ffebee'}; color: ${accuracyCheck.status === 'pass' ? '#2e7d32' : '#c62828'}; border-radius: 3px; cursor: pointer;" title="准确性检测${getErrorDetail(accuracyCheck)}">
                    ${accuracyCheck.status === 'pass' ? '✓' : accuracyCheck.status === 'fail' ? '✗' : '?'}
                  </div>
                  <div style="width: 10%; text-align: center; padding: 2px; background-color: ${logicalCheck.status === 'pass' ? '#e8f5e9' : '#ffebee'}; color: ${logicalCheck.status === 'pass' ? '#2e7d32' : '#c62828'}; border-radius: 3px; cursor: pointer;" title="逻辑性检测${getErrorDetail(logicalCheck)}">
                    ${logicalCheck.status === 'pass' ? '✓' : logicalCheck.status === 'fail' ? '✗' : '?'}
                  </div>
                  <div style="width: 10%; text-align: center; padding: 2px; background-color: ${formatCheck.status === 'pass' ? '#e8f5e9' : '#ffebee'}; color: ${formatCheck.status === 'pass' ? '#2e7d32' : '#c62828'}; border-radius: 3px; cursor: pointer;" title="格式检测${getErrorDetail(formatCheck)}">
                    ${formatCheck.status === 'pass' ? '✓' : formatCheck.status === 'fail' ? '✗' : '?'}
                  </div>
                  <div style="width: 10%; text-align: center; padding: 2px; background-color: ${suspensionCheck.status === 'pass' ? '#e8f5e9' : '#ffebee'}; color: ${suspensionCheck.status === 'pass' ? '#2e7d32' : '#c62828'}; border-radius: 3px; cursor: pointer;" title="停牌检测${getErrorDetail(suspensionCheck)}">
                    ${suspensionCheck.status === 'pass' ? '✓' : suspensionCheck.status === 'fail' ? '✗' : '?'}
                  </div>
                  <div style="width: 15%; text-align: center; padding: 2px; background-color: ${enhancedCheck.status === 'pass' ? '#e8f5e9' : '#ffebee'}; color: ${enhancedCheck.status === 'pass' ? '#2e7d32' : '#c62828'}; border-radius: 3px; cursor: pointer;" title="增强校验${getErrorDetail(enhancedCheck)}">
                    ${enhancedCheck.status === 'pass' ? '✓' : enhancedCheck.status === 'fail' ? '✗' : '?'}
                  </div>
                  <div style="width: 10%; color: #666;">${timestamp}</div>
                </div>
              `;
            } else {
              failedCount++;
              // 失败时也保持7列格式，但只显示关键信息
              resultHtml += `
                <div class="stock-result-item" style="border-bottom: 1px solid #eee; padding: 8px 0; display: flex; align-items: center; font-size: 12px;">
                  <div style="width: 10%; font-weight: bold; color: #f44336;">${stockCode}</div>
                  <div style="width: 15%; color: #f44336;">检测失败</div>
                  <div style="width: 10%; text-align: center; color: #f44336;">✗</div>
                  <div style="width: 10%; text-align: center; color: #f44336;">✗</div>
                  <div style="width: 10%; text-align: center; color: #f44336;">✗</div>
                  <div style="width: 10%; text-align: center; color: #f44336;">✗</div>
                  <div style="width: 15%; text-align: center; color: #f44336;">✗</div>
                  <div style="width: 10%; color: #666; font-size: 11px;">${checkResult.error || '未知错误'}</div>
                </div>
              `;
            }
            
            resultContainer.innerHTML = resultHtml;
            resultContainer.scrollTop = resultContainer.scrollHeight; // 自动滚动到底部
            
            // 更新剩余数量
            document.getElementById('outlierRemaining').textContent = totalCount - processedCount;
            document.getElementById('outlierHandled').textContent = processedCount;
            
            // 短暂延迟，避免请求过于密集
            await new Promise(resolve => setTimeout(resolve, 100));
            
          } catch (error) {
            failedCount++;
            resultHtml += `<div style="color: #ff9800; font-size: 12px;">! ${stockCode}: 请求错误 - ${error.message}</div>`;
            resultContainer.innerHTML = resultHtml;
            resultContainer.scrollTop = resultContainer.scrollHeight;
          }
        }
        
        // 检测完成后更新UI
        progressText.textContent = '检测完成';
        toast(`检测完成: 成功 ${successCount} 只, 失败 ${failedCount} 只`);
      }
      
      detectBtn.addEventListener('click', async () => {
        try {
          // 清空并隐藏之前的股票列表
          const stockListContainer = document.getElementById('stockListContainer');
          const stockListContent = document.getElementById('stockListContent');
          stockListContent.textContent = '';
          stockListContainer.style.display = 'none';
          
          // 显示进度条
          progressEl.style.display = 'block';
          progressBar.style.width = '0%';
          progressText.textContent = '正在获取待检测股票列表...';
        
        // 调用POST方法获取待检测的股票代码（直接发送空请求体）
        // 调用GET方法获取待检测的股票代码
        const postResponse = await fetch(`${API_BASE}/api/stocks/integrity/csv_check`, {
          headers: {
            'Content-Type': 'application/json'
          }
        });
        
        if (!postResponse.ok) {
          throw new Error(`获取待检测股票失败: ${postResponse.status}`);
        }
        
        // 模拟进度更新
        progressBar.style.width = '50%';
        
        const result = await postResponse.json();
        
        // 如果API返回了待检测的股票列表
        if (result.pending_stocks && result.pending_stocks.length > 0) {
          const pendingStocks = result.pending_stocks;
          
          // 更新统计数据
          const detected = pendingStocks.length;
          const handled = 0;
          const remaining = detected;
          
          document.getElementById('outlierDetected').textContent = detected;
          document.getElementById('outlierHandled').textContent = handled;
          document.getElementById('outlierRemaining').textContent = remaining;
          
          // 显示待检测的股票代码
          console.log('待检测的股票代码:', pendingStocks);
          toast(`已获取待检测股票列表：共${detected}只股票需要检测`);
          
          // 显示股票列表容器
          const stockListContainer = document.getElementById('stockListContainer');
          stockListContainer.style.display = 'block';
          
          // 滚动到列表区域
          stockListContainer.scrollIntoView({ behavior: 'smooth', block: 'start' });
          
          // 开始逐条检测每个股票
          await detectStocksOneByOne(pendingStocks, progressBar, progressText);
        } else {
          // 如果没有待检测的股票
          progressBar.style.width = '100%';
          toast('当前没有需要检测的股票');
        }
      } catch (error) {
        console.error('检测异常值失败:', error);
        toast(`检测失败: ${error.message}`);
        progressEl.style.display = 'none';
      }
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
        animateProgress(progressBar, progressText, 100, 'CSV异常值处理中');
        
        // 更新统计数据
        setTimeout(() => {
          const detected = parseInt(document.getElementById('outlierDetected').textContent) || 0;
          const handled = detected; // 全部处理完成
          const remaining = 0;
          
          document.getElementById('outlierHandled').textContent = handled;
          document.getElementById('outlierRemaining').textContent = remaining;
          
          toast(`CSV异常值处理完成：已处理所有${handled}个异常值`);
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
        
        // CSV异常值处理
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
  
  const fullBtn = document.getElementById('updateFull');
  const fullStatusEl = document.getElementById('updateFullStatus');
  const pauseBtn = document.getElementById('updatePause');
  const stopBtn = document.getElementById('updateStop');

  const queueStartBtn = document.getElementById('queueUpdateStart');
  const queueToggleBtn = document.getElementById('queueUpdateToggle');
  const queueStatusEl = document.getElementById('queueUpdateStatus');
   let paused = false;
   let queuePaused = false;

  // 创建连接池状态显示元素
  const poolStatusEl = document.createElement('div');
  poolStatusEl.className = 'connection-pool-status';
  poolStatusEl.style.marginTop = '10px';
  poolStatusEl.style.padding = '8px';
  poolStatusEl.style.backgroundColor = '#f0f0f0';
  poolStatusEl.style.borderRadius = '4px';
  paramsEl.parentNode.appendChild(poolStatusEl);

  fetch(`${API_BASE}/api/stocks/update/status`)
    .then(r => r.json())
    .then(d => {
      const ctrl = d.controller || {};
      const qctrl = d.queue_controller || {};
      paused = !!ctrl.paused;
      queuePaused = !!qctrl.paused;
      qdbEl.textContent = d.questdb?.connected ? '已连接' : '未连接';
      // 更新为连接状态，不再显示连接参数
      paramsEl.textContent = d.questdb?.connected ? '连接状态正常' : (d.questdb?.error || '连接失败');
      basicEl.textContent = d.stock_basic_count || 0;
      
      // 显示连接池状态
      updatePoolStatus(d.connection_pool);
      
      // 初始化按钮文案
      if (pauseBtn) pauseBtn.textContent = paused ? '继续' : '暂停';
      if (queueToggleBtn) queueToggleBtn.textContent = queuePaused ? '继续' : '暂停';
    })
    .catch(err => {
      qdbEl.textContent = '异常';
      paramsEl.textContent = String(err);
      poolStatusEl.textContent = '连接池状态：无法获取';
    });
    
  function updatePoolStatus(poolStats) {
    if (!poolStats) {
      poolStatusEl.textContent = '连接池状态：暂无数据';
      return;
    }
    
    poolStatusEl.innerHTML = `连接池状态：
      <br>活跃连接数：${poolStats.active_connections || 0}
      <br>空闲连接数：${poolStats.idle_connections || 0}
      <br>总连接数：${poolStats.total_connections || 0}
      <br>最大连接数：${poolStats.max_connections || 0}`;
  }

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
        fullStatusEl.textContent = `started=${d.started} at ${d.started_at || ''}`;
        if (!d.started) {
          toast(d.error || '启动失败');
        }
      })
      .catch(err => {
        fullStatusEl.textContent = `失败：${err.message}`;
        toast(`全量更新失败：${err.message}`);
      });
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
      try { d = await r.json(); } catch {}      queuePaused = !!d.paused;
      if (queueToggleBtn) queueToggleBtn.textContent = queuePaused ? '继续' : '暂停';
      queueStatusEl.textContent = queuePaused ? '已暂停队列' : '已继续队列';
      toast(queuePaused ? '已暂停任务队列' : '已继续任务队列');
    } catch(e) {
      toast('操作失败');
    }
  });

  // 复权因子更新功能
  const adjustFactorBtn = document.getElementById('updateAdjustFactor');
  const adjustFactorToggleBtn = document.getElementById('updateAdjustFactorToggle');
  const adjustFactorStatusEl = document.getElementById('updateAdjustFactorStatus');
  let adjustFactorPaused = false;

  // 更新复权因子按钮事件
  adjustFactorBtn?.addEventListener('click', async () => {
    try {
      adjustFactorStatusEl.textContent = '更新中...';
      const r = await fetch(`${API_BASE}/api/stocks/update/adjust_factor/start`, { method: 'POST' });
      let d = {};
      try { d = await r.json(); } catch {}
      
      if (!r.ok) {
        const msg = d.error || d.detail || `HTTP ${r.status}`;
        throw new Error(msg);
      }
      
      adjustFactorStatusEl.textContent = `已启动，${d.message || '开始更新复权因子'}`;
      if (adjustFactorToggleBtn) {
        adjustFactorToggleBtn.disabled = false;
        adjustFactorToggleBtn.textContent = '暂停';
      }
      adjustFactorPaused = false;
      toast('复权因子更新已启动');
    } catch(e) {
      adjustFactorStatusEl.textContent = `失败：${e.message}`;
      toast(`复权因子更新失败：${e.message}`);
    }
  });

  // 复权因子暂停/继续按钮事件
  adjustFactorToggleBtn?.addEventListener('click', async () => {
    try {
      const url = adjustFactorPaused ? `${API_BASE}/api/stocks/update/adjust_factor/resume` : `${API_BASE}/api/stocks/update/adjust_factor/pause`;
      const r = await fetch(url, { method: 'POST' });
      let d = {};
      try { d = await r.json(); } catch {}
      
      adjustFactorPaused = !!d.paused;
      if (adjustFactorToggleBtn) {
        adjustFactorToggleBtn.textContent = adjustFactorPaused ? '继续' : '暂停';
      }
      adjustFactorStatusEl.textContent = adjustFactorPaused ? '已暂停' : '已继续';
      toast(adjustFactorPaused ? '已暂停复权因子更新' : '已继续复权因子更新');
    } catch(e) {
      toast('操作失败');
    }
  });

  // 金融数据更新功能
  const financialDataBtn = document.getElementById('updateFinancialData');
  const financialDataToggleBtn = document.getElementById('updateFinancialDataToggle');
  const financialDataStatusEl = document.getElementById('updateFinancialDataStatus');
  let financialDataPaused = false;

  // 更新金融数据按钮事件
  financialDataBtn?.addEventListener('click', async () => {
    try {
      financialDataStatusEl.textContent = '更新中...';
      const r = await fetch(`${API_BASE}/api/stocks/update/financial_data/start`, { method: 'POST' });
      let d = {};
      try { d = await r.json(); } catch {}
      
      if (!r.ok) {
        const msg = d.error || d.detail || `HTTP ${r.status}`;
        throw new Error(msg);
      }
      
      financialDataStatusEl.textContent = `已启动，${d.message || '开始更新金融数据'}`;
      if (financialDataToggleBtn) {
        financialDataToggleBtn.disabled = false;
        financialDataToggleBtn.textContent = '暂停';
      }
      financialDataPaused = false;
      toast('金融数据更新已启动');
    } catch(e) {
      financialDataStatusEl.textContent = `失败：${e.message}`;
      toast(`金融数据更新失败：${e.message}`);
    }
  });

  // 金融数据暂停/继续按钮事件
  financialDataToggleBtn?.addEventListener('click', async () => {
    try {
      const url = financialDataPaused ? `${API_BASE}/api/stocks/update/financial_data/resume` : `${API_BASE}/api/stocks/update/financial_data/pause`;
      const r = await fetch(url, { method: 'POST' });
      let d = {};
      try { d = await r.json(); } catch {}
      
      financialDataPaused = !!d.paused;
      if (financialDataToggleBtn) {
        financialDataToggleBtn.textContent = financialDataPaused ? '继续' : '暂停';
      }
      financialDataStatusEl.textContent = financialDataPaused ? '已暂停' : '已继续';
      toast(financialDataPaused ? '已暂停金融数据更新' : '已继续金融数据更新');
    } catch(e) {
      toast('操作失败');
    }
  });

  // 指数成份更新功能 - 简化版本，确保按钮点击能正常触发
  console.log('开始初始化指数成份更新功能');
  
  // 直接绑定按钮点击事件，不使用可选链
  document.getElementById('updateIndexComponents').addEventListener('click', async function() {
    console.log('更新指数成份按钮被直接点击');
    
    // 获取状态元素
    const statusEl = document.getElementById('updateIndexComponentsStatus');
    statusEl.textContent = '更新中...';
    
    // 直接发送请求
    try {
      const response = await fetch('http://127.0.0.1:8000/api/stocks/update/index_components/start', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        }
      });
      
      console.log('请求已发送，状态码:', response.status);
      const data = await response.json();
      console.log('响应数据:', data);
      
      if (response.ok) {
        statusEl.textContent = `已启动，${data.message || '开始更新指数成份'}`;
        toast('指数成份更新已启动');
        
        // 启用暂停按钮
        const toggleBtn = document.getElementById('updateIndexComponentsToggle');
        toggleBtn.disabled = false;
        toggleBtn.textContent = '暂停';
      } else {
        statusEl.textContent = `失败：${data.error || data.detail || '未知错误'}`;
        toast(`指数成份更新失败：${data.error || data.detail || '未知错误'}`);
      }
    } catch (error) {
      console.error('更新指数成份失败:', error);
      statusEl.textContent = `失败：${error.message}`;
      toast(`指数成份更新失败：${error.message}`);
    }
  });

  // 指数成份暂停/继续按钮事件 - 简化版本
  document.getElementById('updateIndexComponentsToggle')?.addEventListener('click', async function() {
    console.log('暂停/继续按钮被点击');
    
    // 获取状态元素
    const statusEl = document.getElementById('updateIndexComponentsStatus');
    
    // 判断当前按钮文本，决定是暂停还是继续
    const isPause = this.textContent === '暂停';
    const url = isPause ? 
      'http://127.0.0.1:8000/api/stocks/update/index_components/pause' : 
      'http://127.0.0.1:8000/api/stocks/update/index_components/resume';
    
    try {
      const response = await fetch(url, { method: 'POST' });
      const data = await response.json();
      
      if (response.ok) {
        // 更新按钮文本
        this.textContent = isPause ? '继续' : '暂停';
        // 更新状态文本
        statusEl.textContent = isPause ? '已暂停' : '已继续';
        toast(isPause ? '已暂停指数成份更新' : '已继续指数成份更新');
      } else {
        toast(`操作失败：${data.error || data.detail || '未知错误'}`);
      }
    } catch (error) {
      console.error('暂停/继续操作失败:', error);
      toast(`操作失败：${error.message}`);
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
      paramsEl.textContent = qdb.connected ? '连接状态正常' : (qdb.error || '连接失败');
      basicEl.textContent = data.stock_basic_count ?? 0;
      
      // 更新连接池状态
      updatePoolStatus(data.connection_pool);
    } catch(e) {
      qdbEl.textContent = '加载失败';
      paramsEl.textContent = '-';
      if (poolStatusEl) {
        poolStatusEl.textContent = '连接池状态：无法获取';
      }
    }
  }

  async function pollProgress(){
    try {
      const res = await fetch(`${API_BASE}/api/stocks/update/status`);
      const data = await res.json();
      const cur = data.controller?.current_code || '-';
      const runFlag = data.controller?.running ? '运行中' : (data.controller?.stopped ? '已停止' : '空闲');
      
      // 更新连接池状态
      updatePoolStatus(data.connection_pool);
      
      // 更新QuestDB连接状态显示
      if (qdbEl) qdbEl.textContent = data.questdb?.connected ? '已连接' : '未连接';
      if (paramsEl) paramsEl.textContent = data.questdb?.connected ? '连接状态正常' : (data.questdb?.error || '连接失败');
      // 原有全量状态
      fullStatusEl.textContent = `状态：${runFlag} | 当前：${cur}`;
      // 新增任务队列状态（从 queue_controller 取状态）
      if (queueStatusEl) {
        const qcodes = data.queue_controller?.current_codes || [];
        const qcur = qcodes.length > 0 ? qcodes.join(', ') : '-';
        const qrunFlag = data.queue_controller?.running ? '运行中' : (data.queue_controller?.stopped ? '已停止' : '空闲');
        const updatedCount = data.queue_controller?.updated_count || 0;
        const totalCodes = data.queue_controller?.total_codes || 0;
        const progress = totalCodes > 0 ? `${updatedCount}/${totalCodes} (${Math.round((updatedCount/totalCodes)*100)}%)` : '0/0 (0%)';
        queueStatusEl.textContent = `状态：${qrunFlag} | 当前：${qcur} | 进度：${progress}`;
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

  // 轮询金融数据更新状态
  async function pollFinancialDataProgress() {
    try {
      const res = await fetch(`${API_BASE}/api/stocks/update/financial_data/status`);
      if (res.ok) {
        const data = await res.json();
        if (data.success && data.data) {
          const statusData = data.data;
          // 更新金融数据状态显示
          if (financialDataStatusEl) {
            financialDataStatusEl.textContent = `状态：${statusData.status} | 已更新：${statusData.updated_count}/${statusData.total_count}`;
          }
        }
      }
    } catch(e) {
      console.error('轮询金融数据状态失败:', e);
    }
  }

  // 轮询指数成份更新状态
  async function pollIndexComponentsProgress() {
    try {
      const res = await fetch(`${API_BASE}/api/stocks/update/index_components/status`);
      if (res.ok) {
        const data = await res.json();
        if (data.success && data.data) {
          const statusData = data.data;
          // 更新指数成份状态显示
          if (indexComponentsStatusEl) {
            indexComponentsStatusEl.textContent = `状态：${statusData.status} | 已更新：${statusData.updated_count}/${statusData.total_count} | 当前：${statusData.current_index}`;
          }
        }
      }
    } catch(e) {
      console.error('轮询指数成份状态失败:', e);
    }
  }

  // 已删除增量更新按钮
  if (updateTab) updateTab.addEventListener('click', loadStatus);
  loadStatus();
  // 刷新模式相关变量
  let refreshIntervalId = null;
  let currentRefreshMode = 'auto';
  let currentInterval = 3000; // 默认3秒
  
  // 获取UI元素
  const autoRefreshRadio = document.querySelector('input[name="refreshMode"][value="auto"]');
  const manualRefreshRadio = document.querySelector('input[name="refreshMode"][value="manual"]');
  const refreshIntervalSelect = document.getElementById('refreshInterval');
  const manualRefreshBtn = document.getElementById('manualRefreshBtn');
  const autoRefreshSettings = document.getElementById('autoRefreshSettings');
  const manualRefreshSettings = document.getElementById('manualRefreshSettings');
  
  // 设置刷新模式
  function setupRefreshMode() {
    // 清除现有定时器
    if (refreshIntervalId) {
      clearInterval(refreshIntervalId);
      refreshIntervalId = null;
    }
    
    // 根据选择的模式设置刷新
    if (currentRefreshMode === 'auto') {
      // 显示自动刷新设置，隐藏手动刷新按钮
      autoRefreshSettings.style.display = 'flex';
      manualRefreshSettings.style.display = 'none';
      // 启动定时器，同时轮询所有状态
      refreshIntervalId = setInterval(() => {
        pollProgress();
        pollFinancialDataProgress();
        pollIndexComponentsProgress();
      }, currentInterval);
    } else {
      // 隐藏自动刷新设置，显示手动刷新按钮
      autoRefreshSettings.style.display = 'none';
      manualRefreshSettings.style.display = 'block';
      // 手动模式下不启动定时器
    }
  }
  
  // 自动刷新单选按钮事件
  autoRefreshRadio?.addEventListener('change', function() {
    if (this.checked) {
      currentRefreshMode = 'auto';
      setupRefreshMode();
    }
  });
  
  // 手动刷新单选按钮事件
  manualRefreshRadio?.addEventListener('change', function() {
    if (this.checked) {
      currentRefreshMode = 'manual';
      setupRefreshMode();
    }
  });
  
  // 刷新间隔选择器事件
  refreshIntervalSelect?.addEventListener('change', function() {
    currentInterval = parseInt(this.value, 10);
    if (currentRefreshMode === 'auto') {
      setupRefreshMode();
    }
  });
  
  // 手动刷新按钮事件
  manualRefreshBtn?.addEventListener('click', function() {
    pollProgress();
  });
  
  // 初始化设置
  if (autoRefreshRadio) autoRefreshRadio.checked = true;
  setupRefreshMode();
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
    return `${API_BASE}/api/stocks/tasks/list${qsStr ? ('?' + qsStr) : ''}`;
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

  async function reload(){    try {
      // 显示加载状态
      const loadingEl = document.getElementById('taskLoading');
      if (loadingEl) loadingEl.style.display = 'flex';
      
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
    } finally {
      // 隐藏加载状态
      const loadingEl = document.getElementById('taskLoading');
      if (loadingEl) loadingEl.style.display = 'none';
    }
  }

  applyBtn?.addEventListener('click', () => { page = 1; reload(); });
  
  // 失败重试按钮事件
  const retryBtn = qs('#taskRetryFailed');
  retryBtn?.addEventListener('click', async () => {
    try {
      // 禁用按钮并显示处理中状态
      retryBtn.disabled = true;
      retryBtn.textContent = '处理中...';
      
      // 调用API将失败和处理中的任务改为待处理状态
      const response = await fetch(`${API_BASE}/api/stocks/tasks/retry`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({
          status: ['失败', '处理中']
        })
      });
      
      const data = await response.json();
      
      // 显示处理结果提示
      if (data.success) {
        toast(`成功将 ${data.count || 0} 个任务标记为待处理状态`);
        // 重新加载任务列表
        reload();
      } else {
        toast(`操作失败: ${data.error || '未知错误'}`, 'error');
      }
    } catch (error) {
      toast(`网络错误: ${error.message}`, 'error');
    } finally {
      // 恢复按钮状态
      retryBtn.disabled = false;
      retryBtn.textContent = '失败重试';
    }
  });
  
  // 删除所有任务按钮事件
  const deleteAllBtn = qs('#taskDeleteAll');
  deleteAllBtn?.addEventListener('click', async () => {
    try {
      // 显示确认对话框，防止误操作
      if (!confirm('确定要删除所有任务吗？此操作不可恢复！')) {
        return;
      }
      
      // 禁用按钮并显示处理中状态
      deleteAllBtn.disabled = true;
      deleteAllBtn.textContent = '处理中...';
      
      // 调用API删除所有任务
      const response = await fetch(`${API_BASE}/api/stocks/tasks/retry`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({
          action: 'delete_all'
        })
      });
      
      const data = await response.json();
      
      // 显示处理结果提示
      if (data.success) {
        toast(`成功删除 ${data.count || 0} 个任务`);
        // 重新加载任务列表
        reload();
      } else {
        toast(`删除失败: ${data.error || '未知错误'}`, 'error');
      }
    } catch (error) {
      toast(`网络错误: ${error.message}`, 'error');
    } finally {
      // 恢复按钮状态
      deleteAllBtn.disabled = false;
      deleteAllBtn.textContent = '删除所有任务';
    }
  });
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

// 备份恢复功能实现
(function initDatabaseBackup() {
  console.log('初始化数据库备份恢复功能');
  
  // 选项卡切换逻辑
  const dbTabs = document.querySelectorAll('#database .tab-btn');
  dbTabs.forEach(tab => {
    tab.addEventListener('click', () => {
      const tabId = tab.getAttribute('data-tab');
      
      // 移除所有选项卡和内容的active类
      dbTabs.forEach(t => t.classList.remove('active'));
      document.querySelectorAll('#database .tab-content').forEach(content => {
        content.classList.remove('active');
      });
      
      // 添加当前选项卡和内容的active类
      tab.classList.add('active');
      const contentEl = document.getElementById(tabId);
      if (contentEl) {
        contentEl.classList.add('active');
        console.log(`切换到选项卡: ${tabId}`);
      }
    });
  });
  
  // 执行备份功能
  const backupBtn = qs('#backupBtn');
  const backupStatus = qs('#backupStatus');
  const backupTable = qs('#backupTable');
  
  if (backupBtn) {
    backupBtn.addEventListener('click', async () => {
      try {
        backupBtn.disabled = true;
        backupStatus.textContent = '正在执行备份...';
        
        // 模拟备份过程
        await new Promise(resolve => setTimeout(resolve, 1500));
        
        // 模拟备份成功
        const timestamp = new Date().toLocaleString();
        const size = (Math.random() * 100 + 50).toFixed(2);
        const backupId = 'backup_' + Date.now();
        
        backupStatus.textContent = '备份成功！';
        
        // 添加到备份记录表格
        const row = document.createElement('tr');
        row.innerHTML = `
          <td>${backupId}</td>
          <td>${timestamp}</td>
          <td>${size} MB</td>
          <td>
            <button class="btn-small" onclick="downloadBackup('${backupId}')">下载</button>
            <button class="btn-small danger" onclick="deleteBackup('${backupId}')">删除</button>
          </td>
        `;
        backupTable.appendChild(row);
        
      } catch (error) {
        console.error('备份失败:', error);
        backupStatus.textContent = '备份失败！';
      } finally {
        setTimeout(() => {
          backupBtn.disabled = false;
          backupStatus.textContent = '就绪';
        }, 2000);
      }
    });
  }
  
  // 执行恢复功能
  const restoreBtn = qs('#restoreBtn');
  const restoreStatus = qs('#restoreStatus');
  const restoreLog = qs('#restoreLog');
  const restoreLogBody = restoreLog ? qs('.listview-body', restoreLog) : null;
  const backupFileName = qs('#backupFileName');
  const restoreProgressContainer = qs('#restoreProgressContainer');
  const restoreProgressBar = qs('#restoreProgressBar');
  const restoreProgressText = qs('#restoreProgressText');
  const stockListContainer = qs('#stockList');
  
  // 申万数据恢复功能
  const swRestoreBtn = qs('#swRestoreBtn');
  const swRestoreStatus = qs('#swRestoreStatus');
  const swBackupFileName = qs('#swBackupFileName');
  const swRestoreProgressContainer = qs('#swRestoreProgressContainer');
  const swRestoreProgressBar = qs('#swRestoreProgressBar');
  const swRestoreProgressText = qs('#swRestoreProgressText');
  
  // 添加日志到listview的函数
  function addLogEntry(code, message, status, resultMessage = null, resultStatus = null, dataType = '股票') {
    if (!restoreLogBody) return;
    
    // 获取当前时间
    const now = new Date();
    const timeStr = now.toLocaleTimeString('zh-CN', {
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit'
    });
    
    // 创建日志条目
    const logItem = document.createElement('div');
    logItem.className = 'listview-item';
    logItem.dataset.code = code;
    logItem.dataset.type = dataType;
    
    // 创建状态标签
    let statusHtml = '';
    let statusClass = '';
    
    switch(status) {
      case 'success':
        statusClass = 'success';
        statusHtml = '成功';
        break;
      case 'warning':
        statusClass = 'warning';
        statusHtml = '警告';
        break;
      case 'error':
        statusClass = 'error';
        statusHtml = '失败';
        break;
      case 'info':
      default:
        statusClass = 'info';
        statusHtml = '信息';
        break;
    }
    
    // 创建结果状态标签
    let resultStatusHtml = '';
    let resultStatusClass = '';
    
    if (resultMessage && resultStatus) {
      switch(resultStatus) {
        case 'success':
          resultStatusClass = 'success';
          resultStatusHtml = '成功';
          break;
        case 'warning':
          resultStatusClass = 'warning';
          resultStatusHtml = '警告';
          break;
        case 'error':
          resultStatusClass = 'error';
          resultStatusHtml = '失败';
          break;
        case 'info':
        default:
          resultStatusClass = 'info';
          resultStatusHtml = '信息';
          break;
      }
    }
    
    // 设置日志内容 - 在一行中显示开始信息和结果信息
    logItem.innerHTML = `
      <div class="listview-column time">${timeStr}</div>
      <div class="listview-column stock-code">${code || '-'}</div>
      <div class="listview-column type">${dataType}</div>
      <div class="listview-column message">
        ${message}${resultMessage ? `<span class="result-message"> → ${resultMessage}</span>` : ''}
      </div>
      <div class="listview-column status">
        ${resultStatus ? `<span class="status-badge ${resultStatusClass}">${resultStatusHtml}</span>` : 
                         (statusHtml ? `<span class="status-badge ${statusClass}">${statusHtml}</span>` : '')}
      </div>
    `;
    
    // 根据当前列表中行数决定奇偶行样式
    const itemCount = restoreLogBody.children.length;
    // 新行是第0个位置，所以如果当前行数是奇数，新行就是偶数行
    if (itemCount % 2 === 1) {
      logItem.classList.add('listview-item-even');
    }
    
    // 添加到列表前部并保持在顶部
    if (restoreLogBody.firstChild) {
      restoreLogBody.insertBefore(logItem, restoreLogBody.firstChild);
      
      // 重新更新奇偶行样式
      Array.from(restoreLogBody.children).forEach((item, index) => {
        if (index % 2 === 0) {
          item.classList.remove('listview-item-even');
        } else {
          item.classList.add('listview-item-even');
        }
      });
    } else {
      restoreLogBody.appendChild(logItem);
    }
    restoreLogBody.scrollTop = 0;
    
    // 返回日志条目引用，便于后续更新
    return logItem;
  }
  
  // 更新现有日志条目的函数
  function updateLogEntry(logItem, resultMessage, resultStatus) {
    if (!logItem || !restoreLogBody) return;
    
    // 创建结果状态标签
    let resultStatusHtml = '';
    let resultStatusClass = '';
    
    switch(resultStatus) {
      case 'success':
        resultStatusClass = 'success';
        resultStatusHtml = '成功';
        break;
      case 'warning':
        resultStatusClass = 'warning';
        resultStatusHtml = '警告';
        break;
      case 'error':
        resultStatusClass = 'error';
        resultStatusHtml = '失败';
        break;
      case 'info':
      default:
        resultStatusClass = 'info';
        resultStatusHtml = '信息';
        break;
    }
    
    // 找到消息列和状态列
    const messageColumn = logItem.querySelector('.listview-column.message');
    const statusColumn = logItem.querySelector('.listview-column.status');
    
    if (messageColumn) {
      // 如果已经有结果消息，更新它，否则添加
      const existingResult = messageColumn.querySelector('.result-message');
      if (existingResult) {
        existingResult.textContent = ` → ${resultMessage}`;
      } else {
        messageColumn.innerHTML += `<span class="result-message"> → ${resultMessage}</span>`;
      }
    }
    
    if (statusColumn) {
      statusColumn.innerHTML = `<span class="status-badge ${resultStatusClass}">${resultStatusHtml}</span>`;
    }
    
    // 滚动到底部
    restoreLogBody.scrollTop = restoreLogBody.scrollHeight;
  }
  
  // 清空并重置UI
  function resetUI() {
    // 清空股票列表
    if (stockListContainer) {
      stockListContainer.innerHTML = '<div class="stock-item placeholder">请点击执行恢复按钮获取股票列表</div>';
    }
    
    // 清空日志列表
    if (restoreLogBody) {
      restoreLogBody.innerHTML = `
        <div class="listview-item placeholder">
          <div class="listview-column time"></div>
          <div class="listview-column stock-code"></div>
          <div class="listview-column message">请点击执行恢复按钮开始恢复操作</div>
          <div class="listview-column status"></div>
        </div>
      `;
    }
    
    // 隐藏进度条
    if (restoreProgressContainer) {
      restoreProgressContainer.style.display = 'none';
    }
    
    // 重置进度条状态
    if (restoreProgressBar) {
      restoreProgressBar.style.width = '0%';
    }
    if (restoreProgressText) {
      restoreProgressText.textContent = '0%';
    }
  }
  
  // 显示股票列表
  function displayStockList(stockCodes) {
    if (!stockListContainer || !stockCodes || stockCodes.length === 0) return;
    
    // 清空容器
    stockListContainer.innerHTML = '';
    
    // 添加每个股票代码
    stockCodes.forEach(code => {
      const stockItem = document.createElement('div');
      stockItem.className = 'stock-item';
      stockItem.textContent = code;
      stockListContainer.appendChild(stockItem);
    });
  }
  
  if (restoreBtn && backupFileName) {
    restoreBtn.addEventListener('click', async () => {
      try {
        const path = backupFileName.value.trim();
        if (!path) {
          toast('请输入备份路径');
          return;
        }
        
        // 重置UI
        resetUI();
        
        restoreBtn.disabled = true;
        restoreStatus.textContent = '正在执行恢复...';
        
        // 添加开始日志
        addLogEntry('', `开始恢复数据库，路径: ${path}`, 'info');
        addLogEntry('', '正在查询路径下的股票文件...', 'info');
        
        // 调用后端API，传递路径并获取股票代码列表
        const response = await fetch(`${API_BASE}/api/restore/get_stock_files`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ path: path })
        });
        
        if (!response.ok) {
          throw new Error(`API请求失败: ${response.status}`);
        }
        
        const data = await response.json();
        
        if (data.success && data.stock_codes && data.stock_codes.length > 0) {
          addLogEntry('', `成功获取 ${data.stock_codes.length} 个股票代码`, 'info');
          
          // 显示股票列表
          displayStockList(data.stock_codes);
          
          // 显示进度条
          restoreProgressContainer.style.display = 'block';
          
          // 获取复选框状态，决定是否使用集群表
          const useClusterCheckbox = document.getElementById('useCluster');
          const useCluster = useClusterCheckbox ? useClusterCheckbox.checked : false;
          const tableName = useCluster ? 'stock_daily_all' : 'stock_daily';
          
          // 对每个股票代码调用API进行恢复
          const totalStocks = data.stock_codes.length;
          let successCount = 0;
          let failCount = 0;
          
          for (let i = 0; i < totalStocks; i++) {
            const code = data.stock_codes[i];
            
            // 添加开始处理日志，保存返回的日志条目引用
            const logEntry = addLogEntry(code, '开始恢复数据...', 'info');
            
            try {
              // 调用恢复API，添加表名参数
              const stockResponse = await fetch(`${API_BASE}/api/restore/process`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ code: code, path: path, table_name: tableName })
              });
              
              if (stockResponse.ok) {
                const result = await stockResponse.json();
                // 更新同一个日志条目，添加结果信息
                updateLogEntry(logEntry, result.message || '恢复成功', 'success');
                successCount++;
              } else {
                // 更新同一个日志条目，添加警告信息
                updateLogEntry(logEntry, `恢复可能有问题，状态码: ${stockResponse.status}`, 'warning');
                failCount++;
              }
            } catch (stockError) {
              // 更新同一个日志条目，添加错误信息
              updateLogEntry(logEntry, `恢复失败: ${stockError.message}`, 'error');
              failCount++;
            }
            
            // 更新进度条
            const progress = Math.round(((i + 1) / totalStocks) * 100);
            restoreProgressBar.style.width = `${progress}%`;
            restoreProgressText.textContent = `${progress}% (${i + 1}/${totalStocks})`;
          }
          
          // 添加总结日志
          restoreStatus.textContent = '恢复完成！';
          const summaryMessage = `所有股票处理完成。成功: ${successCount}, 失败: ${failCount}, 总数: ${totalStocks}`;
          addLogEntry('', summaryMessage, failCount > 0 ? 'warning' : 'success');
          
          // 隐藏进度条
          setTimeout(() => {
            restoreProgressContainer.style.display = 'none';
          }, 1000);
          
          toast(summaryMessage);
        } else {
        addLogEntry('', '未找到股票代码文件或路径不存在', 'warning', null, null, '股票');
        restoreStatus.textContent = '恢复完成';
      }
      
    } catch (error) {
      console.error('恢复失败:', error);
      restoreStatus.textContent = '恢复失败！';
      addLogEntry('', `恢复过程中发生错误: ${error.message}`, 'error', null, null, '股票');
      toast(`恢复失败: ${error.message}`);
    } finally {
      setTimeout(() => {
        restoreBtn.disabled = false;
        restoreStatus.textContent = '就绪';
      }, 3000);
    }
  });
}

  // 申万数据恢复功能
  if (swRestoreBtn && swBackupFileName) {
    swRestoreBtn.addEventListener('click', async () => {
      try {
        const path = swBackupFileName.value.trim();
        if (!path) {
          toast('请输入申万数据备份路径');
          return;
        }
        
        swRestoreBtn.disabled = true;
        swRestoreStatus.textContent = '正在执行恢复...';
        
        // 添加开始日志
        addLogEntry('', `开始恢复申万数据，路径: ${path}`, 'info', null, null, '申万');
        addLogEntry('', '正在查询路径下的申万指数文件...', 'info', null, null, '申万');
        
        // 调用后端API，传递路径并获取申万指数代码列表
        const response = await fetch(`${API_BASE}/api/restore/get_sw_files`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ path: path })
        });
        
        if (!response.ok) {
          throw new Error(`API请求失败: ${response.status}`);
        }
        
        const data = await response.json();
        
        if (data.success && data.stock_codes && data.stock_codes.length > 0) {
          addLogEntry('', `成功获取 ${data.stock_codes.length} 个申万指数代码`, 'info', null, null, '申万');
          
          // 显示进度条
          swRestoreProgressContainer.style.display = 'block';
          
          // 对每个申万指数代码调用API进行恢复
          const totalCodes = data.stock_codes.length;
          let successCount = 0;
          let failCount = 0;
          
          for (let i = 0; i < totalCodes; i++) {
            const code = data.stock_codes[i];
            
            // 添加开始处理日志，保存返回的日志条目引用
            const logEntry = addLogEntry(code, '开始恢复数据...', 'info', null, null, '申万');
            
            try {
              // 调用恢复API
              const swResponse = await fetch(`${API_BASE}/api/restore/process`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ code: code, path: path,table_name: 'sw_index' })
              });
              
              if (swResponse.ok) {
                const result = await swResponse.json();
                // 更新同一个日志条目，添加结果信息
                updateLogEntry(logEntry, result.message || '恢复成功', 'success');
                successCount++;
              } else {
                // 更新同一个日志条目，添加警告信息
                updateLogEntry(logEntry, `恢复可能有问题，状态码: ${swResponse.status}`, 'warning');
                failCount++;
              }
            } catch (swError) {
              // 更新同一个日志条目，添加错误信息
              updateLogEntry(logEntry, `恢复失败: ${swError.message}`, 'error');
              failCount++;
            }
            
            // 更新进度条
            const progress = Math.round(((i + 1) / totalCodes) * 100);
            swRestoreProgressBar.style.width = `${progress}%`;
            swRestoreProgressText.textContent = `${progress}% (${i + 1}/${totalCodes})`;
          }
          
          // 添加总结日志
          swRestoreStatus.textContent = '恢复完成！';
          const summaryMessage = `所有申万数据处理完成。成功: ${successCount}, 失败: ${failCount}, 总数: ${totalCodes}`;
          addLogEntry('', summaryMessage, failCount > 0 ? 'warning' : 'success', null, null, '申万');
          
          // 隐藏进度条
          setTimeout(() => {
            swRestoreProgressContainer.style.display = 'none';
          }, 1000);
          
          toast(summaryMessage);
        } else {
          addLogEntry('', '未找到申万指数文件或路径不存在', 'warning', null, null, '申万');
          swRestoreStatus.textContent = '恢复完成';
        }
        
      } catch (error) {
        console.error('申万数据恢复失败:', error);
        swRestoreStatus.textContent = '恢复失败！';
        addLogEntry('', `恢复过程中发生错误: ${error.message}`, 'error', null, null, '申万');
        toast(`申万数据恢复失败: ${error.message}`);
      } finally {
        setTimeout(() => {
          swRestoreBtn.disabled = false;
          swRestoreStatus.textContent = '就绪';
        }, 3000);
      }
    });
  }
  
  // 复权因子数据恢复功能
  const adjustFactorRestoreBtn = qs('#adjustFactorRestoreBtn');
  const adjustFactorBackupFileName = qs('#adjustFactorBackupFileName');
  const adjustFactorRestoreStatus = qs('#adjustFactorRestoreStatus');
  const adjustFactorRestoreProgressContainer = qs('#adjustFactorRestoreProgressContainer');
  const adjustFactorRestoreProgressBar = qs('#adjustFactorRestoreProgressBar');
  const adjustFactorRestoreProgressText = qs('#adjustFactorRestoreProgressText');
  
  if (adjustFactorRestoreBtn && adjustFactorBackupFileName) {
    adjustFactorRestoreBtn.addEventListener('click', async () => {
      try {
        const path = adjustFactorBackupFileName.value.trim();
        if (!path) {
          toast('请输入复权因子数据备份路径');
          return;
        }
        
        adjustFactorRestoreBtn.disabled = true;
        adjustFactorRestoreStatus.textContent = '正在执行恢复...';
        
        // 添加开始日志
        addLogEntry('', `开始恢复复权因子数据，路径: ${path}`, 'info', null, null, '复权因子');
        addLogEntry('', '正在查询路径下的复权因子文件...', 'info', null, null, '复权因子');
        
        // 调用后端API，传递路径并获取复权因子代码列表
        const response = await fetch(`${API_BASE}/api/restore/get_stock_files`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ path: path })
        });
        
        if (!response.ok) {
          throw new Error(`API请求失败: ${response.status}`);
        }
        
        const data = await response.json();
        
        if (data.success && data.stock_codes && data.stock_codes.length > 0) {
          addLogEntry('', `成功获取 ${data.stock_codes.length} 个复权因子代码`, 'info', null, null, '复权因子');
          
          // 显示进度条
          adjustFactorRestoreProgressContainer.style.display = 'block';
          
          // 对每个复权因子代码调用API进行恢复
          const totalCodes = data.stock_codes.length;
          let successCount = 0;
          let failCount = 0;
          
          for (let i = 0; i < totalCodes; i++) {
            const code = data.stock_codes[i];
            
            // 添加开始处理日志，保存返回的日志条目引用
            const logEntry = addLogEntry(code, '开始恢复复权因子数据...', 'info', null, null, '复权因子');
            
            try {
              // 调用恢复API
              const adjustResponse = await fetch(`${API_BASE}/api/restore/process`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ code: code, path: path, table_name: 'fq_factor' })
              });
              
              if (adjustResponse.ok) {
                const result = await adjustResponse.json();
                // 更新同一个日志条目，添加结果信息
                updateLogEntry(logEntry, result.message || '恢复成功', 'success');
                successCount++;
              } else {
                // 更新同一个日志条目，添加警告信息
                updateLogEntry(logEntry, `恢复可能有问题，状态码: ${adjustResponse.status}`, 'warning');
                failCount++;
              }
            } catch (adjustError) {
              // 更新同一个日志条目，添加错误信息
              updateLogEntry(logEntry, `恢复失败: ${adjustError.message}`, 'error');
              failCount++;
            }
            
            // 更新进度条
            const progress = Math.round(((i + 1) / totalCodes) * 100);
            adjustFactorRestoreProgressBar.style.width = `${progress}%`;
            adjustFactorRestoreProgressText.textContent = `${progress}% (${i + 1}/${totalCodes})`;
          }
          
          // 添加总结日志
          adjustFactorRestoreStatus.textContent = '恢复完成！';
          const summaryMessage = `所有复权因子数据处理完成。成功: ${successCount}, 失败: ${failCount}, 总数: ${totalCodes}`;
          addLogEntry('', summaryMessage, failCount > 0 ? 'warning' : 'success', null, null, '复权因子');
          
          // 隐藏进度条
          setTimeout(() => {
            adjustFactorRestoreProgressContainer.style.display = 'none';
          }, 1000);
          
          toast(summaryMessage);
        } else {
          addLogEntry('', '未找到复权因子文件或路径不存在', 'warning', null, null, '复权因子');
          adjustFactorRestoreStatus.textContent = '恢复完成';
        }
        
      } catch (error) {
        console.error('复权因子数据恢复失败:', error);
        adjustFactorRestoreStatus.textContent = '恢复失败！';
        addLogEntry('', `恢复过程中发生错误: ${error.message}`, 'error', null, null, '复权因子');
        toast(`复权因子数据恢复失败: ${error.message}`);
      } finally {
        setTimeout(() => {
          adjustFactorRestoreBtn.disabled = false;
          adjustFactorRestoreStatus.textContent = '就绪';
        }, 3000);
      }
    });
  }

  // 财务数据恢复功能
  const financialRestoreBtn = qs('#financialRestoreBtn');
  const financialBackupFileName = qs('#financialBackupFileName');
  const financialRestoreStatus = qs('#financialRestoreStatus');
  const financialRestoreProgressContainer = qs('#financialRestoreProgressContainer');
  const financialRestoreProgressBar = qs('#financialRestoreProgressBar');
  const financialRestoreProgressText = qs('#financialRestoreProgressText');
  
  if (financialRestoreBtn && financialBackupFileName) {
    financialRestoreBtn.addEventListener('click', async () => {
      try {
        const path = financialBackupFileName.value.trim();
        if (!path) {
          toast('请输入财务数据备份路径');
          return;
        }
        
        financialRestoreBtn.disabled = true;
        financialRestoreStatus.textContent = '正在执行恢复...';
        
        // 添加开始日志
        addLogEntry('', `开始恢复财务数据，路径: ${path}`, 'info', null, null, '财务');
        addLogEntry('', '正在查询路径下的财务文件...', 'info', null, null, '财务');
        
        // 调用后端API，传递路径并获取财务代码列表
        const response = await fetch(`${API_BASE}/api/restore/get_stock_files`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ path: path })
        });
        
        if (!response.ok) {
          throw new Error(`API请求失败: ${response.status}`);
        }
        
        const data = await response.json();
        
        if (data.success && data.stock_codes && data.stock_codes.length > 0) {
          addLogEntry('', `成功获取 ${data.stock_codes.length} 个财务代码`, 'info', null, null, '财务');
          
          // 显示进度条
          financialRestoreProgressContainer.style.display = 'block';
          
          // 对每个财务代码调用API进行恢复
          const totalCodes = data.stock_codes.length;
          let successCount = 0;
          let failCount = 0;
          
          for (let i = 0; i < totalCodes; i++) {
            const code = data.stock_codes[i];
            
            // 添加开始处理日志，保存返回的日志条目引用
            const logEntry = addLogEntry(code, '开始恢复财务数据...', 'info', null, null, '财务');
            
            try {
              // 调用恢复API
              const financialResponse = await fetch(`${API_BASE}/api/restore/process`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ code: code, path: path, table_name: 'stock_fin' })
              });
              
              if (financialResponse.ok) {
                const result = await financialResponse.json();
                // 更新同一个日志条目，添加结果信息
                updateLogEntry(logEntry, result.message || '恢复成功', 'success');
                successCount++;
              } else {
                // 更新同一个日志条目，添加警告信息
                updateLogEntry(logEntry, `恢复可能有问题，状态码: ${financialResponse.status}`, 'warning');
                failCount++;
              }
            } catch (financialError) {
              // 更新同一个日志条目，添加错误信息
              updateLogEntry(logEntry, `恢复失败: ${financialError.message}`, 'error');
              failCount++;
            }
            
            // 更新进度条
            const progress = Math.round(((i + 1) / totalCodes) * 100);
            financialRestoreProgressBar.style.width = `${progress}%`;
            financialRestoreProgressText.textContent = `${progress}% (${i + 1}/${totalCodes})`;
          }
          
          // 添加总结日志
          financialRestoreStatus.textContent = '恢复完成！';
          const summaryMessage = `所有财务数据处理完成。成功: ${successCount}, 失败: ${failCount}, 总数: ${totalCodes}`;
          addLogEntry('', summaryMessage, failCount > 0 ? 'warning' : 'success', null, null, '财务');
          
          // 隐藏进度条
          setTimeout(() => {
            financialRestoreProgressContainer.style.display = 'none';
          }, 1000);
          
          toast(summaryMessage);
        } else {
          addLogEntry('', '未找到财务文件或路径不存在', 'warning', null, null, '财务');
          financialRestoreStatus.textContent = '恢复完成';
        }
        
      } catch (error) {
        console.error('财务数据恢复失败:', error);
        financialRestoreStatus.textContent = '恢复失败！';
        addLogEntry('', `恢复过程中发生错误: ${error.message}`, 'error', null, null, '财务');
        toast(`财务数据恢复失败: ${error.message}`);
      } finally {
        setTimeout(() => {
          financialRestoreBtn.disabled = false;
          financialRestoreStatus.textContent = '就绪';
        }, 3000);
      }
    });
  }
  
  // 指数成份恢复容器交互逻辑
  const indexComponentsFileName = document.getElementById('indexComponentsFileName');
  const indexComponentsRestoreBtn = document.getElementById('indexComponentsRestoreBtn');
  const indexComponentsRestoreStatus = document.getElementById('indexComponentsRestoreStatus');
  const indexComponentsRestoreProgressContainer = document.getElementById('indexComponentsRestoreProgressContainer');
  const indexComponentsRestoreProgressBar = document.getElementById('indexComponentsRestoreProgressBar');
  const indexComponentsRestoreProgressText = document.getElementById('indexComponentsRestoreProgressText');
  
  if (indexComponentsRestoreBtn && indexComponentsFileName) {
    indexComponentsRestoreBtn.addEventListener('click', async () => {
      try {
        const path = indexComponentsFileName.value.trim();
        if (!path) {
          toast('请输入指数成份数据路径');
          return;
        }
        
        indexComponentsRestoreBtn.disabled = true;
        indexComponentsRestoreStatus.textContent = '正在执行恢复...';
        
        // 添加开始日志
        addLogEntry('', `开始恢复指数成份，路径: ${path}`, 'info', null, null, '指数成份恢复');
        
        // 调用后端API，传递路径并获取指数代码列表
        const response = await fetch(`${API_BASE}/api/restore/get_stock_files`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ path: path })
        });
        
        if (!response.ok) {
          throw new Error(`API请求失败: ${response.status}`);
        }
        
        const data = await response.json();
        
        if (data.success && data.stock_codes && data.stock_codes.length > 0) {
          addLogEntry('', `成功获取 ${data.stock_codes.length} 个指数代码`, 'info', null, null, '指数成份恢复');
          
          // 显示进度条
          indexComponentsRestoreProgressContainer.style.display = 'block';
          
          // 对每个指数代码调用API进行恢复
          const totalCodes = data.stock_codes.length;
          let successCount = 0;
          let failCount = 0;
          
          for (let i = 0; i < totalCodes; i++) {
            const code = data.stock_codes[i];
            
            // 添加开始处理日志
            const logEntry = addLogEntry(code, '开始恢复指数成份...', 'info', null, null, '指数成份恢复');
            
            try {
              // 调用恢复API - 使用现有的RestoreStockData API，传递正确的table_name
              const restoreResponse = await fetch(`${API_BASE}/api/restore/process`, {
                method: 'POST',
                headers: {
                  'Content-Type': 'application/json'
                },
                body: JSON.stringify({ code: code, path: path, table_name: 'stock_index' })
              });
              
              if (restoreResponse.ok) {
                const result = await restoreResponse.json();
                updateLogEntry(logEntry, result.message || '恢复成功', 'success');
                successCount++;
              } else {
                updateLogEntry(logEntry, `恢复可能有问题，状态码: ${restoreResponse.status}`, 'warning');
                failCount++;
              }
            } catch (restoreError) {
              updateLogEntry(logEntry, `恢复失败: ${restoreError.message}`, 'error');
              failCount++;
            }
            
            // 更新进度条
            const progress = Math.round(((i + 1) / totalCodes) * 100);
            indexComponentsRestoreProgressBar.style.width = `${progress}%`;
            indexComponentsRestoreProgressText.textContent = `${progress}% (${i + 1}/${totalCodes})`;
          }
          
          // 添加总结日志
          indexComponentsRestoreStatus.textContent = '恢复完成！';
          const summaryMessage = `所有指数成份恢复完成。成功: ${successCount}, 失败: ${failCount}, 总数: ${totalCodes}`;
          addLogEntry('', summaryMessage, failCount > 0 ? 'warning' : 'success', null, null, '指数成份恢复');
          
          // 隐藏进度条
          setTimeout(() => {
            indexComponentsRestoreProgressContainer.style.display = 'none';
          }, 1000);
          
          toast(summaryMessage);
        } else {
          addLogEntry('', '未找到指数成份文件或路径不存在', 'warning', null, null, '指数成份恢复');
          indexComponentsRestoreStatus.textContent = '恢复完成';
        }
        
      } catch (error) {
        console.error('指数成份恢复失败:', error);
        indexComponentsRestoreStatus.textContent = '恢复失败！';
        addLogEntry('', `恢复过程中发生错误: ${error.message}`, 'error', null, null, '指数成份恢复');
        toast(`指数成份恢复失败: ${error.message}`);
      } finally {
        setTimeout(() => {
          indexComponentsRestoreBtn.disabled = false;
          indexComponentsRestoreStatus.textContent = '就绪';
        }, 3000);
      }
    });
  }

  // 模拟恢复过程的函数
  async function simulateRestoreProcess(fileName) {
    // 模拟不同阶段的恢复过程
    const steps = [
      `开始验证备份文件: ${fileName}`,
      '连接到数据库...',
      '备份文件验证通过',
      '准备恢复环境...',
      '开始数据恢复...',
      '正在恢复表结构...',
      '正在恢复数据...',
      '正在重建索引...',
      '恢复完成，正在验证数据一致性...',
      '数据一致性验证通过'
    ];
    
    for (let i = 0; i < steps.length; i++) {
      restoreLog.innerHTML += steps[i] + '<br>';
      // 模拟每一步的延迟
      await new Promise(resolve => setTimeout(resolve, 500 + Math.random() * 800));
    }
  }
  
  // 监听侧边栏切换到数据库备份恢复标签
  const dbTab = document.querySelector('.sidebar .tab[data-target="database"]');
  if (dbTab) {
    dbTab.addEventListener('click', () => {
      console.log('切换到数据库备份恢复标签');
    });
  }
})();

// 全局函数 - 下载备份文件
function downloadBackup(backupId) {
  console.log(`下载备份文件: ${backupId}`);
  toast(`开始下载备份文件: ${backupId}`);
  // 这里可以添加实际的下载逻辑
}

// 全局函数 - 删除备份文件
function deleteBackup(backupId) {
  if (confirm(`确定要删除备份文件 ${backupId} 吗？`)) {
    console.log(`删除备份文件: ${backupId}`);
    
    // 找到并删除表格行
    const rows = document.querySelectorAll('#backupTable tr');
    for (let row of rows) {
      if (row.cells[0].textContent === backupId) {
        row.remove();
        break;
      }
    }
    
    toast(`备份文件 ${backupId} 已删除`);
  }
}

// 股票数据同步功能实现
(function initStockSyncButton() {
    const startBtn = document.getElementById('startStockSyncBtn');
    if (startBtn) {
        startBtn.addEventListener('click', async () => {
            // 获取输入框中的路径
            const mainPathInput = document.getElementById('stockMainDir');
            const appendPathInput = document.getElementById('stockAdditionalDir');
            
            if (!mainPathInput || !appendPathInput) {
                toast('找不到路径输入框');
                return;
            }
            
            const mainPath = mainPathInput.value.trim();
            const appendPath = appendPathInput.value.trim();
            
            // 验证输入
            if (!mainPath) {
                toast('请输入主目录路径');
                return;
            }
            
            if (!appendPath) {
                toast('请输入追加目录路径');
                return;
            }
            
            // 禁用按钮，显示加载状态
            startBtn.disabled = true;
            startBtn.innerHTML = '<i class="ri-loader-2-line ri-spin"></i> 同步中...';
            
            try {
                // 调用后端API
                const response = await fetch(`${API_BASE}/api/restore/merge`, {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                    },
                    body: JSON.stringify({
                        main_path: mainPath,
                        append_path: appendPath
                    })
                });
                console.log('API请求已发送，路径:', `${API_BASE}/api/restore/merge`);
                
                if (!response.ok) {
                    throw new Error(`HTTP error! status: ${response.status}`);
                }
                
                const data = await response.json();
                
                if (data.success) {
                    toast(`同步成功！找到 ${data.total_count} 个股票代码`);
                    
                    // 显示股票代码列表（可以根据需要进行格式化展示）
                    console.log('股票代码列表:', data.stock_codes);
                    
                    // 确保有显示区域，如果不存在则创建
                    let resultArea = document.getElementById('stockSyncResult');
                    if (!resultArea) {
                        // 创建结果显示区域
                        resultArea = document.createElement('div');
                        resultArea.id = 'stockSyncResult';
                        resultArea.className = 'stock-sync-result';
                        
                        // 添加明确的样式，确保可见
                        resultArea.style.display = 'block';
                        resultArea.style.marginTop = '16px';
                        resultArea.style.padding = '16px';
                        resultArea.style.backgroundColor = '#f8f9fa';
                        resultArea.style.border = '1px solid #ddd';
                        resultArea.style.borderRadius = '6px';
                        resultArea.style.zIndex = '100';
                        
                        // 查找放置位置 - 尝试放在同步按钮下方的容器中
                        const startBtn = document.getElementById('startStockSyncBtn');
                        if (startBtn && startBtn.parentNode) {
                            // 放在按钮的父容器的末尾，确保在按钮之后
                            startBtn.parentNode.appendChild(resultArea);
                        } else {
                            // 否则找到stock-sync标签容器并放置在内部
                            const stockSyncTab = document.getElementById('stock-sync');
                            if (stockSyncTab) {
                                stockSyncTab.appendChild(resultArea);
                            } else {
                                // 最后方案：放在body末尾
                                document.body.appendChild(resultArea);
                            }
                        }
                    }
                    // 确保元素可见
                    resultArea.style.display = 'block';
                    
                    // 设置结果区域内容
                    resultArea.innerHTML = `
                        <div class="sync-result">
                            <div class="result-summary">
                                <h3>同步结果</h3>
                                <div style="display: flex; gap: 20px; margin-top: 12px; background: #f0f7ff; padding: 12px; border-radius: 6px; border: 1px solid #d0e3ff;">
                                    <p style="margin: 0; color: var(--primary); font-weight: 500;">主目录文件数: ${data.main_path_count}</p>
                                    <p style="margin: 0; color: var(--primary); font-weight: 500;">追加目录文件数: ${data.append_path_count}</p>
                                    <p style="margin: 0; color: var(--primary); font-weight: 500;">合并后文件数: ${data.total_count}</p>
                                </div>
                            </div>
                            <div class="stock-codes">
                                <h4>股票代码列表:</h4>
                                <div class="code-grid stats-container overflow-auto" style="max-height: 150px; font-size: 11px; line-height: 1.4; white-space: pre-wrap; word-break: break-all; margin-top: 10px;">${data.stock_codes.slice(0, 100).join(', ')}</div>
                                ${data.stock_codes.length > 100 ? `<p class="more-codes">...等${data.stock_codes.length}个股票代码</p>` : ''}
                            </div>
                            <div class="merge-actions">
                                <button id="startMergeBtn" class="btn btn-primary">
                                    开始合并所有股票数据
                                </button>
                                <div id="mergeProgress" class="progress-container" style="display: none;">
                                    <div class="progress-info">
                                        <span>合并进度</span>
                                        <span class="progress-text">0/${data.stock_codes.length}</span>
                                    </div>
                                    <div class="progress-bar-wrapper">
                                        <div class="progress-bar"></div>
                                    </div>
                                </div>
                                <div id="mergeLog" class="merge-log"></div>
                                <div id="mergeStats" class="merge-stats" style="display: none;">
                                    <h4>合并统计</h4>
                                    <div class="stats-content">
                                        <p>总股票数: <span id="totalStocks">${data.stock_codes.length}</span></p>
                                        <p>成功数: <span id="successStocks" class="success">0</span></p>
                                        <p>失败数: <span id="failedStocks" class="error">0</span></p>
                                        <p>新增行数: <span id="totalNewLines">0</span></p>
                                        <p>过滤重复行数: <span id="totalDuplicateLines">0</span></p>
                                    </div>
                                </div>
                            </div>
                        </div>
                    `;
                    
                    // 添加合并按钮点击事件
                    const mergeBtn = document.getElementById('startMergeBtn');
                    const progressContainer = document.getElementById('mergeProgress');
                    const progressBar = progressContainer.querySelector('.progress-bar');
                    const progressText = progressContainer.querySelector('.progress-text');
                    const mergeLog = document.getElementById('mergeLog');
                    const mergeStats = document.getElementById('mergeStats');
                    const successStocksEl = document.getElementById('successStocks');
                    const failedStocksEl = document.getElementById('failedStocks');
                    const totalNewLinesEl = document.getElementById('totalNewLines');
                    const totalDuplicateLinesEl = document.getElementById('totalDuplicateLines');
                    
                    // 安全检查
                    if (mergeBtn && progressContainer && progressBar && progressText && mergeLog) {
                        mergeBtn.addEventListener('click', async () => {
                            // 确认合并操作
                            if (!confirm(`确定要合并 ${data.stock_codes.length} 个股票的数据吗？`)) {
                                return;
                            }
                            
                            // 禁用按钮，显示进度和统计
                            mergeBtn.disabled = true;
                            mergeBtn.innerHTML = '<i class="ri-loader-2-line ri-spin"></i> 合并中...';
                            progressContainer.style.display = 'block';
                            mergeStats.style.display = 'block';
                            mergeLog.innerHTML = '';
                            
                            let successCount = 0;
                            let failCount = 0;
                            let totalNewLines = 0;
                            let totalDuplicateLines = 0;
                            const failedStocks = [];
                            
                            try {
                                // 逐个合并股票数据
                                for (let i = 0; i < data.stock_codes.length; i++) {
                                    const stockCode = data.stock_codes[i];
                                    
                                    // 更新进度
                                    const progress = ((i + 1) / data.stock_codes.length) * 100;
                                    progressBar.style.width = `${progress}%`;
                                    progressText.textContent = `${i + 1}/${data.stock_codes.length}`;
                                    
                                    // 添加日志
                                    mergeLog.innerHTML += `<div class="log-item processing">正在合并股票 ${stockCode}...</div>`;
                                    mergeLog.scrollTop = mergeLog.scrollHeight; // 自动滚动到底部
                                    
                                    // 调用mergeItem API
                                    const mergeResponse = await fetch(`${API_BASE}/api/restore/mergeItem`, {
                                        method: 'POST',
                                        headers: {
                                            'Content-Type': 'application/json',
                                        },
                                        body: JSON.stringify({
                                            main_path: mainPath,
                                            append_path: appendPath,
                                            stock_code: stockCode
                                        })
                                    });
                                    
                                    const mergeData = await mergeResponse.json();
                                    
                                    if (mergeData.success) {
                                        successCount++;
                                        
                                        // 累加统计数据
                                        if (mergeData.new_lines_added) {
                                            totalNewLines += mergeData.new_lines_added;
                                        }
                                        if (mergeData.duplicate_lines_filtered) {
                                            totalDuplicateLines += mergeData.duplicate_lines_filtered;
                                        }
                                        
                                        // 更新统计显示
                                        if (successStocksEl) successStocksEl.textContent = successCount;
                                        if (totalNewLinesEl) totalNewLinesEl.textContent = totalNewLines;
                                        if (totalDuplicateLinesEl) totalDuplicateLinesEl.textContent = totalDuplicateLines;
                                        
                                        mergeLog.innerHTML += `<div class="log-item success">✓ 股票 ${stockCode} 合并成功`;
                                        if (mergeData.new_lines_added !== undefined) {
                                            mergeLog.innerHTML += ` (新增: ${mergeData.new_lines_added}行, 过滤: ${mergeData.duplicate_lines_filtered || 0}行)`;
                                        }
                                        mergeLog.innerHTML += `</div>`;
                                    } else {
                                        failCount++;
                                        failedStocks.push(stockCode);
                                        
                                        // 更新统计显示
                                        if (failedStocksEl) failedStocksEl.textContent = failCount;
                                        
                                        mergeLog.innerHTML += `<div class="log-item error">✗ 股票 ${stockCode} 合并失败: ${mergeData.message || '未知错误'}</div>`;
                                    }
                                    
                                    mergeLog.scrollTop = mergeLog.scrollHeight; // 自动滚动到底部
                                    
                                    // 每处理5个股票，短暂暂停一下，避免请求过快
                                    if ((i + 1) % 5 === 0) {
                                        await new Promise(resolve => setTimeout(resolve, 100));
                                    }
                                }
                                
                                // 合并完成，显示结果
                                let resultMessage = `合并完成：成功 ${successCount} 个，失败 ${failCount} 个`;
                                if (totalNewLines > 0) {
                                    resultMessage += `，新增 ${totalNewLines} 行数据，过滤 ${totalDuplicateLines} 行重复数据`;
                                }
                                if (failCount > 0) {
                                    resultMessage += `<br>失败的股票代码：${failedStocks.join(', ')}`;
                                }
                                mergeLog.innerHTML += `<div class="log-item summary"><strong>${resultMessage}</strong></div>`;
                                toast(resultMessage);
                                
                            } catch (error) {
                                console.error('股票合并过程中发生错误:', error);
                                mergeLog.innerHTML += `<div class="log-item error">合并过程中发生错误: ${error.message}</div>`;
                                toast(`合并过程中发生错误: ${error.message}`);
                            } finally {
                                // 恢复按钮状态
                                mergeBtn.disabled = false;
                                mergeBtn.innerHTML = '开始合并所有股票数据';
                            }
                        });
                    }
                    
                    // 添加基本样式
                    const styleId = 'stock-sync-styles';
                    if (!document.getElementById(styleId)) {
                        const style = document.createElement('style');
                        style.id = styleId;
                        style.textContent = `
                            .stock-sync-result {
                                margin-top: 20px;
                                padding: 20px;
                                background: #f8f9fa;
                                border-radius: 8px;
                                border: 1px solid #e9ecef;
                            }
                            .sync-result h3 {
                                margin-top: 0;
                                color: #343a40;
                            }
                            .result-summary p {
                                margin: 8px 0;
                                color: #495057;
                            }
                            .stock-codes {
                                margin: 20px 0;
                            }
                            .code-grid {
                                background: #fff;
                                padding: 15px;
                                border-radius: 4px;
                                border: 1px solid #dee2e6;
                                white-space: pre-wrap;
                                word-break: break-all;
                                max-height: 200px;
                                overflow-y: auto;
                            }
                            .more-codes {
                                color: #6c757d;
                                font-style: italic;
                            }
                            .merge-actions {
                                margin-top: 20px;
                            }
                            .progress-container {
                                margin: 20px 0;
                                padding: 15px;
                                background: #fff;
                                border-radius: 4px;
                                border: 1px solid #dee2e6;
                            }
                            .progress-info {
                                display: flex;
                                justify-content: space-between;
                                margin-bottom: 10px;
                                font-size: 14px;
                                color: #495057;
                            }
                            .progress-bar-wrapper {
                                height: 8px;
                                background: #e9ecef;
                                border-radius: 4px;
                                overflow: hidden;
                            }
                            .progress-bar {
                                height: 100%;
                                background: #007bff;
                                transition: width 0.3s ease;
                            }
                            .merge-log {
                                background: #fff;
                                border: 1px solid #dee2e6;
                                border-radius: 4px;
                                padding: 15px;
                                max-height: 300px;
                                overflow-y: auto;
                                font-family: monospace;
                                font-size: 14px;
                                margin: 15px 0;
                            }
                            .merge-log .log-item {
                                margin: 5px 0;
                                padding: 5px;
                                border-radius: 3px;
                            }
                            .log-item.processing {
                                color: #007bff;
                            }
                            .log-item.success {
                                color: #28a745;
                                background: #d4edda;
                            }
                            .log-item.error {
                                color: #dc3545;
                                background: #f8d7da;
                            }
                            .log-item.summary {
                                margin-top: 15px;
                                padding: 10px;
                                background: #e9ecef;
                                border-radius: 4px;
                                text-align: center;
                            }
                            .merge-stats {
                                margin-top: 20px;
                                padding: 15px;
                                background: #fff;
                                border: 1px solid #dee2e6;
                                border-radius: 4px;
                            }
                            .merge-stats h4 {
                                margin-top: 0;
                                margin-bottom: 10px;
                                color: #343a40;
                            }
                            .stats-content p {
                                margin: 5px 0;
                                display: flex;
                                justify-content: space-between;
                            }
                            .stats-content .success {
                                color: #28a745;
                                font-weight: bold;
                            }
                            .stats-content .error {
                                color: #dc3545;
                                font-weight: bold;
                            }
                        `;
                        document.head.appendChild(style);
                    }
                } else {
                    toast(`同步失败: ${data.message || '未知错误'}`);
                }
            } catch (error) {
                console.error('股票数据同步错误:', error);
                toast(`同步失败: ${error.message}`);
            } finally {
                // 恢复按钮状态
                startBtn.disabled = false;
                startBtn.innerHTML = '开始同步';
            }
        });
    }
})();

// 确保页面加载完成后初始化
if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', function() {
        // 页面加载完成后重新初始化股票同步按钮
        const initStockSyncFunction = window.initStockSyncButton;
        if (typeof initStockSyncFunction === 'function') {
            initStockSyncFunction();
        }
        
        // 初始化申万数据同步按钮
        const startSwSyncBtn = document.getElementById('startSwSyncBtn');
        if (startSwSyncBtn) {
            startSwSyncBtn.addEventListener('click', async () => {
                // 获取输入框中的路径
                const mainDirInput = document.getElementById('mainDirectoryInput');
                const appendDirInput = document.getElementById('appendDirectoryInput');
                
                if (!mainDirInput || !appendDirInput) {
                    toast('找不到路径输入框');
                    return;
                }
                
                const mainDir = mainDirInput.value.trim();
                const appendDir = appendDirInput.value.trim();
                
                // 验证输入
                if (!mainDir) {
                    toast('请输入主目录路径');
                    return;
                }
                
                if (!appendDir) {
                    toast('请输入追加目录路径');
                    return;
                }
                
                // 禁用按钮，显示加载状态
                startSwSyncBtn.disabled = true;
                startSwSyncBtn.innerHTML = '<i class="ri-loader-2-line ri-spin"></i> 同步中...';
                
                // 移除进度条，直接处理和显示结果
                
                try {
                    // 调用后端API - 申万指数合并专用
                    const response = await fetch(`${API_BASE}/api/restore/sw_merge`, {
                        method: 'POST',
                        headers: {
                            'Content-Type': 'application/json',
                        },
                        body: JSON.stringify({
                            main_path: mainDir,
                            append_path: appendDir
                        })
                    });
                    
                    if (!response.ok) {
                        throw new Error(`HTTP error! status: ${response.status}`);
                    }
                    
                    const data = await response.json();
                    
                    if (data.success) {
                        toast(`申万数据同步成功！找到 ${data.total_count} 个文件`);
                        
                        // 创建或更新结果显示区域
                        let resultContainer = document.getElementById('swSyncResult');
                        if (!resultContainer) {
                            // 创建结果显示容器
                            resultContainer = document.createElement('div');
                            resultContainer.id = 'swSyncResult';
                            resultContainer.className = 'mt-4 p-4 bg-light rounded shadow';
                            
                            // 插入到按钮下方
                            startSwSyncBtn.parentNode.appendChild(resultContainer);
                        }
                        
                        // 构建结果内容
                        resultContainer.innerHTML = `
                            <h4 class="mb-2">同步结果</h4>
                            <div class="stats-container mb-3">
                                <span class="stat-item">主目录文件数: <span class="stat-value">${data.main_dir_file_count || data.main_path_count}</span></span>
                                <span class="stat-item">追加目录文件数: <span class="stat-value">${data.append_dir_file_count || data.append_path_count}</span></span>
                                <span class="stat-item">合并后文件数: <span class="stat-value">${data.merged_file_count || data.total_count}</span></span>
                            </div>
                            <div class="mb-3">
                                <h5>股票代码列表:</h5>
                                <div class="code-grid stats-container overflow-auto" style="max-height: 150px; font-size: 11px; line-height: 1.4; white-space: pre-wrap; word-break: break-all; margin-top: 10px;">
                                    ${data.formatted_stock_codes || (data.stock_codes && Array.isArray(data.stock_codes) ? 
                                      data.stock_codes.slice(0, 10).map(code => `<span class="code-tag">${code}</span>`).join(' ') + 
                                      (data.stock_codes.length > 10 ? ` <span class="code-more">... 等${data.stock_codes.length - 10}个</span>` : '') 
                                      : '')}
                                </div>
                            </div>
                            <button id="startSwMergeAllBtn" class="btn btn-primary">开始合并所有股票数据</button>
                            <div class="mt-3">
                                <div class="bg-light p-2 border rounded mb-2">
                                    <h6 class="mb-1">合并日志:</h6>
                                    <div id="swMergeLog" class="bg-white p-2 border rounded overflow-auto" style="max-height: 200px;">
                                        <!-- 日志列表将在这里动态生成 -->
                                    </div>
                                </div>
                            </div>
                        `;
                        
                        // 添加合并按钮事件监听
                        const mergeAllBtn = document.getElementById('startSwMergeAllBtn');
                        if (mergeAllBtn) {
                            mergeAllBtn.addEventListener('click', () => {
                                // 这里可以添加合并所有数据的逻辑
                                // 首先清空并设置日志区域
                                const logArea = document.getElementById('swMergeLog');
                                // 修改为ul列表（如果HTML中仍然是div，这里动态修改为ul）
                                logArea.outerHTML = `<ul id="swMergeLog" class="log-list" style="list-style: none; padding: 0; margin: 0; max-height: 300px; overflow-y: auto;"></ul>`;
                                const logList = document.getElementById('swMergeLog');
                                
                                // 添加进度条容器
                                const progressContainer = document.createElement('div');
                                progressContainer.id = 'swMergeProgress';
                                progressContainer.style.width = '100%';
                                progressContainer.style.height = '20px';
                                progressContainer.style.backgroundColor = '#f0f0f0';
                                progressContainer.style.borderRadius = '4px';
                                progressContainer.style.marginBottom = '10px';
                                progressContainer.style.overflow = 'hidden';
                                
                                // 添加进度条
                                const progressBar = document.createElement('div');
                                progressBar.id = 'swMergeProgressBar';
                                progressBar.style.width = '0%';
                                progressBar.style.height = '100%';
                                progressBar.style.backgroundColor = '#4CAF50';
                                progressBar.style.transition = 'width 0.3s ease';
                                
                                // 添加进度文本
                                const progressText = document.createElement('div');
                                progressText.id = 'swMergeProgressText';
                                progressText.style.position = 'absolute';
                                progressText.style.width = '100%';
                                progressText.style.textAlign = 'center';
                                progressText.style.lineHeight = '20px';
                                progressText.style.fontSize = '12px';
                                progressText.style.color = '#333';
                                progressText.textContent = '0/0 (0%)';
                                
                                progressContainer.appendChild(progressBar);
                                progressContainer.appendChild(progressText);
                                
                                // 在日志列表前插入进度条
                                logList.parentNode.insertBefore(progressContainer, logList);
                                
                                // 添加初始日志
                                const initialLog = document.createElement('li');
                                initialLog.className = 'log-item log-info';
                                initialLog.textContent = '开始合并所有数据...';
                                logList.appendChild(initialLog);
                                logList.scrollTop = logList.scrollHeight;
                                
                                // 显示处理中状态
                                mergeAllBtn.disabled = true;
                                mergeAllBtn.innerHTML = '<i class="ri-loader-2-line ri-spin"></i> 合并中...';
                                
                                // 实际调用API进行单个指数合并
                                let processedCount = 0;
                                let successCount = 0; // 修改为let以便更新
                                let failedCount = 0;
                                const totalCount = data.total_count;
                                const successCodes = []; // 存储成功处理的代码
                                
                                const processNextItem = async () => {
                                    if (processedCount < totalCount) {
                                            const stockCode = data.stock_codes[processedCount];
                                            
                                            // 更新进度条
                                            const progressPercentage = Math.round((processedCount / totalCount) * 100);
                                            const progressBar = document.getElementById('swMergeProgressBar');
                                            const progressText = document.getElementById('swMergeProgressText');
                                            if (progressBar && progressText) {
                                                progressBar.style.width = `${progressPercentage}%`;
                                                // 修复进度文本显示，确保与进度条同步
                                                progressText.textContent = `${processedCount}/${totalCount} (${progressPercentage}%)`;
                                                // 确保进度文本正确定位
                                                progressText.style.position = 'absolute';
                                                progressText.style.width = '100%';
                                                progressText.style.textAlign = 'center';
                                                progressText.style.lineHeight = '20px';
                                                progressText.style.fontSize = '12px';
                                                progressText.style.color = '#333';
                                                progressText.style.zIndex = '10';
                                            }
                                            
                                            processedCount++;
                                        
                                        if (logList) {
                                            const logItem = document.createElement('li');
                                            logItem.className = 'log-item log-processing';
                                            logItem.innerHTML = `正在合并 <span style="font-weight: bold;">${stockCode}</span>`;
                                            logList.appendChild(logItem);
                                            logList.scrollTop = logList.scrollHeight;
                                        }
                                        
                                        try {
                                            // 调用单个指数合并API
                                            const response = await fetch(`${API_BASE}/api/restore/sw_mergeItem`, {
                                                method: 'POST',
                                                headers: {
                                                    'Content-Type': 'application/json',
                                                },
                                                body: JSON.stringify({
                                                    main_path: mainDir,
                                                    append_path: appendDir,
                                                    code: stockCode
                                                })
                                            });
                                            
                                            if (response.ok) {
                                                const result = await response.json();
                                                if (logList) {
                                                const resultLog = document.createElement('li');
                                                if (result.success) {
                                                    resultLog.className = 'log-item log-success';
                                                    resultLog.textContent = `✅ 成功: ${stockCode} - ${result.message || '合并成功'}`;
                                                    successCount++;
                                                    successCodes.push(stockCode);
                                                    // 可选：添加可展开的详情
                                                    if (result.data) {
                                                        resultLog.style.cursor = 'pointer';
                                                        const details = document.createElement('div');
                                                        details.className = 'log-details';
                                                        details.style.display = 'none';
                                                        details.style.paddingLeft = '20px';
                                                        details.style.fontSize = '12px';
                                                        details.style.color = '#666';
                                                        // 格式化显示详细数据
                                                        const stats = [];
                                                        if (result.data.total_rows !== undefined) stats.push(`总行数: ${result.data.total_rows}`);
                                                        if (result.data.new_rows !== undefined) stats.push(`新增: ${result.data.new_rows}`);
                                                        if (result.data.duplicate_rows !== undefined) stats.push(`重复: ${result.data.duplicate_rows}`);
                                                        details.textContent = stats.join(', ');
                                                        resultLog.appendChild(details);
                                                        // 添加点击展开/收起事件
                                                        resultLog.addEventListener('click', () => {
                                                            details.style.display = details.style.display === 'none' ? 'block' : 'none';
                                                        });
                                                    }
                                                } else {
                                                    resultLog.className = 'log-item log-error';
                                                    resultLog.textContent = `❌ 失败: ${stockCode} - ${result.message || '合并失败'}`;
                                                    failedCount++;
                                                }
                                                logList.appendChild(resultLog);
                                                logList.scrollTop = logList.scrollHeight;
                                            }
                                            } else {
                                                if (logList) {
                                                const errorLog = document.createElement('li');
                                                errorLog.className = 'log-item log-error';
                                                errorLog.textContent = `❌ 错误: ${stockCode} - 请求失败 (${response.status})`;
                                                failedCount++;
                                                logList.appendChild(errorLog);
                                                logList.scrollTop = logList.scrollHeight;
                                            }
                                            }
                                        } catch (error) {
                                            console.error(`合并${stockCode}时出错:`, error);
                                            if (logList) {
                                                const errorLog = document.createElement('li');
                                                errorLog.className = 'log-item log-error';
                                                errorLog.textContent = `❌ 异常: ${stockCode} - ${error.message}`;
                                                failedCount++;
                                                logList.appendChild(errorLog);
                                                logList.scrollTop = logList.scrollHeight;
                                            }
                                        }
                                        
                                        // 继续处理下一个项目
                                        setTimeout(processNextItem, 100); // 短暂延迟避免请求过于密集
                                    } else {
                                            // 更新最终进度条
                                            const progressBar = document.getElementById('swMergeProgressBar');
                                            const progressText = document.getElementById('swMergeProgressText');
                                            if (progressBar && progressText) {
                                                progressBar.style.width = '100%';
                                                progressBar.style.backgroundColor = successCount === totalCount ? '#4CAF50' : '#FF9800';
                                                progressText.textContent = `${processedCount}/${totalCount} (100%)`;
                                                // 确保进度文本正确定位
                                                progressText.style.position = 'absolute';
                                                progressText.style.width = '100%';
                                                progressText.style.textAlign = 'center';
                                                progressText.style.lineHeight = '20px';
                                                progressText.style.fontSize = '12px';
                                                progressText.style.color = '#333';
                                                progressText.style.zIndex = '10';
                                            }
                                            
                                            if (logList) {
                                            const finalLog = document.createElement('li');
                                            finalLog.className = 'log-item log-success log-final';
                                            finalLog.textContent = '所有数据合并处理完成！';
                                            
                                            // 添加完成统计信息
                                            const statsSummary = document.createElement('div');
                                            statsSummary.className = 'log-summary';
                                            statsSummary.style.fontSize = '14px';
                                            statsSummary.style.marginTop = '8px';
                                            statsSummary.style.padding = '8px';
                                            statsSummary.style.backgroundColor = '#f0f9ff';
                                            statsSummary.style.borderRadius = '4px';
                                            statsSummary.innerHTML = `处理总数: ${totalCount}, 成功: ${successCount}, 失败: ${failedCount}`;
                                            
                                            // 添加成功处理的代码列表，放入滚动容器中
                                            if (successCodes.length > 0) {
                                                const codesSummary = document.createElement('div');
                                                codesSummary.className = 'codes-summary';
                                                codesSummary.style.fontSize = '12px';
                                                codesSummary.style.marginTop = '4px';
                                                codesSummary.style.padding = '4px';
                                                codesSummary.style.backgroundColor = '#e8f5e8';
                                                codesSummary.style.borderRadius = '4px';
                                                 
                                                // 创建一个有滚动功能的容器
                                                const codesContainer = document.createElement('div');
                                                codesContainer.className = 'code-grid stats-container';
                                                codesContainer.style.maxHeight = '150px';
                                                codesContainer.style.overflowY = 'auto';
                                                codesContainer.style.fontSize = '11px';
                                                codesContainer.style.lineHeight = '1.4';
                                                codesContainer.style.whiteSpace = 'pre-wrap'; // 允许长文本换行
                                                codesContainer.style.wordBreak = 'break-all'; // 允许单词断开
                                                 
                                                // 显示部分代码，剩余用省略号
                                                const displayCount = Math.min(10, successCodes.length);
                                                const displayCodes = successCodes.slice(0, displayCount).map(code => `<span class="code-tag">${code}</span>`).join(' ');
                                                const remainingCount = successCodes.length - displayCount;
                                                 
                                                codesContainer.innerHTML = displayCodes + (remainingCount > 0 ? ` <span class="code-more">... 等${remainingCount}个</span>` : '');
                                                
                                                 
                                                // 添加点击展开/收起功能
                                                codesSummary.style.cursor = 'pointer';
                                                codesSummary.innerHTML = `正常处理的代码: <strong>${successCodes.length}</strong>个`;
                                                codesSummary.appendChild(codesContainer);
                                                codesSummary.addEventListener('click', function() {
                                                    if (remainingCount > 0) {
                                                        codesContainer.innerHTML = successCodes.map(code => `<span class="code-tag">${code}</span>`).join(' ');
                                                        codesSummary.removeEventListener('click', arguments.callee);
                                                    }
                                                });
                                                
                                                statsSummary.appendChild(codesSummary);
                                            }
                                            
                                            logList.appendChild(finalLog);
                                            logList.appendChild(statsSummary);
                                            logList.scrollTop = logList.scrollHeight;
                                        }
                                        mergeAllBtn.disabled = false;
                                        mergeAllBtn.innerHTML = '开始合并所有股票数据';
                                    }
                                };
                                
                                // 开始处理第一个项目
                                processNextItem();
                            });
                        }
                    } else {
                        toast(`申万数据同步失败: ${data.message || '未知错误'}`);
                    }
                } catch (error) {
                    console.error('申万数据同步错误:', error);
                    toast(`同步失败: ${error.message}`);
                } finally {
                    // 恢复按钮状态
                    startSwSyncBtn.disabled = false;
                    startSwSyncBtn.innerHTML = '开始同步';
                }
            });
        }
    });
}

