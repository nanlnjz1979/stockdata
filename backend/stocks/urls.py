from django.urls import path, include
from rest_framework.routers import DefaultRouter
from views.views import StockBasicViewSet, StockFinanceViewSet, DataStatusView, UpdateStatusView, UpdateFullView, QueueUpdatePauseView, QueueUpdateResumeView, QueueUpdateStopView, QueueUpdateStartView
from views.task_list_view import TaskListView
from views.sw_industry_api import SWIndustryDataAPI, SWIndustryClassificationAPI, SWThirdLevelIndustryCodesAPI
from views.integrity_views import DataIntegrityCheckView, FullIntegrityCheckView, CSVStdCheck, StockFormatStandardizationView
from views.taskview import TaskMonitorView, QTaskListView, ScheduleListView, RecentTasksView
from .data.daily import DailyDataView
from .data.basic import StocksSHView, StocksSZView, StocksBJView
from .data.dragon_tiger import DragonTigerView
from views.heatmap import HeatmapDataView
from views.config_views import ScheduleConfigView, ScheduleApplyView
from views.restore_backup import GetRestoreStockFiles, RestoreStockData, MergeStockData, MergeStockItem, SwMergeData, MergeSWIndexData
from views.adjust_factor_views import AdjustFactorUpdateView, AdjustFactorStatusView
from views.financial_data_views import FinancialDataUpdateView, FinancialDataStatusView, FinancialDataPauseView, FinancialDataResumeView
from views.index_components_views import IndexComponentsUpdateView, IndexComponentsStatusView, IndexComponentsPauseView, IndexComponentsResumeView, GetAllIndicesView
router = DefaultRouter()
router.register(r'stocks/basic', StockBasicViewSet, basename='stocks-basic')
router.register(r'stocks/finance', StockFinanceViewSet, basename='stocks-finance')
# UserFollow路由已移除
urlpatterns = [
    path('', include(router.urls)),
    # 实时行情API已移除
    path('stocks/status', DataStatusView.as_view()),
    path('stocks/update/status', UpdateStatusView.as_view()),
    # UpdateRunView已移除
    path('stocks/update/full', UpdateFullView.as_view()),

    path('stocks/update/queue/pause', QueueUpdatePauseView.as_view()),
    path('stocks/update/queue/resume', QueueUpdateResumeView.as_view()),
    path('stocks/update/queue/stop', QueueUpdateStopView.as_view()),
    path('stocks/update/queue/start', QueueUpdateStartView.as_view()),
    path('stocks/tasks/list', TaskListView.as_view()),
    path('stocks/tasks/retry', TaskListView.as_view()),
    path('configs/schedule', ScheduleConfigView.as_view()),
    path('configs/schedule/apply', ScheduleApplyView.as_view()),  # 立即生效API
    
    #  任务监控相关API
    path('tasks/monitor', TaskMonitorView.as_view()),           # 任务统计
    path('tasks/list', QTaskListView.as_view()),                 # 任务列表
    path('tasks/schedules', ScheduleListView.as_view()),        # 调度任务列表
    path('tasks/recent', RecentTasksView.as_view()),            # 最近任务
    
    # 数据完整性检查相关API
    path('stocks/integrity/check', DataIntegrityCheckView.as_view()),  # 完整性检查统计和单股票检查
    path('stocks/integrity/full', FullIntegrityCheckView.as_view()),     # 完整的完整性检查
    path('stocks/format/standardization',  StockFormatStandardizationView.as_view() ),    # 这个替换成原来的

    path('stocks/integrity/csv_check', CSVStdCheck.as_view() ),    # CSV文件数据的格式标准化检查
    
    #股票数据相关API，用来给qweshare的接口调用
    path('stocks/data/daily', DailyDataView.as_view()),# 日线数据
    path('stocks/data/basic/sh', StocksSHView.as_view()),# 上证基础数据
    path('stocks/data/basic/sz', StocksSZView.as_view()),# 深证基础数据
    path('stocks/data/basic/bj', StocksBJView.as_view()),# 北京基础数据
    # 龙虎榜数据相关API
    path('stocks/data/dragon_tiger', DragonTigerView.as_view()),# 龙虎榜列表数据
    path('stocks/data/heatmap', HeatmapDataView.as_view()),# 热力图数据
    # 龙虎榜详细数据API已移除
    # 申万行业数据API
    path('stocks/sw/generate', SWIndustryDataAPI.as_view()),# 生成申万行业分类数据
    path('stocks/sw/classification', SWIndustryClassificationAPI.as_view()),# 查询申万行业分类数据
    path('stocks/sw/third_level_industry_codes', SWThirdLevelIndustryCodesAPI.as_view()),# 获取三级行业成分股
    
      # 股票文件处理相关API

    path('restore/get_stock_files/', GetRestoreStockFiles.as_view()),
    path('restore/get_sw_files/', GetRestoreStockFiles.as_view()),
    path('restore/process/', RestoreStockData.as_view()),

    path('restore/merge/', MergeStockData.as_view()),
    path('restore/mergeItem/', MergeStockItem.as_view()),

    path('restore/sw_merge/', SwMergeData.as_view()),
    path('restore/sw_mergeItem/', MergeSWIndexData.as_view()),
    
    # 复权因子更新API
    path('stocks/update/adjust_factor/start', AdjustFactorUpdateView.as_view()),# 更新复权因子
    path('stocks/update/adjust_factor/status', AdjustFactorStatusView.as_view()),# 获取复权因子更新状态
    path('stocks/update/adjust_factor/pause', QueueUpdatePauseView.as_view()),# 暂停复权因子更新
    path('stocks/update/adjust_factor/resume', QueueUpdateResumeView.as_view()),# 恢复复权因子更新
    
    # 金融数据更新API
    path('stocks/update/financial_data/start', FinancialDataUpdateView.as_view()),# 更新金融数据
    path('stocks/update/financial_data/status', FinancialDataStatusView.as_view()),# 获取金融数据更新状态
    path('stocks/update/financial_data/pause', FinancialDataPauseView.as_view()),# 暂停金融数据更新
    path('stocks/update/financial_data/resume', FinancialDataResumeView.as_view()),# 恢复金融数据更新
    
    # 指数成份更新API
    path('stocks/update/index_components/start', IndexComponentsUpdateView.as_view()),# 更新指数成份
    path('stocks/update/index_components/status', IndexComponentsStatusView.as_view()),# 获取指数成份更新状态
    path('stocks/update/index_components/pause', IndexComponentsPauseView.as_view()),# 暂停指数成份更新
    path('stocks/update/index_components/resume', IndexComponentsResumeView.as_view()),# 恢复指数成份更新
    path('stocks/update/index_components/all', GetAllIndicesView.as_view()),# 获取所有指数列表
]