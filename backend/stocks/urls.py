from django.urls import path, include
from rest_framework.routers import DefaultRouter
from views.views import StockBasicViewSet, StockFinanceViewSet, DataStatusView, UpdateStatusView, UpdateRunView, UpdateFullView, UpdatePauseView, UpdateResumeView, UpdateStopView, TaskListView, QueueUpdateStartView, QueueUpdatePauseView, QueueUpdateResumeView, QueueUpdateStopView
from views.integrity_views import DataIntegrityCheckView, FullIntegrityCheckView, StockFormatStandardizationView
from views.taskview import TaskMonitorView, QTaskListView, ScheduleListView, RecentTasksView
from .data.daily import DailyDataView
from .data.basic import StocksSHView, StocksSZView, StocksBJView
from .data.dragon_tiger import DragonTigerView
from views.heatmap import HeatmapDataView
from views.config_views import ScheduleConfigView, ScheduleApplyView

router = DefaultRouter()
router.register(r'stocks/basic', StockBasicViewSet, basename='stocks-basic')
router.register(r'stocks/finance', StockFinanceViewSet, basename='stocks-finance')
# UserFollow路由已移除

urlpatterns = [
    path('', include(router.urls)),
    # 实时行情API已移除
    path('stocks/status', DataStatusView.as_view()),
    path('stocks/update/status', UpdateStatusView.as_view()),
    path('stocks/update/run', UpdateRunView.as_view()),
    path('stocks/update/full', UpdateFullView.as_view()),
    path('stocks/update/pause', UpdatePauseView.as_view()),
    path('stocks/update/resume', UpdateResumeView.as_view()),
    path('stocks/update/stop', UpdateStopView.as_view()),
    path('stocks/update/queue/start', QueueUpdateStartView.as_view()),
    path('stocks/update/queue/pause', QueueUpdatePauseView.as_view()),
    path('stocks/update/queue/resume', QueueUpdateResumeView.as_view()),
    path('stocks/update/queue/stop', QueueUpdateStopView.as_view()),
    path('stocks/tasks', TaskListView.as_view()),
    path('configs/schedule', ScheduleConfigView.as_view()),
    path('configs/schedule/apply', ScheduleApplyView.as_view()),  # 立即生效API
    
    # 任务监控相关API
    path('tasks/monitor', TaskMonitorView.as_view()),           # 任务统计
    path('tasks/list', QTaskListView.as_view()),                 # 任务列表
    path('tasks/schedules', ScheduleListView.as_view()),        # 调度任务列表
    path('tasks/recent', RecentTasksView.as_view()),            # 最近任务
    
    # 数据完整性检查相关API
    path('stocks/integrity/check', DataIntegrityCheckView.as_view()),  # 完整性检查统计和单股票检查
    path('stocks/integrity/full', FullIntegrityCheckView.as_view()),     # 完整的完整性检查
    path('stocks/format/standardization', StockFormatStandardizationView.as_view(), name='stock_format_standardization'),    # 数据格式标准化检查
    
    #股票数据相关API，用来给qweshare的接口调用
    path('stocks/data/daily', DailyDataView.as_view()),# 日线数据
    path('stocks/data/basic/sh', StocksSHView.as_view()),# 上证基础数据
    path('stocks/data/basic/sz', StocksSZView.as_view()),# 深证基础数据
    path('stocks/data/basic/bj', StocksBJView.as_view()),# 北京基础数据
    # 龙虎榜数据相关API
    path('stocks/data/dragon_tiger', DragonTigerView.as_view()),# 龙虎榜列表数据
    path('stocks/data/heatmap', HeatmapDataView.as_view()),# 热力图数据
    # 龙虎榜详细数据API已移除
]