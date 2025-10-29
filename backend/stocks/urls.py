from django.urls import path, include
from rest_framework.routers import DefaultRouter
from .views import StockBasicViewSet, StockFinanceViewSet, UserFollowViewSet, QuotePlaceholderView, DataStatusView, UpdateStatusView, UpdateRunView, UpdateFullView, UpdatePauseView, UpdateResumeView, UpdateStopView, TaskListView, QueueUpdateStartView, QueueUpdatePauseView, QueueUpdateResumeView, QueueUpdateStopView
from .data.daily import DailyDataView
from .data.basic import StocksSHView, StocksSZView, StocksBJView

router = DefaultRouter()
router.register(r'stocks/basic', StockBasicViewSet, basename='stocks-basic')
router.register(r'stocks/finance', StockFinanceViewSet, basename='stocks-finance')
router.register(r'user/follow', UserFollowViewSet, basename='user-follow')

urlpatterns = [
    path('', include(router.urls)),
    path('stocks/quotes', QuotePlaceholderView.as_view()),
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
    
    path('stocks/data/daily', DailyDataView.as_view()),# 日线数据
    path('stocks/data/basic/sh', StocksSHView.as_view()),# 上证基础数据
    path('stocks/data/basic/sz', StocksSZView.as_view()),# 深证基础数据
    path('stocks/data/basic/bj', StocksBJView.as_view()),# 北京基础数据
]