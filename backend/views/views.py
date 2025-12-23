from rest_framework import viewsets, status
from rest_framework.views import APIView
from rest_framework.decorators import action
from rest_framework.response import Response
from django.conf import settings
from django.utils import timezone
from rest_framework import status
import os
from pathlib import Path

from stocks.models import StockBasic, StockFinance
from stocks.serializers import StockBasicSerializer, StockFinanceSerializer


class StockBasicViewSet(viewsets.ModelViewSet):
    queryset = StockBasic.objects.all()
    serializer_class = StockBasicSerializer

    @action(detail=False, methods=['get'])
    def search(self, request):
        q = request.query_params.get('q', '')
        qs = StockBasic.objects.filter(stock_name__icontains=q) | StockBasic.objects.filter(stock_code__icontains=q)
        return Response(StockBasicSerializer(qs[:50], many=True).data)


class StockFinanceViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = StockFinance.objects.all()
    serializer_class = StockFinanceSerializer

    @action(detail=False, methods=['get'])
    def by_code(self, request):
        code = request.query_params.get('code')
        if not code:
            return Response({'detail': 'code required'}, status=status.HTTP_400_BAD_REQUEST)
        qs = StockFinance.objects.filter(stock__stock_code=code).order_by('-report_date')
        return Response(StockFinanceSerializer(qs, many=True).data)


# 占位：按时间范围查询历史K线（后续接入TimescaleDB）


import threading
import time

# 导入队列更新相关功能
from stocks.tasks.QueueUpdateTask import get_queue_ctrl

_update_ctrl = {
    'thread': None,
    'stop_event': threading.Event(),
    'state': {
        'running': False,
        'paused': False,
        'stopped': False,
        'updated_count': 0,
        'total_codes': 0,
        'current_code': None,
        'started_at': None,
        'ended_at': None,
    }
}

def _start_full_update_thread():
    if _update_ctrl['thread'] and _update_ctrl['state']['running']:
        return False
    _update_ctrl['stop_event'].clear()
    _update_ctrl['state'].update({
        'running': True,
        'paused': False,
        'stopped': False,
        'updated_count': 0,
        'total_codes': 0,
        'current_code': None,
        'started_at': timezone.now(),
        'ended_at': None,
    })
    from stocks.tasks import DownloadDailyTask, QtasksOrm
    
    def worker():
        try:
           
            from global_config.stock_info import StockInfo

            basics = StockInfo.get_all_stocks()    #基础股票代码库
            _update_ctrl['state']['total_codes'] = len(basics)
            
            orm = QtasksOrm()
            task = DownloadDailyTask(orm)
            for item in basics:     #基础库中的一条记录
                # 检查是否需要停止
                if _update_ctrl['stop_event'].is_set():
                    break
                
                # 检查是否暂停
                while _update_ctrl['state']['paused']:
                    if _update_ctrl['stop_event'].is_set():
                        break
                    time.sleep(0.2)
                if _update_ctrl['stop_event'].is_set():
                    break
                
                code = item.get('code')     #股票代码
                if not code:
                    continue
                
                market = item.get('market') #市场上海，深圳，北京
                listing_date = item.get('listing_date')#上市时间
                if not listing_date:
                    start_date = '19841118'             #如果上市时间没有，则默认是19841118，中国最早股票上市日期
                else:
                    try:
                        if hasattr(listing_date, 'strftime'):
                            start_date = listing_date.strftime("%Y%m%d")
                        else:
                            s = str(listing_date)
                            start_date = s.replace('-', '')
                    except Exception:
                        start_date = '19841118'
                
                # 更新当前处理的股票代码
                _update_ctrl['state']['current_code'] = code
                
                # 只生成任务，不执行具体下载和更新
                task.generate("Download_Full_Daily", f"Download daily data for {code}", {"code": code, "start_date": start_date, "end_date": timezone.now().strftime("%Y%m%d"), "market": market ,"adjust": "all"}, priority=0)
                
                # 更新状态计数
                _update_ctrl['state']['updated_count'] += 1
                time.sleep(0.01)  # 避免过快生成任务
        finally:
            _update_ctrl['state']['running'] = False
            _update_ctrl['state']['stopped'] = _update_ctrl['stop_event'].is_set()
            _update_ctrl['state']['ended_at'] = timezone.now()
            _update_ctrl['thread'] = None
    t = threading.Thread(target=worker, daemon=True)
    _update_ctrl['thread'] = t
    t.start()
    return True

# 队列更新 API：从任务列表取任务执行
# 从QueueUpdateTask导入队列更新视图
from stocks.tasks.QueueUpdateTask import QueueUpdateStartView

class UpdateStatusView(APIView):
    
    def get(self, request):
        
        from backend.db.db_pool import get_conn, put_conn, get_pool_stats
        from global_config.stock_info import StockInfo

        qdb_ok = False
        qdb_error = None
        stock_basic_count = 0
        conn = None

        try:
            # 使用默认连接池获取连接
            conn = get_conn()
            # ClickHouse客户端直接支持execute方法，不需要cursor
            # 执行简单查询验证连接
            conn.execute("SELECT 1")
            qdb_ok = True
            # 使用StockInfo获取股票数量
            stock_basic_count = StockInfo.get_stock_count()
        except Exception as e:
            qdb_error = str(e)
        finally:
            # 确保连接被正确归还
            if conn:
                put_conn(conn)

        # 获取连接池状态
        pool_stats = get_pool_stats()

        # 控制器状态
        ctrl = _update_ctrl['state'].copy()

        # 队列控制器状态
        queue_ctrl = get_queue_ctrl()['state'].copy()

        return Response({
            'stock_basic_count': stock_basic_count,
            'controller': ctrl,
            'queue_controller': queue_ctrl,
            'questdb': {
                'connected': qdb_ok,
                'error': qdb_error,
            },
            'connection_pool': pool_stats
        })

class UpdateFullView(APIView):
    def post(self, request):
        """触发全量更新：可暂停/继续/停止。若QuestDB连接失败，返回错误。"""
        # 在启动线程前快速检查QuestDB连接，失败则直接返回错误
        

        started = _start_full_update_thread()
        return Response({
            'started': started,
            'started_at': _update_ctrl['state']['started_at'],
            'total_codes': _update_ctrl['state']['total_codes'],
            'note': '后台执行 akshare 全量更新（日线，可暂停/继续/停止）'
        })


class QueueUpdatePauseView(APIView):
    def post(self, request):
        queue_ctrl = get_queue_ctrl()
        if queue_ctrl['state']['running'] and not queue_ctrl['state']['paused']:
            queue_ctrl['state']['paused'] = True
        return Response({'running': queue_ctrl['state']['running'], 'paused': queue_ctrl['state']['paused']})

class QueueUpdateResumeView(APIView):
    def post(self, request):
        queue_ctrl = get_queue_ctrl()
        if queue_ctrl['state']['running'] and queue_ctrl['state']['paused']:
            queue_ctrl['state']['paused'] = False
        return Response({'running': queue_ctrl['state']['running'], 'paused': queue_ctrl['state']['paused']})

class QueueUpdateStopView(APIView):
    def post(self, request):
        queue_ctrl = get_queue_ctrl()
        if queue_ctrl['state']['running']:
            queue_ctrl['stop_event'].set()
        return Response({'running': queue_ctrl['state']['running'], 'stopped': queue_ctrl['state']['stopped']})


# QuotePlaceholderView已移除，实时行情功能已取消

class DataStatusView(APIView):
    def get(self, request):
        import os
        import psycopg2

        trends = []

        return Response({
            'trends': trends,
        })
