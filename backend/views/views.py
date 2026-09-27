from rest_framework import viewsets, status
from rest_framework.views import APIView
from rest_framework.decorators import action
from rest_framework.response import Response
from django.conf import settings
from django.utils import timezone
from rest_framework import status
import os
from pathlib import Path
import logging

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

logger = logging.getLogger(__name__)

# 导入队列更新相关功能
from stocks.tasks.QueueUpdateTask import get_queue_ctrl, get_queue_performance, reconcile_queue_state, request_queue_stop

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
        return {
            'started': False,
            'already_running': True,
            'message': '全量任务正在生成中，请不要重复点击',
        }

    from stocks.tasks import DownloadDailyTask, QtasksOrm

    try:
        orm = QtasksOrm.get_instance()
        pending_full_count = orm.count_tasks(
            status="待处理",
            task_type=DownloadDailyTask.taskID(),
        )
        processing_full_count = orm.count_tasks(
            status="处理中",
            task_type=DownloadDailyTask.taskID(),
        )
    except Exception as e:
        logger.error("检查已有全量更新任务失败: %s", e)
        pending_full_count = 0
        processing_full_count = 0

    existing_full_count = pending_full_count + processing_full_count
    if existing_full_count > 0:
        logger.info(
            "[全量任务生成] 已有任务待处理或处理中，跳过重复生成 pending=%d processing=%d",
            pending_full_count,
            processing_full_count,
        )
        return {
            'started': False,
            'already_exists': True,
            'message': f'已有 {existing_full_count} 个全量更新任务待处理/处理中，无需重复生成',
            'pending_count': pending_full_count,
            'processing_count': processing_full_count,
            'total_codes': existing_full_count,
        }

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
    
    def worker():
        try:
           
            from global_config.stock_info import StockInfo

            basics = StockInfo.get_all_stocks()    #基础股票代码库
            _update_ctrl['state']['total_codes'] = len(basics)
            logger.info("[全量任务生成] 开始生成 code_count=%d", len(basics))
            
            orm = QtasksOrm.get_instance()
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
            logger.info(
                "[全量任务生成] 完成 generated=%d total=%d stopped=%s",
                _update_ctrl['state']['updated_count'],
                _update_ctrl['state']['total_codes'],
                _update_ctrl['stop_event'].is_set(),
            )
        except Exception as e:
            logger.exception("[全量任务生成] 失败: %s", e)
        finally:
            _update_ctrl['state']['running'] = False
            _update_ctrl['state']['stopped'] = _update_ctrl['stop_event'].is_set()
            _update_ctrl['state']['ended_at'] = timezone.now()
            _update_ctrl['thread'] = None
    t = threading.Thread(target=worker, daemon=True)
    _update_ctrl['thread'] = t
    t.start()
    return {
        'started': True,
        'message': '全量更新任务开始生成',
    }

# 队列更新 API：从任务列表取任务执行
# 从QueueUpdateTask导入队列更新视图
from stocks.tasks.QueueUpdateTask import QueueUpdateStartView

class UpdateStatusView(APIView):
    
    def get(self, request):
        
        from backend.db.db_pool import get_conn, put_conn, get_pool_stats
        from global_config.stock_info import StockInfo
        from stocks.tasks import QtasksOrm

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
        try:
            orm = QtasksOrm.get_instance()
            pending_count = orm.count_tasks(status="待处理")
            processing_count = orm.count_tasks(status="处理中")
        except Exception:
            pending_count = 0
            processing_count = len(queue_ctrl.get('current_codes') or [])

        if reconcile_queue_state(pending_count=pending_count, processing_count=processing_count):
            queue_ctrl = get_queue_ctrl()['state'].copy()
            try:
                pending_count = orm.count_tasks(status="待处理")
                processing_count = orm.count_tasks(status="处理中")
            except Exception:
                pending_count = 0
                processing_count = len(queue_ctrl.get('current_codes') or [])

        updated_count = queue_ctrl.get('updated_count') or 0
        dynamic_total = updated_count + pending_count + processing_count
        queue_ctrl['pending_count'] = pending_count
        queue_ctrl['processing_count'] = processing_count
        queue_ctrl['total_codes'] = max(queue_ctrl.get('total_codes') or 0, dynamic_total)
        queue_ctrl.update(get_queue_performance(
            pending_count=pending_count,
            processing_count=processing_count,
        ))
        if not queue_ctrl.get('running') and queue_ctrl.get('stopped') and pending_count > 0:
            queue_ctrl['message'] = f'队列已停止，剩余 {pending_count} 个待处理，点击启动可继续'
        elif not queue_ctrl.get('running') and pending_count == 0 and (queue_ctrl.get('updated_count') or 0) > 0:
            queue_ctrl['message'] = '队列执行完成'

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
        

        result = _start_full_update_thread()
        started = bool(result.get('started')) if isinstance(result, dict) else bool(result)
        return Response({
            'started': started,
            'already_running': bool(result.get('already_running')) if isinstance(result, dict) else False,
            'already_exists': bool(result.get('already_exists')) if isinstance(result, dict) else False,
            'message': result.get('message') if isinstance(result, dict) else '',
            'started_at': _update_ctrl['state']['started_at'],
            'total_codes': result.get('total_codes', _update_ctrl['state']['total_codes']) if isinstance(result, dict) else _update_ctrl['state']['total_codes'],
            'pending_count': result.get('pending_count') if isinstance(result, dict) else None,
            'processing_count': result.get('processing_count') if isinstance(result, dict) else None,
            'note': '后台执行 akshare 全量更新（日线，可暂停/继续/停止）'
        })


class IncrementalTaskGenerateView(APIView):
    def post(self, request):
        """生成增量更新任务：只入队，不直接下载。"""
        from db.db_pool import get_conn, put_conn
        from stocks.tasks import IncrementalUpdateTask, QtasksOrm

        conn = None
        try:
            orm = QtasksOrm.get_instance()
            before_pending = orm.count_tasks(status="待处理", task_type=IncrementalUpdateTask.taskID())
            before_processing = orm.count_tasks(status="处理中", task_type=IncrementalUpdateTask.taskID())

            conn = get_conn()
            task = IncrementalUpdateTask(orm)
            result = task.generate(
                IncrementalUpdateTask.taskID(),
                "生成股票增量更新任务",
                params={},
                priority=1,
                conn=conn,
            )

            after_pending = orm.count_tasks(status="待处理", task_type=IncrementalUpdateTask.taskID())
            after_processing = orm.count_tasks(status="处理中", task_type=IncrementalUpdateTask.taskID())
            generated = int(result.get("generated", max(0, after_pending - before_pending)) or 0)
            return Response({
                "success": True,
                "message": f"已生成 {generated} 个增量更新任务",
                "generated": generated,
                "skipped_no_range": result.get("skipped_no_range", 0),
                "skipped_non_trading": result.get("skipped_non_trading", 0),
                "skipped_duplicate": result.get("skipped_duplicate", 0),
                "total": result.get("total", 0),
                "pending_before": before_pending,
                "pending_after": after_pending,
                "processing_before": before_processing,
                "processing_after": after_processing,
            })
        except Exception as e:
            logger.exception("[增量任务生成] API失败: %s", e)
            return Response({
                "success": False,
                "message": f"生成增量更新任务失败: {str(e)}",
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            if conn:
                put_conn(conn)


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
        result = request_queue_stop()
        queue_ctrl = get_queue_ctrl()['state']

        pending_count = 0
        processing_count = 0
        try:
            from stocks.tasks import QtasksOrm
            orm = QtasksOrm.get_instance()
            pending_count = orm.count_tasks(status="待处理")
            processing_count = orm.count_tasks(status="处理中")
        except Exception as e:
            logger.warning("[队列结束] 获取任务计数失败: %s", e)

        return Response({
            **result,
            'running': queue_ctrl['running'],
            'paused': queue_ctrl['paused'],
            'stopping': queue_ctrl.get('stopping', False),
            'pending_count': pending_count,
            'processing_count': processing_count,
        })


# QuotePlaceholderView已移除，实时行情功能已取消

class DataStatusView(APIView):
    def get(self, request):
        from backend.db.db_pool import get_pool_stats
        from global_config.stock_info import StockInfo

        trends = []

        try:
            stock_count = StockInfo.get_stock_count()
        except Exception:
            stock_count = 0

        try:
            pool_stats = get_pool_stats()
        except Exception:
            pool_stats = {}

        return Response({
            'trends': trends,
            'stock_count': stock_count,
            'connection_pool': pool_stats,
        })
