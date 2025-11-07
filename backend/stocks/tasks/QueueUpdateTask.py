from django.conf import settings
from django.utils import timezone
from rest_framework import status
from rest_framework.response import Response
from rest_framework.views import APIView
import threading
import time
import logging
import sys
from pathlib import Path
from db.db_pool import get_conn, put_conn

# 将项目根目录添加到系统路径
project_root = Path(settings.BASE_DIR).parent
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

# 配置日志
logger = logging.getLogger(__name__)

# 队列更新独立控制器
_queue_ctrl = {
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

def get_queue_ctrl():
    """获取队列控制器（供外部使用）"""
    return _queue_ctrl

def _start_queue_update_thread():
    """启动队列更新线程"""
    if _queue_ctrl['thread'] and _queue_ctrl['state']['running']:
        return False
    _queue_ctrl['stop_event'].clear()
    _queue_ctrl['state'].update({
        'running': True,
        'paused': False,
        'stopped': False,
        'updated_count': 0,
        'total_codes': 0,
        'current_code': None,
        'started_at': timezone.now(),
        'ended_at': None,
    })

    from stocks.tasks import DTBInstTradingTrackerTask,DownloadDailyTask, QtasksOrm
    

    def worker():
        try:

            

            
            # 从连接池获取连接
            conn = get_conn()
            orm = QtasksOrm(conn)

            # 预估待处理总数
            test_task_type = DTBInstTradingTrackerTask.taskID()
            pending = orm.list_tasks(status="待处理", task_type = test_task_type ,limit=100000)
            _queue_ctrl['state']['total_codes'] = len(pending)
            # 按优先级逐个处理
            idx = 0
            while not _queue_ctrl['stop_event'].is_set():
                while _queue_ctrl['state']['paused']:
                    if _queue_ctrl['stop_event'].is_set():
                        break
                    time.sleep(0.2)
                if _queue_ctrl['stop_event'].is_set():
                    break

                # 取下一个待处理任务
                item = orm.next_pending_task(task_type = test_task_type) 
                #item = orm.next_pending_task()
                if not item:
                    break
                tid = item.get('task_id')
                taskType = item.get('task_type')
                
                try:
                    claimed = orm.claim_task(tid)#变成"处理中"
                except Exception:
                    claimed = True

                if not claimed:
                    # 未成功认领，稍后重试
                    time.sleep(0.2)
                    continue
                # 解析参数，填充当前代码便于前端显示
                try:
                    import json
                    params = json.loads(item.get('task_params') or '{}')
                except Exception:
                    params = {}

                code = params.get('code')
                if not code and isinstance(params.get('codes'), list) and params.get('codes'):
                    code = params.get('codes')[0]
                _queue_ctrl['state']['current_code'] = code or item.get('task_type')
                # 根据类型构造任务，目前支持 download_daily 和 DTBInstTradingTrackerTask
                if taskType == DownloadDailyTask.taskID():  #下载全部的数据
                    t = DownloadDailyTask( orm )
                elif taskType == DTBInstTradingTrackerTask.taskID():
                    t = DTBInstTradingTrackerTask( orm )
                else:
                    continue
                t.task_id = item.get('task_id')
                t.task_type = item.get('task_type')
                t.task_desc = item.get('task_desc')
                t.params_str = item.get('task_params') or '{}'
                t.priority = item.get('priority') or 0
                ok = False
                try:
                    ok = t.run(conn=conn)
                except Exception:
                    ok = False
                try:
                    orm.update_task_status(t.task_id, "成功" if ok else "失败")
                except Exception:
                    pass
                _queue_ctrl['state']['updated_count'] += 1
                idx += 1
                time.sleep(0.01)
            try:
                # 归还连接到连接池，而不是关闭
                put_conn(conn)
            except Exception as e:
                logger.error(f"归还连接到连接池失败: {str(e)}")
        finally:
            _queue_ctrl['state']['running'] = False
            _queue_ctrl['state']['stopped'] = _queue_ctrl['stop_event'].is_set()
            _queue_ctrl['state']['ended_at'] = timezone.now()
            _queue_ctrl['thread'] = None

    t = threading.Thread(target=worker, daemon=True)
    _queue_ctrl['thread'] = t
    t.start()
    return True

class QueueUpdateStartView(APIView):
    """队列更新启动视图"""
    def post(self, request):

        started = _start_queue_update_thread()
        return Response({
            'started': started,
            'started_at': _queue_ctrl['state']['started_at'],
            'total_codes': _queue_ctrl['state']['total_codes'],
            'note': '后台执行 任务队列更新（仅消费待处理任务，可暂停/继续/停止）'
        })