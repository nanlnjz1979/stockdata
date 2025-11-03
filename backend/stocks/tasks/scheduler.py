from datetime import datetime, timedelta
from typing import Optional

from django_q.tasks import schedule
from django_q.models import Schedule
from django.utils import timezone

from pathlib import Path
from django.conf import settings
import sys

# 动态引入项目根外的模块（data_pipeline, global_config）
project_root = Path(settings.BASE_DIR).parent
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

from global_config.config import GlobalConfig
from data_pipeline.collector import qdb_connect


def _compute_next_run_for_daily(hhmmss: str) -> datetime:
    """根据 HH:MM:SS 计算下一次运行时间（本地时区）。"""
    # 使用更安全的方式获取当前时间，确保是aware datetime
    now = timezone.now()
    try:
        hh, mm, ss = [int(x) for x in hhmmss.split(':')]
    except Exception:
        hh, mm, ss = 0, 0, 0
    today_run = now.replace(hour=hh, minute=mm, second=ss, microsecond=0)
    if today_run > now:
        return today_run
    return today_run + timedelta(days=1)


def _parse_schedule_time(val) -> tuple[datetime, int, int]:
    """
    解析 schedule_time，返回 (next_run, schedule_type, repeats)
    - 若为每日固定时间（'1970-01-01 HH:MM:SS' 或 'HH:MM[:SS]'），则使用 DAILY、repeats=-1
    - 若为具体 datetime（一次性），则使用 ONCE、repeats=1
    """
    from django_q.models import Schedule as S
    if val is None:
        return (timezone.now() + timedelta(minutes=1), S.ONCE, 1)
    # datetime
    if hasattr(val, 'strftime'):
        dt = timezone.make_aware(val) if timezone.is_naive(val) else val
        # 如果是 1970-01-01，视为每日固定时间
        if dt.date() == datetime(1970, 1, 1).date():
            hhmmss = dt.strftime('%H:%M:%S')
            return (_compute_next_run_for_daily(hhmmss), S.DAILY, -1)
        return (dt, S.ONCE, 1)
    
    s = str(val).strip()
    # 1970-01-01 HH:MM:SS 视为每日固定时间
    if s.startswith('1970-01-01 '):
        hhmmss = s.split(' ')[1]
        return (_compute_next_run_for_daily(hhmmss), S.DAILY, -1)
    # HH:MM 或 HH:MM:SS
    if ':' in s and len(s) in (5, 8):
        if len(s) == 5:
            s = s + ':00'
        return (_compute_next_run_for_daily(s), S.DAILY, -1)
    # 兜底：立即执行一次
    return (timezone.now() + timedelta(minutes=1), S.ONCE, 1)

# 集中存放所有任务对象，便于后续扩展与维护
from stocks.tasks.incremental_update import IncrementalUpdateTask
from stocks.tasks.DTBInstTradingTracker import DTBInstTradingTrackerTask
tasks = {
    'STOCK_Update': IncrementalUpdateTask,
    'LHB_InstituteTrack': DTBInstTradingTrackerTask,
}
def run_config_job(config_id: str) -> bool:
    """django-q 执行函数：根据配置ID触发对应任务。"""
    print(f'-------------------------{config_id}----------------------------')
    import json
    try:
        # 避免循环依赖，按需导入具体任务实现
        from stocks.tasks import DownloadDailyTask, QdbOrm, DTBInstTradingTrackerTask
    except Exception as e:
        print(f"[Scheduler] Import tasks failed: {e}")
        return False

    conn = None
    try:
        conn = qdb_connect()
        gc = GlobalConfig(conn)
        cfg = gc.get_schedule_config(config_id) or {}
        params_raw = cfg.get('params')
        try:
            params = json.loads(params_raw) if isinstance(params_raw, str) else (params_raw or {})
        except Exception:
            params = {}
        task_desc = cfg.get('task_desc') or cfg.get('name') or config_id

        # 将任务写入 QuestDB 的 tasks 表，并执行实际逻辑
        orm = QdbOrm(conn)
        if config_id in tasks:
            task = tasks[config_id](orm)
            task.generate(task_type=config_id, task_desc=task_desc, params=params, priority=0)
            try:
                orm.update_task_status(task.task_id, '待处理')
            except Exception:
                pass
            ok = task.run(conn=conn)
            try:
                orm.update_task_status(task.task_id, '成功' if ok else '失败')
            except Exception:
                pass
            return bool(ok)
        else:
            # 未知配置ID：仅记录日志
            print(f"[Scheduler] Unknown config_id: {config_id}; params={params}")
            return False
    except Exception as e:
        print(f"[Scheduler] Job error for {config_id}: {e}")
        return False
    finally:
        try:
            if conn:
                conn.close()
        except Exception:
            pass


def build_schedules_from_global_config() -> int:
    """读取 QuestDB 的 schedule_configs 并在 django-q 中生成/更新定时任务。返回生成/更新的数量。"""
    conn = None
    #Schedule.objects.all().delete()
    try:
        conn = qdb_connect()
        gc = GlobalConfig(conn)
        cfgs = getattr(gc, '_schedule_configs', {}) or {}
        count = 0
        for cfg_id, cfg in cfgs.items():
            enabled = cfg.get('enabled') in (1, '1', True, 'true', 't', 'yes', 'y')
            if not enabled:
                # 若存在同名计划，禁用或删除
                try:
                    sch = Schedule.objects.filter(name=f"CFG_{cfg_id}").first()
                    if sch:
                        sch.delete()
                except Exception:
                    pass
                continue
            schedule_time = cfg.get('schedule_time')
            next_run, schedule_type, repeats = _parse_schedule_time(schedule_time)
            # 创建或更新计划
            name = f"CFG_{cfg_id}"
            existing = Schedule.objects.filter(name=name).first()
            if existing:
                existing.func = 'stocks.tasks.scheduler.run_config_job'
                existing.args = str([cfg_id])
                existing.next_run = next_run
                existing.schedule_type = schedule_type
                existing.repeats = repeats
                existing.save()
                # 打印更新操作的结果
                print(f"[Scheduler] Schedule updated: {existing.name}, id={existing.id}")
            else:
                # 保存schedule函数的返回结果
                schedule_result = schedule('stocks.tasks.scheduler.run_config_job', cfg_id,
                         name=name,
                         schedule_type=schedule_type,
                         next_run=next_run,
                         repeats=repeats)
                # 打印返回结果
                print(f"[Scheduler] Schedule created with result: {schedule_result}")
            count += 1
        # 遍历所有已创建的 Schedule 对象并输出信息
        for sch in Schedule.objects.all():
            print(f"[Scheduler] Created/Updated schedule: "
                  f"name={sch.name}, "
                  f"func={sch.func}, "
                  f"args={sch.args}, "
                  f"schedule_type={sch.schedule_type}, "
                  f"next_run={sch.next_run}, "
                  f"repeats={sch.repeats}")
        return count
    finally:
        try:
            if conn:
                conn.close()
        except Exception:
            pass