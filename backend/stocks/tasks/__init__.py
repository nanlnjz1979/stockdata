import logging

from .base import BaseTask
from .download_daily import DownloadDailyTask
from .tasksOrm import QtasksOrm
from .DTBInstTradingTracker import DTBInstTradingTrackerTask
from .incremental_update import IncrementalUpdateTask
from .scheduler import run_config_job

logger = logging.getLogger(__name__)

# 导入任务监控模块
try:
    from .task_monitor import (
        get_all_tasks,
        get_all_schedules,
        get_task_statistics,
        get_recent_tasks
    )
    
    __all__ = [
        "BaseTask", 
        "DownloadDailyTask", 
        "QtasksOrm", 
        "DTBInstTradingTrackerTask", 
        "IncrementalUpdateTask",
        'get_all_tasks',
        'get_all_schedules',
        'get_task_statistics',
        'get_recent_tasks',
        'run_config_job',
    ]
except Exception as e:
    logger.exception("导入任务监控模块失败: %s", e)
