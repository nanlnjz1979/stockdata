import os
import re
from datetime import datetime
from typing import Dict


class GlobalConfig:
    """
    全局配置集中管理：支持从环境变量加载，供后端各模块统一引用。
    用法：
        # from .config import GlobalConfig  # not needed here
        GlobalConfig.load_env()  # 可选，模块导入时已自动执行一次
        params = GlobalConfig.qdb_params()
    """
# 默认 schedule_configs 数据，用于初始化或恢复
    DEFAULT_SCHEDULE_CONFIGS = {
        "LHB_InstituteTrack": {
            "name": "机构席位追踪(龙虎榜)",
            "task_desc": "新浪财经-龙虎榜-机构席位追踪",
            "params": '{"market":"CN","adjust":"hfq"}',
            "schedule_time": "16:30",
            "enabled": 1
        },
        "STOCK_Update": {
            "name": "更新新每日股票数据",
            "task_desc": "更新新每日股票数据",
            "params": '{"market":"CN","adjust":"hfq"}',
            "schedule_time": "16:30",
            "enabled": 1
        }
    }

    def __init__(self, questdb_conn):
        """
        初始化时从 QuestDB 的 schedule_configs 表读取配置，存入字典。
        :param questdb_conn: 已建立好的 QuestDB 连接对象（无需再次连接）
        """
        self._schedule_configs: Dict[str, str] = {}
        cursor = questdb_conn.cursor()
        try:
            cursor.execute(
                "SELECT id,  name, task_desc, params, schedule_time, enabled FROM schedule_configs"
            )
            rows = cursor.fetchall()
            for row in rows:
                _id,  name, task_desc, params, schedule_time, enabled = row
                self._schedule_configs[_id] = {
                    "name": name,
                    "task_desc": task_desc,
                    "params": params,
                    "schedule_time": schedule_time,
                    "enabled": enabled
                }
            # 如果默认配置项不在_schedule_configs中，则补充进去
            for key, value in self.DEFAULT_SCHEDULE_CONFIGS.items():
                if key not in self._schedule_configs:
                    self._schedule_configs[key] = value

            # 将补充后的配置写回数据库
            self.save_all_schedule_configs_to_db(questdb_conn)
        finally:
            cursor.close()
       

    def get_schedule_config(self, key: str, default: str = None) -> str:
        """
        读取指定参数
        :param key: 配置项键名
        :param default: 若不存在则返回默认值
        :return: 配置值
        """
        return self._schedule_configs.get(key, default)

    def set_schedule_config(self, key: str, value: str):
        """
        设置指定参数（仅内存字典，不自动写回数据库）
        :param key: 配置项键名
        :param value: 配置项值
        """
        self._schedule_configs[key] = value

    @staticmethod
    def _normalize_schedule_time_for_db(val):
        if val is None:
            return None
        if isinstance(val, datetime):
            return val
        s = str(val).strip()
        # 支持 HH:MM 或 HH:MM:SS
        if re.match(r"^\d{2}:\d{2}(:\d{2})?$", s):
            if len(s) == 5:
                s = s + ":00"
            return f"1970-01-01 {s}"
        # 其他情况直接返回字符串，交由驱动解析
        return s

    @staticmethod
    def _normalize_enabled_for_db(val):
        if isinstance(val, bool):
            return 1 if val else 0
        if isinstance(val, (int,)):
            return 1 if val != 0 else 0
        s = str(val).strip().lower()
        return 1 if s in ("1", "true", "t", "yes", "y") else 0

    def save_all_schedule_configs_to_db(self, questdb_conn):
        """
        将内存中的 schedule_configs 全部写回数据库。
        表结构：id symbol (主键), name, task_desc, params, schedule_time, enabled
        """
        cursor = questdb_conn.cursor()
        try:
            # 先清空原表，再批量插入
            cursor.execute("TRUNCATE TABLE schedule_configs")
            for _id, cfg in self._schedule_configs.items():
                sql = (
                    """
                    INSERT INTO schedule_configs
                        (id, name, task_desc, params, schedule_time, enabled)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """
                )
                params = (
                    _id,
                    cfg["name"],
                    cfg["task_desc"],
                    cfg["params"],
                    GlobalConfig._normalize_schedule_time_for_db(cfg.get("schedule_time")),
                    GlobalConfig._normalize_enabled_for_db(cfg.get("enabled")),
                )
                print("[GlobalConfig] SQL:", sql.strip())
                print("[GlobalConfig] Params:", params)
                try:
                    cursor.execute(sql, params)
                except Exception as e:
                    import traceback
                    print("[GlobalConfig] Execute error:", e)
                    try:
                        print("[GlobalConfig] Params (repr):", repr(params))
                    except Exception:
                        pass
                    traceback.print_exc()
                    # 继续抛出，便于上层看到错误并中止
                    raise
            questdb_conn.commit()
        finally:
            cursor.close()