import re
from datetime import datetime
from typing import Dict

# 导入MongoDB连接相关函数
from backend.db import get_mongo_conn, put_mongo_conn


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

    def __init__(self, mongo_conn=None):
        """
        初始化时从 MongoDB 的 schedule_configs 集合读取配置，存入字典。
        :param mongo_conn: 已建立好的 MongoDB 连接对象（可选，不提供则自动获取）
        """
        self._schedule_configs: Dict[str, str] = {}
        
        # 获取MongoDB连接
        conn = mongo_conn or get_mongo_conn()
        should_close = not mongo_conn
        
        try:
            # 获取数据库和集合
            db = conn['stockdata']
            schedule_configs_col = db['schedule_configs']
            
            # 从MongoDB读取所有配置
            rows = list(schedule_configs_col.find())
            
            for row in rows:
                _id = row.get('_id')
                if _id:
                    self._schedule_configs[_id] = {
                        "name": row.get("name"),
                        "task_desc": row.get("task_desc"),
                        "params": row.get("params"),
                        "schedule_time": row.get("schedule_time"),
                        "enabled": row.get("enabled")
                    }
            
            # 如果默认配置项不在_schedule_configs中，则补充进去
            for key, value in self.DEFAULT_SCHEDULE_CONFIGS.items():
                if key not in self._schedule_configs:
                    self._schedule_configs[key] = value

            # 将补充后的配置写回数据库
            self.save_all_schedule_configs_to_db(conn)
        finally:
            # 如果是自动获取的连接，则关闭
            if should_close and conn:
                put_mongo_conn(conn)
       

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

    def save_all_schedule_configs_to_db(self, mongo_conn=None):
        """
        将内存中的 schedule_configs 全部写回MongoDB。
        集合结构：_id, name, task_desc, params, schedule_time, enabled
        """
        # 获取MongoDB连接
        conn = mongo_conn or get_mongo_conn()
        should_close = not mongo_conn
        
        try:
            # 获取数据库和集合
            db = conn['stockdata']
            schedule_configs_col = db['schedule_configs']
            
            # 先清空原集合，再批量插入
            schedule_configs_col.delete_many({})
            
            # 准备插入的数据
            insert_data = []
            for _id, cfg in self._schedule_configs.items():
                insert_data.append({
                    "_id": _id,
                    "name": cfg["name"],
                    "task_desc": cfg["task_desc"],
                    "params": cfg["params"],
                    "schedule_time": cfg.get("schedule_time"),
                    "enabled": cfg.get("enabled")
                })
            
            # 批量插入到MongoDB
            if insert_data:
                schedule_configs_col.insert_many(insert_data)
        finally:
            # 如果是自动获取的连接，则关闭
            if should_close and conn:
                put_mongo_conn(conn)