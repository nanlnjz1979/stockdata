import json
import uuid
import logging
from typing import Any, Dict, Optional, Union
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)


class BaseTask(ABC):
    """
    抽象基础任务类（面向 QuestDB 的 ORM 适配器）：
    - 生成任务：通过“ORM对象”的方法写入 QuestDB 的 tasks 表
      期望 ORM 对象至少实现：
        * insert_task(task_id, task_type, task_desc, task_params, priority, status)
        * update_task_status(task_id, status)
    - 运行任务：默认流程更新任务状态（待处理→处理中→成功/失败）
    子类必须实现 `run()` 执行具体逻辑。
    """

    def __init__(
        self,
        orm,
        task_type: str,
        task_desc: str = "",
        params: Optional[Union[Dict[str, Any], str]] = None,
        priority: int = 0,
    ) -> None:
        self.orm = orm
        self.task_type = task_type
        self.task_desc = task_desc
        self.params_str = self._ensure_json_str(params)
        self.priority = priority
        self.task_id: Optional[str] = None

    @staticmethod
    def _ensure_json_str(params: Optional[Union[Dict[str, Any], str]]) -> str:
        if params is None:
            return "{}"
        if isinstance(params, dict):
            try:
                return json.dumps(params, ensure_ascii=False)
            except Exception:
                return "{}"
        # 字符串尝试解析为 JSON，若失败则包裹为 {"value": ...}
        try:
            parsed = json.loads(params)
            if isinstance(parsed, dict):
                return json.dumps(parsed, ensure_ascii=False)
            return json.dumps({"value": parsed}, ensure_ascii=False)
        except Exception:
            return json.dumps({"value": params}, ensure_ascii=False)

    def generate(self) -> str:
        """生成任务记录，状态初始化为“待处理”，返回 task_id。"""
        self.task_id = uuid.uuid4().hex
        if not hasattr(self.orm, 'insert_task'):
            raise AttributeError(
                'ORM对象需实现 insert_task(task_id, task_type, task_desc, task_params, priority, status)'
            )
        try:
            self.orm.insert_task(
                self.task_id,
                self.task_type,
                self.task_desc,
                self.params_str,
                int(self.priority or 0),
                "待处理",
            )
            logger.info("生成任务: %s (%s)", self.task_id, self.task_type)
            return self.task_id
        except Exception as e:
            logger.exception("生成任务失败: %s", e)
            raise

    def execute(self) -> bool:
        """
        执行任务：
        - 若未生成则先生成任务（状态=待处理）
        - 更新状态为“处理中”，执行 `run()`
        - 根据结果更新为“成功”或“失败”
        返回布尔值表示成功/失败。
        """
        if not self.task_id:
            self.generate()
        # 进入处理中
        if hasattr(self.orm, 'update_task_status'):
            try:
                self.orm.update_task_status(self.task_id, "处理中")
            except Exception:
                pass
        try:
            ok = self.run()
        except Exception as exc:
            logger.exception("任务执行异常: %s", exc)
            if hasattr(self.orm, 'update_task_status'):
                try:
                    self.orm.update_task_status(self.task_id, "失败")
                except Exception:
                    pass
            return False
        if hasattr(self.orm, 'update_task_status'):
            try:
                self.orm.update_task_status(self.task_id, "成功" if ok else "失败")
            except Exception:
                pass
        return bool(ok)

    @abstractmethod
    def run(self) -> bool:
        """
        子类需要实现具体执行逻辑。
        返回 True 表示成功，False 表示失败。
        """
        raise NotImplementedError