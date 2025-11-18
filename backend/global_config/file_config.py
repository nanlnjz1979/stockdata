import os
import json
from typing import Any, Dict, Optional


class BaseFileConfig:
    """
    配置管理基类
    提供配置文件的读写功能，子类只需设置_config_file属性即可
    """
    # 配置文件路径 - 由子类设置
    _config_file: str = ""
    # 内存中的配置数据
    _config_data: Dict[str, Any] = {}
    # 初始化标志
    _initialized: bool = False
    
    @classmethod
    def _initialize(cls):
        """
        初始化配置，如果尚未初始化则加载配置文件
        """
        if not cls._initialized:
            cls._load_config()
            cls._initialized = True
    
    @classmethod
    def _load_config(cls):
        """
        从配置文件加载数据到内存
        """
        try:
            if os.path.exists(cls._config_file):
                with open(cls._config_file, 'r', encoding='utf-8') as f:
                    cls._config_data = json.load(f)
            else:
                # 如果文件不存在，初始化空配置
                cls._config_data = {}
                # 创建配置文件
                cls._save_config()
        except Exception as e:
            print(f"加载配置文件失败: {e}")
            # 加载失败时使用空配置
            cls._config_data = {}
    
    @classmethod
    def _save_config(cls):
        """
        将内存中的配置数据保存到文件
        """
        try:
            # 确保目录存在
            os.makedirs(os.path.dirname(cls._config_file), exist_ok=True)
            # 写入文件
            with open(cls._config_file, 'w', encoding='utf-8') as f:
                json.dump(cls._config_data, f, ensure_ascii=False, indent=4)
        except Exception as e:
            print(f"保存配置文件失败: {e}")
    
    @classmethod
    def get(cls, key: str, default: Any = None) -> Any:
        """
        从内存中获取配置值
        
        Args:
            key: 配置项的键名
            default: 如果键不存在，返回的默认值
            
        Returns:
            配置值，如果键不存在则返回默认值
        """
        cls._initialize()
        return cls._config_data.get(key, default)
    
    @classmethod
    def set(cls, key: str, value: Any) -> None:
        """
        设置配置值，并保存到文件
        
        Args:
            key: 配置项的键名
            value: 配置项的值
        """
        cls._initialize()
        # 更新内存中的数据
        cls._config_data[key] = value
        # 保存到文件
        cls._save_config()
    
    @classmethod
    def delete(cls, key: str) -> bool:
        """
        删除配置项，并保存到文件
        
        Args:
            key: 要删除的配置项键名
            
        Returns:
            是否删除成功
        """
        cls._initialize()
        if key in cls._config_data:
            del cls._config_data[key]
            cls._save_config()
            return True
        return False
    
    @classmethod
    def clear(cls) -> None:
        """
        清空所有配置，并保存到文件
        """
        cls._initialize()
        cls._config_data = {}
        cls._save_config()
    
    @classmethod
    def get_all(cls) -> Dict[str, Any]:
        """
        获取所有配置项
        
        Returns:
            包含所有配置项的字典
        """
        cls._initialize()
        return cls._config_data.copy()


class FileConfig(BaseFileConfig):
    """
    全局配置管理类
    配置数据存储在config.json文件中，支持key-value方式存取
    数据加载到内存中，get操作直接从内存读取，set操作同时更新内存和文件
    支持直接通过类名调用：FileConfig.set(), FileConfig.get()
    """
    # 配置文件路径
    _config_file: str = os.path.join(os.path.dirname(__file__), 'config.json')


class DataConfig(BaseFileConfig):
    """
    数据配置管理类
    配置数据存储在data_config.json文件中，功能与FileConfig相同
    """
    # 配置文件路径 - 使用不同的文件名
    _config_file: str = os.path.join(os.path.dirname(__file__), 'data.json')


# 提供一个便捷的全局实例访问方式
def get_config() -> FileConfig:
    """
    获取FileConfig的单例实例
    
    Returns:
        FileConfig实例
    """
    return FileConfig()