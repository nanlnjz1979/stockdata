"""
路径处理工具模块
提供跨平台兼容的路径处理函数，兼容Windows、Linux、macOS
"""
import os
from django.conf import settings


def normalize_path(path: str) -> str:
    """
    统一路径格式，跨平台兼容
    
    将所有路径分隔符统一为正斜杠(/)
    移除多余的分隔符
    规范化路径
    
    Args:
        path: 输入路径字符串
        
    Returns:
        规范化后的路径字符串
    """
    if not path:
        return path
    
    # 1. 统一所有反斜杠为正斜杠
    path = path.replace('\\', '/')
    
    # 2. 使用os.path.normpath进行规范化
    # 这会处理 . 和 .. 等相对路径
    normalized = os.path.normpath(path)
    
    # 3. 再次确保使用正斜杠（在Windows上os.path.normpath会返回反斜杠）
    normalized = normalized.replace('\\', '/')
    
    return normalized


def join_path(*paths: str) -> str:
    """
    安全连接路径，跨平台兼容
    
    Args:
        *paths: 要连接的路径片段
        
    Returns:
        连接后的完整路径
    """
    # 先规范化每个路径片段
    normalized_paths = [normalize_path(p) for p in paths]
    
    # 连接路径
    joined = os.path.join(*normalized_paths)
    
    # 再次规范化，确保使用正斜杠
    return normalize_path(joined)


def safe_join(base_path: str, *paths: str) -> str:
    """
    安全连接路径，防止路径遍历攻击
    
    Args:
        base_path: 基础路径（必须是绝对路径）
        *paths: 要连接的路径片段
        
    Returns:
        连接后的安全路径
        
    Raises:
        ValueError: 如果连接后的路径不在base_path内
    """
    # 规范化基础路径
    base_path = normalize_path(base_path)
    
    # 连接所有路径
    joined = join_path(base_path, *paths)
    
    # 规范化最终路径
    joined = normalize_path(joined)
    
    # 确保最终路径在基础路径内
    if not joined.startswith(base_path):
        raise ValueError(f"路径访问受限: {joined} 不在 {base_path} 内")
    
    return joined


def get_backup_dir() -> str:
    """
    获取数据库备份目录
    
    Returns:
        备份目录的完整路径
    """
    default_dir = join_path(settings.BASE_DIR, 'data', 'backups')
    return getattr(settings, 'DB_BACKUP_DIR', default_dir)


def ensure_dir_exists(dir_path: str) -> str:
    """
    确保目录存在，如果不存在则创建
    
    Args:
        dir_path: 目录路径
        
    Returns:
        规范化后的目录路径
    """
    dir_path = normalize_path(dir_path)
    if not os.path.exists(dir_path):
        os.makedirs(dir_path, exist_ok=True)
    return dir_path


def get_relative_path(absolute_path: str, base_path: str = None) -> str:
    """
    获取相对于基础路径的相对路径
    
    Args:
        absolute_path: 绝对路径
        base_path: 基础路径，默认为settings.BASE_DIR
        
    Returns:
        相对路径
    """
    if base_path is None:
        base_path = settings.BASE_DIR
    
    absolute_path = normalize_path(absolute_path)
    base_path = normalize_path(base_path)
    
    # 使用os.path.relpath获取相对路径
    relative = os.path.relpath(absolute_path, base_path)
    
    # 确保使用正斜杠
    return normalize_path(relative)
