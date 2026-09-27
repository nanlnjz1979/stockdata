#!/usr/bin/env python3
import sys
import os
from pathlib import Path

# 加入项目路径
sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).parent.parent))

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "stockserver.settings")
import django
django.setup()

from stocks.utils.path_utils import normalize_path, join_path, safe_join
from django.conf import settings

print("=== 测试路径工具函数 ===\n")

# 测试 normalize_path
print("1. 测试 normalize_path:")
test_paths = [
    'data\\daily',
    'data/daily',
    'data\\\\\\\\daily',
    'data/./daily/../daily'
]
for p in test_paths:
    result = normalize_path(p)
    print(f"   输入: {repr(p)}")
    print(f"   输出: {repr(result)}")
print()

# 测试 join_path
print("2. 测试 join_path:")
base = settings.BASE_DIR
try:
    result = join_path(base, 'data', 'daily')
    print(f"   输入: ({repr(base)}, 'data', 'daily')")
    print(f"   输出: {repr(result)}")
except Exception as e:
    print(f"   错误: {type(e).__name__}: {e}")
    import traceback
    traceback.print_exc()
print()

# 测试 safe_join
print("3. 测试 safe_join:")
allowed_base = join_path(base, 'data')
try:
    result = safe_join(allowed_base, 'data/daily')
    print(f"   输入: allowed_base={repr(allowed_base)}, 'data/daily'")
    print(f"   输出: {repr(result)}")
except Exception as e:
    print(f"   错误: {type(e).__name__}: {e}")
    import traceback
    traceback.print_exc()

try:
    result = safe_join(allowed_base, 'daily')
    print(f"   输入: allowed_base={repr(allowed_base)}, 'daily'")
    print(f"   输出: {repr(result)}")
except Exception as e:
    print(f"   错误: {type(e).__name__}: {e}")
    import traceback
    traceback.print_exc()
print()

print("=== 测试完成 ===")
