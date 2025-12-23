"""数据库连接池测试脚本"""

import os
import sys
import time
import logging

# 配置日志
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('db_pool_test')

def test_pool_import():
    """测试连接池模块导入"""
    logger.info("=== 测试连接池模块导入 ===")
    try:
        from db import db_pool
        logger.info("✓ 成功导入db_pool模块")
        return True
    except Exception as e:
        logger.error(f"✗ 导入db_pool模块失败: {e}")
        return False

def test_pool_functions():
    """测试连接池核心函数"""
    logger.info("=== 测试连接池核心函数 ===")
    try:
        from db import get_pool, get_pool_stats
        pool = get_pool()
        logger.info("✓ 成功获取连接池实例")
        
        stats = get_pool_stats()
        logger.info(f"✓ 连接池状态: {stats}")
        return True
    except Exception as e:
        logger.error(f"✗ 测试连接池函数失败: {e}")
        return False

def test_connection_operations():
    """测试连接获取和归还操作"""
    logger.info("=== 测试连接获取和归还操作 ===")
    try:
        from db import get_conn, put_conn, get_pool_stats
        
        # 获取连接
        conn = get_conn(
            host=os.getenv('CLICKHOUSE_HOST', '192.168.1.16'),
            port=int(os.getenv('CLICKHOUSE_PORT', '9000')),
            user=os.getenv('CLICKHOUSE_USER', 'default'),
            password=os.getenv('CLICKHOUSE_PASSWORD', ''),
            database=os.getenv('CLICKHOUSE_DATABASE', 'default')
        )
        logger.info("✓ 成功从连接池获取连接")
        
        # 执行简单查询，适配不同数据库连接
        try:
            result = None
            if hasattr(conn, '_client'):  # ClickHouse连接对象
                result = conn._client.execute("SELECT 1")
                logger.info(f"✓ 连接有效，查询结果: {result}")
            elif hasattr(conn, 'execute'):  # 直接支持execute的连接对象
                result = conn.execute("SELECT 1")
                logger.info(f"✓ 连接有效，查询结果: {result}")
            else:  # 传统数据库连接
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                result = cursor.fetchone()
                cursor.close()
                logger.info(f"✓ 连接有效，查询结果: {result}")
        except Exception as e:
            logger.error(f"✗ 连接有效性测试失败: {e}")
        
        # 归还连接
        put_conn(conn)
        logger.info("✓ 成功归还连接到连接池")
        
        # 查看连接池状态
        stats = get_pool_stats()
        logger.info(f"✓ 归还后连接池状态: {stats}")
        
        return True
    except Exception as e:
        logger.error(f"✗ 测试连接操作失败: {e}")
        return False

def test_multiple_connections():
    """测试多连接并发获取"""
    logger.info("=== 测试多连接并发获取 ===")
    try:
        from db import get_conn, put_conn
        
        # 获取多个连接
        connections = []
        for i in range(3):
            conn = get_conn(
                host=os.getenv('CLICKHOUSE_HOST', '192.168.1.16'),
                port=int(os.getenv('CLICKHOUSE_PORT', '9000')),
                user=os.getenv('CLICKHOUSE_USER', 'default'),
                password=os.getenv('CLICKHOUSE_PASSWORD', ''),
                database=os.getenv('CLICKHOUSE_DATABASE', 'default')
            )
            connections.append(conn)
            logger.info(f"✓ 获取连接 {i+1}")
        
        # 归还所有连接
        for i, conn in enumerate(connections):
            put_conn(conn)
            logger.info(f"✓ 归还连接 {i+1}")
        
        logger.info("✓ 多连接测试成功")
        return True
    except Exception as e:
        logger.error(f"✗ 多连接测试失败: {e}")
        return False

def main():
    """主测试函数"""
    logger.info("开始测试数据库连接池...")
    
    # 添加项目路径
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    
    # 运行所有测试
    tests = [
        test_pool_import,
        test_pool_functions,
        test_connection_operations,
        test_multiple_connections
    ]
    
    passed = 0
    for test_func in tests:
        if test_func():
            passed += 1
        print()
        time.sleep(0.5)  # 添加短暂延迟以便查看日志
    
    # 打印测试结果
    logger.info(f"测试完成: {passed}/{len(tests)} 测试通过")
    
    if passed == len(tests):
        logger.info("🎉 所有测试通过！数据库连接池正常工作。")
    else:
        logger.warning("⚠️  部分测试失败，请检查连接池配置。")


if __name__ == '__main__':
    main()