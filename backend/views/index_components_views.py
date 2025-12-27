from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
import logging
import traceback
from datetime import datetime
import os
from django.conf import settings
from global_config.data_fetch import AkshareFetcher, DataFetchError

logger = logging.getLogger(__name__)

# 定义全局状态变量，用于跟踪指数成份更新状态
index_components_update_status = {
    "running": False,
    "paused": False,
    "total_count": 0,
    "updated_count": 0,
    "status": "idle",
    "last_update_time": datetime.now().isoformat(),
    "current_index": "-"
}


class IndexComponentsUpdateView(APIView):
    """
    指数成份更新API视图
    调用方式: POST /api/stocks/update/index_components/start
    功能: 更新指数的成份股数据
    """
    
    def post(self, request):
        global index_components_update_status
        import concurrent.futures
        import threading
        try:
            # 记录开始时间
            start_time = datetime.now()
            logger.info(f"开始更新指数成份，开始时间: {start_time}")
            
            # 获取所有指数列表
            fetcher = AkshareFetcher()
            all_indices = fetcher.get_all_indices()
            
            # 使用所有指数，保留代码和名称信息
            indices_to_update = all_indices  # 直接使用包含代码和名称的指数列表
            
            # 创建保存指数成份数据的目录
            index_components_dir = os.path.join(settings.BASE_DIR, 'data', 'index_components')
            os.makedirs(index_components_dir, exist_ok=True)
            
            # 更新全局状态
            index_components_update_status = {
                "running": True,
                "paused": False,
                "total_count": len(indices_to_update),
                "updated_count": 0,
                "status": "running",
                "last_update_time": start_time.isoformat(),
                "current_index": "-"
            }
            
            # 统计结果和线程锁
            successful_indices = 0
            errors = []
            lock = threading.Lock()
            
            # 定义处理单个指数的函数
            def process_index(index_info):
                nonlocal successful_indices, errors
                index_code = index_info["code"]
                index_name = index_info["name"]
                
                try:
                    # 检查是否已经存在对应的CSV文件
                    csv_file_path = os.path.join(index_components_dir, f"{index_code}.csv")
                    if os.path.exists(csv_file_path):
                        logger.info(f"指数{index_code}({index_name})的成份股数据已存在，跳过处理")
                        with lock:
                            index_components_update_status["updated_count"] += 1
                            successful_indices += 1
                        return True
                    
                    logger.info(f"开始处理指数{index_code}({index_name})的成份股数据")
                    
                    # 更新当前处理的指数代码（非线程安全，但不影响功能）
                    index_components_update_status["current_index"] = index_code
                    
                    # 创建本地fetcher实例，避免线程间共享
                    local_fetcher = AkshareFetcher()
                    # 调用fetch_index_stock_cons方法获取并处理指数成份股数据
                    index_stocks = local_fetcher.fetch_index_stock_cons(symbol=index_code)
                    
                    if index_stocks:
                        # 保存处理后的数据到文件，文件名就是指数代码
                        import pandas as pd
                        df = pd.DataFrame(index_stocks)
                        # 添加指数代码和名称列
                        df['index_code'] = index_code
                        df['index_name'] = index_name
                        csv_file_path = os.path.join(index_components_dir, f"{index_code}.csv")
                        df.to_csv(csv_file_path, index=False, encoding='utf-8')
                        logger.info(f"成功处理并保存指数{index_code}({index_name})的成份股数据到{csv_file_path}，共{len(df)}条记录")
                        with lock:
                            successful_indices += 1
                    else:
                        logger.warning(f"未获取到指数{index_code}的有效成份股数据")
                    
                    with lock:
                        index_components_update_status["updated_count"] += 1
                    return True
                except Exception as e:
                    error_msg = f"处理指数{index_code}的成份股数据失败: {str(e)}"
                    logger.error(error_msg)
                    traceback.print_exc()
                    with lock:
                        errors.append(error_msg)
                        index_components_update_status["updated_count"] += 1
                    return False
            
            # 使用ThreadPoolExecutor进行多线程处理，默认5个线程
            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                # 提交所有任务
                futures = [executor.submit(process_index, index_info) for index_info in indices_to_update]
                # 等待所有任务完成
                concurrent.futures.wait(futures)
            
            # 计算耗时
            end_time = datetime.now()
            time_cost = (end_time - start_time).total_seconds()
            
            # 更新全局状态为已完成
            index_components_update_status = {
                "running": False,
                "paused": False,
                "total_count": len(indices_to_update),
                "updated_count": len(indices_to_update),
                "status": "completed",
                "last_update_time": end_time.isoformat(),
                "current_index": "-"
            }
            
            logger.info(f"指数成份更新完成，共处理{len(indices_to_update)}个指数，成功{successful_indices}个，耗时{time_cost:.2f}秒")
            
            # 构建响应
            response_data = {
                "success": True,
                "message": f"指数成份更新完成，共处理{len(indices_to_update)}个指数，成功{successful_indices}个",
                "total_indices": len(indices_to_update),
                "successful_indices": successful_indices,
                "time_cost": time_cost
            }
            
            if errors:
                response_data["errors"] = errors
            
            return Response(response_data)
            
        except Exception as e:
            # 更新全局状态为失败
            index_components_update_status = {
                "running": False,
                "paused": False,
                "total_count": 0,
                "updated_count": 0,
                "status": "failed",
                "last_update_time": datetime.now().isoformat(),
                "current_index": "-"
            }
            
            logger.error(f"更新指数成份失败: {str(e)}")
            logger.error(traceback.format_exc())
            return Response({
                "success": False,
                "message": f"更新指数成份失败: {str(e)}",
                "error": str(e)
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class IndexComponentsStatusView(APIView):
    """
    获取指数成份更新状态API视图
    调用方式: GET /api/stocks/update/index_components/status
    功能: 获取指数成份更新的状态信息
    """
    
    def get(self, request):
        global index_components_update_status
        try:
            # 返回真实的全局状态
            return Response({
                "success": True,
                "data": {
                    "running": index_components_update_status["running"],
                    "paused": index_components_update_status["paused"],
                    "total_count": index_components_update_status["total_count"],
                    "updated_count": index_components_update_status["updated_count"],
                    "status": index_components_update_status["status"],
                    "last_update_time": index_components_update_status["last_update_time"],
                    "current_index": index_components_update_status["current_index"]
                }
            })
            
        except Exception as e:
            logger.error(f"获取指数成份更新状态失败: {str(e)}")
            logger.error(traceback.format_exc())
            return Response({
                "success": False,
                "message": f"获取指数成份更新状态失败: {str(e)}",
                "error": str(e)
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class IndexComponentsPauseView(APIView):
    """
    暂停指数成份更新API视图
    调用方式: POST /api/stocks/update/index_components/pause
    功能: 暂停指数成份更新任务
    """
    
    def post(self, request):
        try:
            # 这里模拟暂停逻辑
            global index_components_update_status
            index_components_update_status["paused"] = True
            logger.info("暂停指数成份更新")
            return Response({
                "success": True,
                "message": "指数成份更新已暂停",
                "paused": True
            })
        except Exception as e:
            logger.error(f"暂停指数成份更新失败: {str(e)}")
            logger.error(traceback.format_exc())
            return Response({
                "success": False,
                "message": f"暂停指数成份更新失败: {str(e)}",
                "error": str(e)
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class IndexComponentsResumeView(APIView):
    """
    恢复指数成份更新API视图
    调用方式: POST /api/stocks/update/index_components/resume
    功能: 恢复指数成份更新任务
    """
    
    def post(self, request):
        try:
            # 这里模拟恢复逻辑
            global index_components_update_status
            index_components_update_status["paused"] = False
            logger.info("恢复指数成份更新")
            return Response({
                "success": True,
                "message": "指数成份更新已恢复",
                "paused": False
            })
        except Exception as e:
            logger.error(f"恢复指数成份更新失败: {str(e)}")
            logger.error(traceback.format_exc())
            return Response({
                "success": False,
                "message": f"恢复指数成份更新失败: {str(e)}",
                "error": str(e)
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class GetAllIndicesView(APIView):
    """
    获取所有指数列表API视图
    调用方式: GET /api/stocks/update/index_components/all
    功能: 获取所有指数的列表
    """
    
    def get(self, request):
        try:
            logger.info("开始获取所有指数列表")
            
            # 创建AkshareFetcher实例
            fetcher = AkshareFetcher()
            
            # 调用get_all_indices方法获取所有指数列表
            indices = fetcher.get_all_indices()
            
            logger.info(f"成功获取所有指数列表，共{len(indices)}个指数")
            
            return Response({
                "success": True,
                "message": "获取所有指数列表成功",
                "data": indices
            })
        except Exception as e:
            logger.error(f"获取所有指数列表失败: {str(e)}")
            logger.error(traceback.format_exc())
            return Response({
                "success": False,
                "message": f"获取所有指数列表失败: {str(e)}",
                "error": str(e)
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
