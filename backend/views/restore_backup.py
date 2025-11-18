from django.http import JsonResponse
from rest_framework.views import APIView
import os
import subprocess
import json
from datetime import datetime
import logging
from django.conf import settings
import requests

# 配置日志记录器
logger = logging.getLogger(__name__)

# 数据库备份目录配置 - 可以从settings中读取，这里使用默认值
DEFAULT_BACKUP_DIR = os.path.join(settings.BASE_DIR, 'data', 'backups')


def get_backup_dir():
    """获取备份目录"""
    return getattr(settings, 'DB_BACKUP_DIR', DEFAULT_BACKUP_DIR)


def ensure_backup_dir_exists():
    """确保备份目录存在"""
    backup_dir = get_backup_dir()
    if not os.path.exists(backup_dir):
        os.makedirs(backup_dir)
    return backup_dir


# 获取股票代码文件列表API - 类视图实现
class GetRestoreStockFiles(APIView):
    """
    遍历指定路径下的文件，返回股票代码列表
    URL: /api/restore/get_stock_files/
    方法: POST
    请求体: {"path": "data/daily"}
    """
    def post(self, request):
        try:
            # 解析请求体
            data = json.loads(request.body)
            path = data.get('path')
            
            if not path:
                return JsonResponse({
                    'success': False,
                    'message': '请提供文件路径'
                }, status=400)
            
            # 构建完整路径
            # 注意：这里需要防止路径遍历攻击，限制只能访问特定目录下的文件
            base_dir = getattr(settings, 'BASE_DIR', os.getcwd())
            # 只允许访问data目录下的文件
            allowed_base = str(os.path.join(base_dir, 'data'))
            
            # 规范化路径
            if path.startswith('data\\') or path.startswith('data/'):
                # 如果路径已经以data开头，直接使用
                full_path = os.path.normpath(os.path.join(base_dir, path))
            else:
                # 否则，默认在data目录下
                full_path = os.path.normpath(os.path.join(allowed_base, path))
            
            # 安全检查：确保路径在allowed_base内
            if not full_path.startswith(allowed_base):
                return JsonResponse({
                    'success': False,
                    'message': '无权访问该路径'
                }, status=403)
            
            # 检查路径是否存在
            if not os.path.exists(full_path):
                return JsonResponse({
                    'success': False,
                    'message': f'路径不存在: {path}'
                }, status=404)
            
            # 检查路径是否是目录
            if not os.path.isdir(full_path):
                return JsonResponse({
                    'success': False,
                    'message': f'提供的路径不是目录: {path}'
                }, status=400)
            
            # 获取股票代码列表
            stock_codes = []
            
            # 遍历目录下的所有文件
            for filename in os.listdir(full_path):
                # 假设股票代码文件是以股票代码命名的（例如：000001.txt, 600000.json等）
                # 这里我们提取文件名中的股票代码部分
                # 简单实现：移除扩展名，假设文件名本身就是股票代码
                code = os.path.splitext(filename)[0]
                
                # 简单验证：股票代码通常是6位数字
                if code.isdigit() and len(code) == 6:
                    stock_codes.append(code)
            
            logger.info(f"在路径 {path} 下找到 {len(stock_codes)} 个股票代码文件")
            
            # 返回股票代码列表
            return JsonResponse({
                'success': True,
                'message': f'成功获取 {len(stock_codes)} 个股票代码',
                'stock_codes': stock_codes,
                'total_count': len(stock_codes)
            })
            
        except json.JSONDecodeError:
            return JsonResponse({
                'success': False,
                'message': '无效的JSON请求体'
            }, status=400)
        except Exception as e:
            logger.exception("获取股票文件列表时发生异常")
            return JsonResponse({
                'success': False,
                'message': f'获取股票代码失败: {str(e)}'
            }, status=500)

def _analyze_questdb_response(response):
    """专业分析QuestDB响应"""
    
    
    import re
    if response.status_code == 200:
        resp_text = response.text
        
        # 优先检查"Rows handled"和"Rows imported"是否相等
        rows_handled_match = re.search(r'Rows handled\s+\|\s+(\d+)\s+\|', resp_text, re.IGNORECASE)
        rows_imported_match = re.search(r'Rows imported\s+\|\s+(\d+)\s+\|', resp_text, re.IGNORECASE)
        
        if rows_handled_match and rows_imported_match:
            rows_handled = int(rows_handled_match.group(1))
            rows_imported = int(rows_imported_match.group(1))
            
            print(f"📊 处理行数: {rows_handled}, 导入行数: {rows_imported}")
            
            # 如果处理行数和导入行数相同，则认为导入成功
            if rows_handled == rows_imported and rows_handled > 0:
                print(f"✅ 导入成功: 成功导入 {rows_imported} 行数据")
                return True
            elif rows_handled == 0:
                print(f"⚠️  警告: 没有处理任何数据行")
                return False
            else:
                print(f"❌ 导入失败: 处理{rows_handled}行，但只成功导入{rows_imported}行")
                return False
        
        # QuestDB 9.1 成功响应详细解析
        success_cases = [
            (r'Successfully imported (\d+) rows?', "标准成功导入"),
            (r'Created (\w+)\s*and data loaded', "表创建并加载"),
            (r'(\d+)\s*Rows?\s*imported', "行数导入确认"),
            (r'imported successfully', "成功导入确认")
        ]
        
        for pattern, description in success_cases:
            match = re.search(pattern, resp_text, re.IGNORECASE)
            if match:
                print(f"✅ {description}")
                if match.groups():
                    print(f"📊 详细信息: {match.group(0)}")
                return True
        
        # 检查特定错误类型
        error_cases = [
            (r'errno=-?\d+', "系统错误"),
            (r'table.*does not exist', "表不存在"),
            (r'duplicate column', "列重复"),
            (r'invalid table name', "无效表名"),
            (r'malformed CSV', "CSV格式错误"),
            (r'timestamp parse error', "时间戳解析错误")
        ]
        
        for pattern, error_type in error_cases:
            if re.search(pattern, resp_text, re.IGNORECASE):
                print(f"❌ {error_type}: {resp_text}")
                return False
        
        # 未知响应但HTTP 200
        print(f"⚠️  未知成功响应: {resp_text}")
        return True
        
    elif response.status_code == 400:
        print(f"❌ 客户端请求错误: {response.text}")
        return False
    elif response.status_code == 500:
        print(f"❌ QuestDB服务器错误: {response.text}")
        return False
    else:
        print(f"❌ 意外HTTP状态: {response.status_code} - {response.text}")
        return False
        
def import_csv_to_database(csv_file_path, table_name, schema=None, timestamp_col='trade_date', partition_by='DAY', delimiter=',', force_header=True, atomic=True):
    """
    将CSV文件导入到数据库
    
    Args:
        csv_file_path: CSV文件路径
        table_name: 表名
        schema: 列的schema定义
        timestamp_col: 时间戳列名
        partition_by: 分区方式
        delimiter: CSV分隔符
        force_header: 第一行是否为列头
        atomic: 是否原子操作
        
    Returns:
        tuple: (success, message, data)
    """
    try:
        import_url = "http://localhost:9000/imp"
        
        # 准备文件上传
        with open(csv_file_path, 'rb') as f:
            files = {
                'data': (os.path.basename(csv_file_path), f, 'text/csv')
            }
            
            # 准备参数
            params = {
                'name': table_name,
                'timestamp': timestamp_col,
                'partitionBy': partition_by,
                'delimiter': delimiter,
                'forceHeader': str(force_header).lower(),
                'atomic': str(atomic).lower()
            }
            
            # 如果提供了schema，则添加到参数中
            if schema:
                params['schema'] = schema
            
            logger.info(f"调用导入服务: {import_url}，表名: {table_name}")
            
            # 发送POST请求到导入服务
            response = requests.post(
                import_url,
                files=files,
                params=params,
                timeout=60  # 设置超时时间
            )
            
            # 检查响应状态
            if response.status_code == 200:
                # 直接使用_analyze_questdb_response函数判断导入是否成功
                is_success = _analyze_questdb_response(response)
                if is_success:
                    logger.info(f"CSV文件 {os.path.basename(csv_file_path)} 导入成功")
                    return True, "导入成功", {"response_text": response.text}
                else:
                    logger.error(f"CSV文件导入失败: {response.text}")
                    return False, f"导入失败: {response.text}", None
            else:
                logger.error(f"导入服务请求失败，状态码: {response.status_code}, 响应: {response.text}")
                return False, f"导入服务请求失败，状态码: {response.status_code}", None
                
    except requests.exceptions.RequestException as e:
        logger.exception(f"调用导入服务时发生请求异常")
        return False, f"导入服务调用失败: {str(e)}", None
    except Exception as e:
        logger.exception(f"处理CSV文件导入时发生异常")
        return False, f"处理失败: {str(e)}", None


# 处理单个股票代码API - 类视图实现
class RestoreStockData(APIView):
    """
    处理单个股票代码的API
    URL: /api/restore/process
    方法: POST
    请求体: {"code": "000001", "path": "data/daily"}
    """
    def post(self, request):
        try:
            # 解析请求体获取股票代码和路径
            data = json.loads(request.body)
            stock_code = data.get('code')
            stock_path = data.get('path')
            
            # 验证股票代码格式
            if not stock_code or not stock_code.isdigit() or len(stock_code) != 6:
                return JsonResponse({
                    'success': False,
                    'message': '无效的股票代码格式'
                }, status=400)
            
            # 验证路径参数
            if not stock_path:
                return JsonResponse({
                    'success': False,
                    'message': '缺少路径参数'
                }, status=400)
            
            # 记录处理请求
            logger.info(f"接收到股票代码 {stock_code} 的处理请求，路径: {stock_path}")
            
            # 构建CSV文件的完整路径
            base_dir = getattr(settings, 'BASE_DIR', os.getcwd())
            allowed_base = str(os.path.join(base_dir, 'data'))
            
            # 规范化路径
            if stock_path.startswith('data\\') or stock_path.startswith('data/'):
                full_path = os.path.normpath(os.path.join(base_dir, stock_path))
            else:
                full_path = os.path.normpath(os.path.join(allowed_base, stock_path))
            
            # 安全检查：确保路径在allowed_base内
            if not full_path.startswith(allowed_base):
                return JsonResponse({
                    'success': False,
                    'message': '无权访问该路径'
                }, status=403)
            
            # 构建CSV文件路径
            csv_file_path = os.path.join(full_path, f"{stock_code}.csv")
            
            # 检查CSV文件是否存在
            if not os.path.exists(csv_file_path):
                return JsonResponse({
                    'success': False,
                    'message': f'文件不存在: {csv_file_path}'
                }, status=404)
            
            # 定义股票数据的schema（根据实际CSV格式调整）
            
            stock_schema = "code:string,trade_date:timestamp,adjust_type:string,open:double,close:double,high:double,low:double,volume:long,amount:double,turnover:double,outstanding_share:double"

            # 调用封装的导入函数
            success, message, data = import_csv_to_database(
                csv_file_path=csv_file_path,
                table_name="stock_daily",
                schema=stock_schema,
                timestamp_col="trade_date",
                partition_by="DAY",
                delimiter=",",
                force_header=True,
                atomic=True
            )
            
            # 根据导入结果返回响应
            if success:
                logger.info(f"股票 {stock_code} 导入成功")
                return JsonResponse({
                    'success': True,
                    'message': f'股票代码 {stock_code} 导入成功',
                    'stock_code': stock_code,
                    'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'import_result': data
                })
            else:
                logger.error(f"股票 {stock_code} 导入失败: {message}")
                return JsonResponse({
                    'success': False,
                    'message': message
                }, status=500)
                
        except json.JSONDecodeError:
            return JsonResponse({
                'success': False,
                'message': '无效的JSON请求体'
            }, status=400)
        except Exception as e:
            logger.exception(f"处理股票代码时发生异常")
            return JsonResponse({
                'success': False,
                'message': f'处理失败: {str(e)}'
            }, status=500)

class MergeStockData(APIView):
    """
    合并两个目录下的股票代码文件列表
    URL: /api/restore/merge
    方法: POST
    请求体: {"main_path": "data/daily", "append_path": "data/daily_append"}
    """
    def post(self, request):
        try:
            # 解析请求体
            data = json.loads(request.body)
            main_path = data.get('main_path')
            append_path = data.get('append_path')
            
            # 验证参数
            if not main_path or not append_path:
                return JsonResponse({
                    'success': False,
                    'message': '缺少必要的路径参数'
                }, status=400)
            
            # 构建完整路径
            base_dir = getattr(settings, 'BASE_DIR', os.getcwd())
            allowed_base = str(os.path.join(base_dir, 'data'))
            
            # 安全检查和路径规范化
            def normalize_path(path):
                if path.startswith('data\\') or path.startswith('data/'):
                    full_path = os.path.normpath(os.path.join(base_dir, path))
                else:
                    full_path = os.path.normpath(os.path.join(allowed_base, path))
                
                if not full_path.startswith(allowed_base):
                    raise ValueError(f'无权访问该路径: {path}')
                
                if not os.path.exists(full_path) or not os.path.isdir(full_path):
                    raise ValueError(f'无效的路径: {path}')
                
                return full_path
            
            try:
                # 规范化并验证两个路径
                main_full_path = normalize_path(main_path)
                append_full_path = normalize_path(append_path)
            except ValueError as e:
                return JsonResponse({
                    'success': False,
                    'message': str(e)
                }, status=400)
            
            # 获取两个目录下的股票代码列表
            def get_stock_codes_from_dir(dir_path):
                stock_codes = []
                try:
                    for filename in os.listdir(dir_path):
                        # 假设股票代码文件是以股票代码命名的（例如：000001.csv, 600000.csv等）
                        code = os.path.splitext(filename)[0]
                        # 简单验证：股票代码通常是6位数字
                        if code.isdigit() and len(code) == 6:
                            stock_codes.append(code)
                except Exception as e:
                    logger.error(f"读取目录 {dir_path} 时发生错误: {str(e)}")
                    raise ValueError(f"读取目录失败: {str(e)}")
                
                return sorted(stock_codes)
            
            # 获取两个目录的股票代码列表
            main_stock_codes = get_stock_codes_from_dir(main_full_path)
            append_stock_codes = get_stock_codes_from_dir(append_full_path)
            
            # 获取两个列表的交集
            # 使用集合的交集操作，然后转换回列表并排序
            all_stock_codes = sorted(list(set(main_stock_codes) & set(append_stock_codes)))
            
            # 记录日志
            logger.info(f"获取股票代码交集：主目录({len(main_stock_codes)}个) 和 追加目录({len(append_stock_codes)}个) 的交集共有({len(all_stock_codes)}个)")
            
            # 返回成功响应
            return JsonResponse({
                'success': True,
                'message': f'成功获取两个目录的股票代码交集，共{len(all_stock_codes)}个股票代码',
                'stock_codes': all_stock_codes,
                'main_path_count': len(main_stock_codes),
                'append_path_count': len(append_stock_codes),
                'total_count': len(all_stock_codes),
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            })
            
        except json.JSONDecodeError:
            return JsonResponse({
                'success': False,
                'message': '无效的JSON请求体'
            }, status=400)
        except ValueError as e:
            logger.error(f"参数验证错误: {str(e)}")
            return JsonResponse({
                'success': False,
                'message': str(e)
            }, status=400)
        except Exception as e:
            logger.exception("合并股票代码时发生异常")
            return JsonResponse({
                'success': False,
                'message': f'处理失败: {str(e)}'
            }, status=500)


# 合并单个股票数据API - 类视图实现
class MergeStockItem(APIView):
    """
    合并单个股票代码的CSV文件，从追加目录合并到主目录
    URL: /api/restore/mergeItem/
    方法: POST
    请求体: {
        "main_path": "data/daily",
        "append_path": "data/daily_append",
        "stock_code": "000001"
    }
    """
    def post(self, request):
        try:
            # 解析请求体
            data = json.loads(request.body)
            main_path = data.get('main_path')
            append_path = data.get('append_path')
            stock_code = data.get('stock_code')
            
            # 验证必要参数
            if not all([main_path, append_path, stock_code]):
                return JsonResponse({
                    'success': False,
                    'message': '请提供完整的参数：main_path, append_path, stock_code'
                }, status=400)
            
            # 验证股票代码格式
            if not stock_code.isdigit() or len(stock_code) != 6:
                return JsonResponse({
                    'success': False,
                    'message': '股票代码必须是6位数字'
                }, status=400)
            
            # 定义基础路径并转换为字符串
            allowed_base = str(settings.BASE_DIR)
            
            def normalize_path(path):
                """规范化并验证路径"""
                if path.startswith('/') or path.startswith('\\'):
                    # 不允许绝对路径
                    raise ValueError(f'不允许使用绝对路径: {path}')
                
                # 规范化路径并验证
                full_path = os.path.normpath(os.path.join(allowed_base, path))
                
                if not full_path.startswith(allowed_base):
                    raise ValueError(f'无权访问该路径: {path}')
                
                if not os.path.exists(full_path) or not os.path.isdir(full_path):
                    raise ValueError(f'无效的路径: {path}')
                
                return full_path
            
            try:
                # 规范化并验证两个路径
                main_full_path = normalize_path(main_path)
                append_full_path = normalize_path(append_path)
            except ValueError as e:
                return JsonResponse({
                    'success': False,
                    'message': str(e)
                }, status=400)
            
            # 定义CSV文件路径
            main_csv_path = os.path.join(main_full_path, f'{stock_code}.csv')
            append_csv_path = os.path.join(append_full_path, f'{stock_code}.csv')
            
            # 检查文件是否存在
            if not os.path.exists(main_csv_path):
                return JsonResponse({
                    'success': False,
                    'message': f'主目录中未找到股票{stock_code}的CSV文件'
                }, status=404)
            
            if not os.path.exists(append_csv_path):
                return JsonResponse({
                    'success': False,
                    'message': f'追加目录中未找到股票{stock_code}的CSV文件'
                }, status=404)
            
            # 读取两个CSV文件的内容
            main_data = []
            append_data = []
            
            # 读取主目录CSV
            try:
                with open(main_csv_path, 'r', encoding='utf-8') as f:
                    main_data = f.readlines()
            except Exception as e:
                logger.error(f"读取主目录CSV文件失败: {str(e)}")
                return JsonResponse({
                    'success': False,
                    'message': f'读取主目录CSV文件失败: {str(e)}'
                }, status=500)
            
            # 读取追加目录CSV
            try:
                with open(append_csv_path, 'r', encoding='utf-8') as f:
                    append_data = f.readlines()
            except Exception as e:
                logger.error(f"读取追加目录CSV文件失败: {str(e)}")
                return JsonResponse({
                    'success': False,
                    'message': f'读取追加目录CSV文件失败: {str(e)}'
                }, status=500)
            
            # 合并数据（保留主目录的表头，只添加追加目录的数据行）
            if not main_data:
                return JsonResponse({
                    'success': False,
                    'message': '主目录CSV文件为空'
                }, status=400)
            
            # 检查两个文件是否有相同的表头
            if len(main_data) > 0 and len(append_data) > 0:
                if main_data[0].strip() != append_data[0].strip():
                    logger.warning(f"股票{stock_code}的两个CSV文件表头不一致")
            
            # 创建合并后的数据（保留主目录的表头，添加追加目录的数据行）
            merged_data = main_data.copy()
            
            # 如果追加目录有数据行（跳过表头）
            if len(append_data) > 1:
                # 创建主目录数据行的集合（用于去重），跳过表头
                main_data_set = set(main_data[1:]) if len(main_data) > 1 else set()
                
                # 统计添加的新行数量
                new_lines_count = 0
                
                # 只添加主目录中不存在的数据行
                for line in append_data[1:]:
                    if line not in main_data_set:
                        merged_data.append(line)
                        new_lines_count += 1
            else:
                new_lines_count = 0
            
            # 保存合并后的数据到主目录
            try:
                # 先创建备份
                backup_path = os.path.join(main_full_path, f'{stock_code}_backup_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv')
                with open(backup_path, 'w', encoding='utf-8') as f:
                    f.writelines(main_data)
                
                # 写入合并后的数据
                with open(main_csv_path, 'w', encoding='utf-8') as f:
                    f.writelines(merged_data)
                
                # 合并成功后删除备份文件
                try:
                    if os.path.exists(backup_path):
                        os.remove(backup_path)
                        logger.info(f"已删除股票{stock_code}的备份文件: {backup_path}")
                except Exception as e:
                    # 删除备份失败不影响主流程，只记录日志
                    logger.warning(f"删除备份文件失败: {str(e)}")
            except Exception as e:
                logger.error(f"保存合并后的CSV文件失败: {str(e)}")
                return JsonResponse({
                    'success': False,
                    'message': f'保存合并后的CSV文件失败: {str(e)}'
                }, status=500)
            
            # 记录日志
            logger.info(f"成功合并股票{stock_code}的数据：主目录({len(main_data)}行) + 追加目录({len(append_data)-1}行) - 重复行 = 合并后({len(merged_data)}行)，新增{new_lines_count}行")
            
            # 返回成功响应
            return JsonResponse({
                'success': True,
                'message': f'股票{stock_code}数据合并成功，过滤了{(len(append_data)-1)-new_lines_count}行重复数据',
                'stock_code': stock_code,
                'main_file_lines': len(main_data),
                'append_file_lines': len(append_data),
                'merged_file_lines': len(merged_data),
                'new_lines_added': new_lines_count,
                'duplicate_lines_filtered': (len(append_data)-1)-new_lines_count if len(append_data) > 1 else 0,
                'backup_created': False,  # 合并成功后备份文件已被删除
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            })
        
        except json.JSONDecodeError:
            return JsonResponse({
                'success': False,
                'message': '无效的JSON请求体'
            }, status=400)
        except ValueError as e:
            logger.error(f"参数验证错误: {str(e)}")
            return JsonResponse({
                'success': False,
                'message': str(e)
            }, status=400)
        except Exception as e:
            logger.exception(f"合并股票{stock_code}数据时发生异常")
            return JsonResponse({
                'success': False,
                'message': f'处理失败: {str(e)}'
            }, status=500)