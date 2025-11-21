from django.http import JsonResponse
from rest_framework.views import APIView
import os
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
                if (code.isdigit() and len(code) == 6) or (code.endswith('.SI') and code[:-3].isdigit() and len(code[:-3]) == 6):
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
    """专业分析QuestDB响应，支持JSON格式响应"""
    
    import re
    if response.status_code == 200:
        resp_text = response.text
        
        # 首先尝试解析JSON格式响应
        try:
            resp_json = json.loads(resp_text)
            # 检查是否是标准的QuestDB JSON响应格式
            if isinstance(resp_json, dict) and 'status' in resp_json:
                # 根据status字段判断成功与否
                if resp_json['status'] == 'OK':
                    rows_imported = resp_json.get('rowsImported', 0)
                    rows_rejected = resp_json.get('rowsRejected', 0)
                    location = resp_json.get('location', '')
                    
                    print(f"📊 JSON响应分析: 表名={location}, 导入行数={rows_imported}, 拒绝行数={rows_rejected}")
                    
                    # 如果有拒绝的行，认为导入部分失败
                    if rows_rejected > 0:
                        print(f"❌ 导入部分失败: {rows_rejected} 行被拒绝")
                        # 检查列错误
                        error_columns = []
                        for col in resp_json.get('columns', []):
                            if col.get('errors', 0) > 0:
                                error_columns.append(f"{col['name']}({col['errors']}错误)")
                        if error_columns:
                            print(f"❌ 列错误详情: {', '.join(error_columns)}")
                        return False
                    # 如果导入行数大于0且没有拒绝的行，则认为成功
                    elif rows_imported > 0:
                        print(f"✅ 导入成功: 成功导入 {rows_imported} 行数据到表 {location}")
                        return True
                    else:
                        print(f"⚠️  警告: 没有导入任何数据行")
                        return False
                else:
                    # status不为OK，解析错误信息
                    error_msg = resp_json.get('error', resp_text)
                    print(f"❌ JSON响应错误: {error_msg}")
                    return False
        except json.JSONDecodeError:
            # 不是有效的JSON，继续使用正则表达式解析文本响应
            pass
        
        # 原有逻辑：处理非JSON格式的响应
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
                'atomic': str(atomic).lower(),
                'fmt': 'json'
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
                # 尝试解析JSON响应以获取详细错误信息
                error_message = None
                try:
                    resp_json = json.loads(response.text)
                    if isinstance(resp_json, dict):
                        # 提取详细错误信息
                        if resp_json.get('status') != 'OK':
                            error_message = resp_json.get('error', '导入失败')
                        elif resp_json.get('rowsRejected', 0) > 0:
                            rows_rejected = resp_json.get('rowsRejected', 0)
                            rows_imported = resp_json.get('rowsImported', 0)
                            # 构建错误信息
                            error_parts = [f"导入部分失败: {rows_rejected} 行被拒绝"]
                            # 添加列错误详情
                            error_columns = []
                            for col in resp_json.get('columns', []):
                                if col.get('errors', 0) > 0:
                                    error_columns.append(f"{col['name']}({col['errors']}错误)")
                            if error_columns:
                                error_parts.append(f"错误列: {', '.join(error_columns)}")
                            error_message = '; '.join(error_parts)
                except json.JSONDecodeError:
                    # 不是有效的JSON，使用原始文本
                    pass
                
                # 直接使用_analyze_questdb_response函数判断导入是否成功
                is_success = _analyze_questdb_response(response)
                if is_success:
                    logger.info(f"CSV文件 {os.path.basename(csv_file_path)} 导入成功")
                    return True, "导入成功", {"response_text": response.text}
                else:
                    # 使用提取的错误信息或默认错误信息
                    if not error_message:
                        error_message = f"导入失败: 数据格式可能不匹配或存在错误"
                    logger.error(f"CSV文件导入失败: {response.text}")
                    return False, error_message, None
            else:
                # 尝试从错误响应中提取更详细的信息
                error_message = f"导入服务请求失败，状态码: {response.status_code}"
                try:
                    error_data = json.loads(response.text)
                    if isinstance(error_data, dict) and 'error' in error_data:
                        error_message += f": {error_data['error']}"
                except (json.JSONDecodeError, KeyError):
                    # 如果无法解析JSON，使用原始响应
                    if len(response.text) > 100:
                        error_message += f": {response.text[:100]}..."
                    else:
                        error_message += f": {response.text}"
                
                logger.error(f"导入服务请求失败: {error_message}")
                return False, error_message, None
                
    except requests.exceptions.ConnectionError:
        error_message = "无法连接到QuestDB服务，请检查服务是否运行"
        logger.exception(f"调用导入服务时发生连接异常")
        return False, error_message, None
    except requests.exceptions.Timeout:
        error_message = "连接QuestDB服务超时，请检查服务响应情况"
        logger.exception(f"调用导入服务时发生超时异常")
        return False, error_message, None
    except requests.exceptions.RequestException as e:
        logger.exception(f"调用导入服务时发生请求异常")
        return False, f"导入服务调用失败: {str(e)}", None
    except FileNotFoundError:
        error_message = f"CSV文件不存在: {csv_file_path}"
        logger.exception(f"找不到CSV文件")
        return False, error_message, None
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
            # 解析请求体获取股票代码、路径和表名
            data = json.loads(request.body)
            stock_code = data.get('code')
            stock_path = data.get('path')
            table_name = data.get('table_name')  
            
            # 验证表名参数
            if not table_name:
                return JsonResponse({
                    'success': False,
                    'message': '缺少表名参数'
                }, status=400)

            # 验证股票代码格式：6位数字 或 6位数字.SI
            if not (
                (stock_code.isdigit() and len(stock_code) == 6) or
                (stock_code.endswith('.SI') and stock_code[:-3].isdigit() and len(stock_code[:-3]) == 6)
            ):
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
            
            # 记录处理请求，包含表名信息
            logger.info(f"接收到股票代码 {stock_code} 的处理请求，路径: {stock_path}，表名: {table_name}")
            
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
            
            if table_name == 'stock_daily':
                stock_schema = "code:string,trade_date:timestamp,adjust_type:string,open:double,close:double,high:double,low:double,volume:long,amount:double,turnover:double,outstanding_share:double"
                timestamp_col="trade_date",
            if table_name == 'sw_index':
                stock_schema = "IndustryCode:string,date:timestamp,lyrPe:double,lyrPeQuantile:double,ttmPe:double,ttmPeQuantile:double,pb:double,pbQuantile:double,dvRatio:Ratio:double,dvRatioQuantile:double,dvTtm:double,dvTtmQuantile:double,addLyrPe:double,addLyrPeQuantile:double,addTtmPe:double,addTtmPeQuantile:double,addPb:double,addPbQuantile:double,addDvRatio:Ratio:double,addDvTtm:double,turnoverRate:double,turnoverRateF:double,addTurnoverRate:double,addTurnoverRateF:double,turnoverRateFQuantile:double:double,totalMv:double,close_price:double,addClose:double,middleLyrPe:int,middleLyrPeQuantile:int,middleTtmPe:int,middleTtmPeQuantile:int,middlePb:int,middlePbQuantile:int,belowNetAssetPercent:double,belowNetAssetCount:int,total_count:int,value5:string,value10:string,value20:string,value60:string,indexClose:double,amount:string,amountCongestion:string,amountCongestionQuantile:int"
                timestamp_col="date",

            # 调用封装的导入函数，使用动态表名
            success, message, data = import_csv_to_database(
                csv_file_path=csv_file_path,
                table_name=table_name,  # 使用接收到的表名
                schema=stock_schema,
                timestamp_col= timestamp_col,
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
            
            # 直接使用追加目录的股票代码列表
            all_stock_codes = append_stock_codes
            
            # 记录日志
            logger.info(f"使用追加目录的股票代码：追加目录共有({len(all_stock_codes)}个)股票代码")
            
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


class SwMergeData(APIView):
    """
    获取两个目录下申万指数代码的交集
    URL: /api/restore/sw_merge
    方法: POST
    请求体: {"main_path": "data/sw_index", "append_path": "data/sw_index_append"}
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
            
            # 获取两个目录下的申万指数代码列表
            def get_sw_codes_from_dir(dir_path):
                sw_codes = []
                try:
                    for filename in os.listdir(dir_path):
                        # 申万指数代码通常不是纯数字，可以是字母开头加数字
                        # 例如：801010.SI (申万农林牧渔指数)
                        code = os.path.splitext(filename)[0]
                        # 这里我们不做严格验证，只需要文件名（不含扩展名）
                        sw_codes.append(code)
                except Exception as e:
                    logger.error(f"读取目录 {dir_path} 时发生错误: {str(e)}")
                    raise ValueError(f"读取目录失败: {str(e)}")
                
                return sorted(sw_codes)
            
            # 获取两个目录的申万指数代码列表
            main_sw_codes = get_sw_codes_from_dir(main_full_path)
            append_sw_codes = get_sw_codes_from_dir(append_full_path)
            
            # 直接返回追加目录下的申万指数代码列表
            all_sw_codes = append_sw_codes
            
            # 记录日志
            logger.info(f"获取申万指数代码列表：追加目录({len(append_sw_codes)}个)")
            
            # 返回成功响应 - 格式与股票数据同步界面保持一致
            return JsonResponse({
                'success': True,
                'message': f'成功获取追加目录的申万指数代码列表，共{len(all_sw_codes)}个指数代码',
                'stock_codes': all_sw_codes,
                'main_path_count': len(main_sw_codes),
                'append_path_count': len(append_sw_codes),
                'total_count': len(all_sw_codes),
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                # 额外添加与股票数据同步界面类似的字段
                'main_dir_file_count': len(main_sw_codes),
                'append_dir_file_count': len(append_sw_codes),
                'merged_file_count': len(all_sw_codes),
                # 格式化的代码列表，用于前端显示
                'formatted_stock_codes': ','.join(all_sw_codes)
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
            logger.exception("合并申万指数代码时发生异常")
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
            
            # 检查追加目录文件是否存在
            if not os.path.exists(append_csv_path):
                return JsonResponse({
                    'success': False,
                    'message': f'追加目录中未找到股票{stock_code}的CSV文件'
                }, status=404)
            
            # 读取两个CSV文件的内容
            main_data = []
            append_data = []
            main_file_exists = os.path.exists(main_csv_path)
            
            # 读取主目录CSV（如果存在）
            if main_file_exists:
                try:
                    with open(main_csv_path, 'r', encoding='utf-8') as f:
                        main_data = f.readlines()
                except Exception as e:
                    logger.error(f"读取主目录CSV文件失败: {str(e)}")
                    return JsonResponse({
                        'success': False,
                        'message': f'读取主目录CSV文件失败: {str(e)}'
                    }, status=500)
            else:
                logger.info(f"主目录中未找到股票{stock_code}的CSV文件，将直接使用追加目录文件")
            
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
            
            # 合并数据
            
            # 如果主目录数据为空，直接使用追加目录的数据
            if len(main_data) == 0:
                merged_data = append_data.copy()
                new_lines_count = len(append_data) - 1 if len(append_data) > 1 else 0
                logger.info(f"主目录数据为空，直接使用追加目录的{len(append_data)}行数据")
            else:
                # 检查两个文件是否有相同的表头
                if len(append_data) > 0:
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
            
            # 对合并后的数据进行排序（先按复权方式，再按日期）
            if len(merged_data) > 1:
                # 提取表头
                header = merged_data[0]
                # 提取数据行
                data_lines = merged_data[1:]
                
                # 尝试解析表头，找到复权方式和日期列的索引
                header_parts = header.strip().split(',')
                adjust_type_idx = -1
                trade_date_idx = -1
                for i, col in enumerate(header_parts):
                    if 'adjust_type' in col.lower():
                        adjust_type_idx = i
                    elif 'trade_date' in col.lower():
                        trade_date_idx = i
                
                # 如果找到了必要的列，则进行排序
                if adjust_type_idx >= 0 and trade_date_idx >= 0:
                    # 定义排序键函数
                    def sort_key(line):
                        parts = line.strip().split(',')
                        # 处理可能的格式问题
                        if len(parts) > max(adjust_type_idx, trade_date_idx):
                            # 复权类型排序（空字符串优先）
                            adjust_key = parts[adjust_type_idx] if adjust_type_idx < len(parts) else ''
                            # 日期排序
                            date_key = parts[trade_date_idx] if trade_date_idx < len(parts) else ''
                            return (adjust_key, date_key)
                        return ('', '')
                    
                    # 对数据行进行排序
                    data_lines.sort(key=sort_key)
                    # 重新组合表头和排序后的数据行
                    merged_data = [header] + data_lines
                    logger.info(f"已对股票{stock_code}的合并数据进行排序")
            
            # 保存合并后的数据到主目录
            try:
                backup_created = False
                # 只有当主目录文件存在时才创建备份
                if main_file_exists:
                    # 先创建备份
                    backup_path = os.path.join(main_full_path, f'{stock_code}_backup_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv')
                    with open(backup_path, 'w', encoding='utf-8') as f:
                        f.writelines(main_data)
                    backup_created = True
                
                # 写入合并后的数据
                with open(main_csv_path, 'w', encoding='utf-8') as f:
                    f.writelines(merged_data)
                
                # 合并成功后删除备份文件
                if backup_created:
                    try:
                        if os.path.exists(backup_path):
                            os.remove(backup_path)
                            logger.info(f"已删除股票{stock_code}的备份文件: {backup_path}")
                            backup_created = False
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
            if main_file_exists:
                logger.info(f"成功合并股票{stock_code}的数据：主目录({len(main_data)}行) + 追加目录({len(append_data)-1}行) - 重复行 = 合并后({len(merged_data)}行)，新增{new_lines_count}行")
            else:
                logger.info(f"成功创建股票{stock_code}的CSV文件：从追加目录复制{len(append_data)}行数据")
            
            # 返回成功响应
            response_message = f'股票{stock_code}数据合并成功，过滤了{(len(append_data)-1)-new_lines_count}行重复数据' if main_file_exists else f'股票{stock_code}数据文件创建成功，从追加目录复制了所有数据'
            
            return JsonResponse({
                'success': True,
                'message': response_message,
                'stock_code': stock_code,
                'main_file_lines': len(main_data),
                'append_file_lines': len(append_data),
                'merged_file_lines': len(merged_data),
                'new_lines_added': new_lines_count,
                'duplicate_lines_filtered': (len(append_data)-1)-new_lines_count if len(append_data) > 1 else 0,
                'backup_created': backup_created,
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
            logger.exception(f"处理股票{stock_code}数据时发生异常")
            return JsonResponse({
                'success': False,
                'message': f'处理失败: {str(e)}'
            }, status=500)


class MergeSWIndexData(APIView):
    """
    合并单个申万指数代码的CSV文件，从追加目录合并到主目录
    URL: /api/restore/sw_mergeItem/
    方法: POST
    请求体: {
        "main_path": "data/sw_index",
        "append_path": "data/sw_index_append",
        "code": "801010.SI"
    }
    """
    def post(self, request):
        try:
            # 解析请求体
            data = json.loads(request.body)
            main_path = data.get('main_path')
            append_path = data.get('append_path')
            code = data.get('code')  # 注意这里使用code而不是stock_code，因为申万指数代码格式不同
            
            # 验证必要参数
            if not all([main_path, append_path, code]):
                return JsonResponse({
                    'success': False,
                    'message': '请提供完整的参数：main_path, append_path, code'
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
            main_csv_path = os.path.join(main_full_path, f'{code}.csv')
            append_csv_path = os.path.join(append_full_path, f'{code}.csv')
            
            # 检查追加目录文件是否存在
            if not os.path.exists(append_csv_path):
                return JsonResponse({
                    'success': False,
                    'message': f'追加目录中未找到申万指数{code}的CSV文件'
                }, status=404)
            
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
            
            # 主目录文件不存在时的处理
            if not os.path.exists(main_csv_path):
                # 记录日志
                logger.info(f"主目录中未找到申万指数{code}的CSV文件，将直接复制追加目录文件")
                
                # 复制追加目录的文件到主目录
                try:
                    # 先备份目录（如果需要）
                    backup_dir = os.path.join(allowed_base, 'data', 'backup')
                    if not os.path.exists(backup_dir):
                        os.makedirs(backup_dir)
                    
                    # 直接使用追加目录的数据作为合并数据
                    merged_data = append_data.copy()
                    
                    # 保存数据到主目录
                    with open(main_csv_path, 'w', encoding='utf-8') as f:
                        f.writelines(merged_data)
                    
                    # 返回成功响应
                    return JsonResponse({
                        'success': True,
                        'message': f'申万指数{code}数据已从追加目录直接复制到主目录',
                        'data': {
                            'total_rows': len(merged_data),
                            'new_rows': len(merged_data) - 1 if len(merged_data) > 0 else 0,  # 减去表头
                            'duplicate_rows': 0
                        }
                    })
                except Exception as e:
                    logger.error(f"复制申万指数{code}文件失败: {str(e)}")
                    return JsonResponse({
                        'success': False,
                        'message': f'复制文件失败: {str(e)}'
                    }, status=500)
            
            # 读取主目录CSV（主目录文件存在时）
            main_data = []
            try:
                with open(main_csv_path, 'r', encoding='utf-8') as f:
                    main_data = f.readlines()
            except Exception as e:
                logger.error(f"读取主目录CSV文件失败: {str(e)}")
                return JsonResponse({
                    'success': False,
                    'message': f'读取主目录CSV文件失败: {str(e)}'
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
                    logger.warning(f"申万指数{code}的两个CSV文件表头不一致")
            
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
            
            # 对合并后的数据进行排序（尝试按日期排序）
            if len(merged_data) > 1:
                # 提取表头
                header = merged_data[0]
                # 提取数据行
                data_lines = merged_data[1:]
                
                # 尝试解析表头，找到日期列的索引
                header_parts = header.strip().split(',')
                date_idx = -1
                for i, col in enumerate(header_parts):
                    if 'date' in col.lower() or 'trade_date' in col.lower():
                        date_idx = i
                        break
                
                # 如果找到了日期列，则按日期排序
                if date_idx >= 0:
                    # 定义排序键函数
                    def sort_key(line):
                        parts = line.strip().split(',')
                        # 处理可能的格式问题
                        if len(parts) > date_idx:
                            # 日期排序
                            date_key = parts[date_idx] if date_idx < len(parts) else ''
                            return date_key
                        return ''
                    
                    # 对数据行进行排序
                    data_lines.sort(key=sort_key)
                    # 重新组合表头和排序后的数据行
                    merged_data = [header] + data_lines
                    logger.info(f"已对申万指数{code}的合并数据进行排序")
            
            # 保存合并后的数据到主目录
            try:
                # 先创建备份
                backup_path = os.path.join(main_full_path, f'{code}_backup_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv')
                with open(backup_path, 'w', encoding='utf-8') as f:
                    f.writelines(main_data)
                
                # 写入合并后的数据
                with open(main_csv_path, 'w', encoding='utf-8') as f:
                    f.writelines(merged_data)
                
                # 合并成功后删除备份文件
                try:
                    if os.path.exists(backup_path):
                        os.remove(backup_path)
                        logger.info(f"已删除申万指数{code}的备份文件: {backup_path}")
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
            logger.info(f"成功合并申万指数{code}的数据：主目录({len(main_data)}行) + 追加目录({len(append_data)-1}行) - 重复行 = 合并后({len(merged_data)}行)，新增{new_lines_count}行")
            
            # 返回成功响应
            return JsonResponse({
                'success': True,
                'message': f'申万指数{code}数据合并成功，过滤了{(len(append_data)-1)-new_lines_count}行重复数据',
                'code': code,
                'main_file_lines': len(main_data),
                'append_file_lines': len(append_data),
                'merged_file_lines': len(merged_data),
                'new_lines_added': new_lines_count,
                'duplicate_lines_filtered': (len(append_data)-1)-new_lines_count if len(append_data) > 1 else 0,
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
            logger.exception(f"合并申万指数{code}数据时发生异常")
            return JsonResponse({
                'success': False,
                'message': f'处理失败: {str(e)}'
            }, status=500)