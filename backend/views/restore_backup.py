from django.http import JsonResponse
from rest_framework.views import APIView
import os
import json
from datetime import datetime
import logging
from django.conf import settings
import requests

# 导入全局路径处理工具
from stocks.utils import normalize_path, join_path, safe_join, get_backup_dir

# 配置日志记录器
logger = logging.getLogger(__name__)

# 从FileConfig类读取配置
from global_config.file_config import FileConfig

# 加载配置
db_config = FileConfig.get('database', {})

# 如果配置为空，设置默认配置（ClickHouse默认配置）
if not db_config:
    db_config = {
        "host": "localhost",
        "port": 9000,
        "httpport": 8123,
        "user": "default",
        "password": "",
        "database": "default"
    }
    # 保存到配置文件
    FileConfig.set('database', db_config)


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
            base_dir = getattr(settings, 'BASE_DIR', os.getcwd())
            allowed_base = join_path(base_dir, 'data')
            
            # 规范化路径
            if normalize_path(path).startswith('data/'):
                full_path = join_path(base_dir, path)
            else:
                full_path = join_path(allowed_base, path)
            
            # 安全检查
            full_path = safe_join(allowed_base, full_path.replace(allowed_base, '').lstrip('/'))
            
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
            
            for filename in os.listdir(full_path):
                code = os.path.splitext(filename)[0]
                
                # 简单验证：股票代码通常是6位数字
                if (code.isdigit() and len(code) == 6) or (code.endswith('.SI') and code[:-3].isdigit() and len(code[:-3]) == 6):
                    stock_codes.append(code)
            
            logger.info(f"在路径 {path} 下找到 {len(stock_codes)} 个股票代码文件")
            
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


def _analyze_clickhouse_response(response):
    """专业分析ClickHouse响应"""
    
    if response.status_code == 200:
        resp_text = response.text
        
        if not resp_text or resp_text.strip() == '':
            print(f"✅ ClickHouse导入成功: 数据已成功插入")
            return True
        
        try:
            resp_json = json.loads(resp_text)
            if isinstance(resp_json, dict):
                if 'error' in resp_json:
                    print(f"❌ ClickHouse JSON响应错误: {resp_json['error']}")
                    return False
                rows_affected = resp_json.get('RowsAffected', 0)
                if rows_affected > 0:
                    print(f"✅ ClickHouse导入成功: 成功插入 {rows_affected} 行数据")
                    return True
                else:
                    print(f"⚠️  警告: ClickHouse没有导入任何数据行")
                    return False
        except json.JSONDecodeError:
            pass
        
        resp_text = resp_text.strip()
        if resp_text:
            print(f"❌ ClickHouse导入失败: {resp_text}")
            return False
        
        return True
    elif response.status_code == 400:
        error_msg = response.text
        try:
            resp_json = json.loads(response.text)
            error_msg = resp_json.get('error', error_msg)
        except json.JSONDecodeError:
            pass
        print(f"❌ ClickHouse客户端请求错误: {error_msg}")
        return False
    elif response.status_code == 500:
        error_msg = response.text
        try:
            resp_json = json.loads(response.text)
            error_msg = resp_json.get('error', error_msg)
        except json.JSONDecodeError:
            pass
        print(f"❌ ClickHouse服务器错误: {error_msg}")
        return False
    else:
        error_msg = response.text
        try:
            resp_json = json.loads(response.text)
            error_msg = resp_json.get('error', error_msg)
        except json.JSONDecodeError:
            pass
        print(f"❌ ClickHouse意外HTTP状态: {response.status_code} - {error_msg}")
        return False


def import_csv_to_database(csv_file_path, table_name, schema=None, timestamp_col='date', partition_by='DAY', delimiter=',', force_header=True, atomic=True):
    """
    将CSV文件导入到ClickHouse数据库
    
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
        db_host = db_config.get('host', 'localhost')
        http_port = db_config.get('httpport', 8123)
        user = db_config.get('user', 'default')
        password = db_config.get('password', '')
        database = db_config.get('database', 'default')
        
        # 使用全局join_path处理ClickHouse URL路径
        import_url = f"http://{db_host}:{http_port}/"
        
        with open(csv_file_path, 'rb') as f:
            csv_content = f.read()
        
        if schema:
            insert_clause = f"INSERT INTO {table_name} {schema}"
        else:
            insert_clause = f"INSERT INTO {table_name}"
            
        sql = f"{insert_clause} SETTINGS \
            max_partitions_per_insert_block=1000, \
            format_csv_allow_double_quotes=1, \
            format_csv_delimiter=',', \
            format_csv_allow_single_quotes=0, \
            input_format_csv_skip_first_lines=1 \
            FORMAT CSV"
        
        logger.info(f"调用ClickHouse导入服务: {import_url}，表名: {table_name}")
        logger.info(f"执行SQL: {sql}")
        
        response = requests.post(
            import_url,
            params={
                'query': sql,
                'user': user,
                'password': password,
                'database': database
            },
            data=csv_content,
            headers={
                'Content-Type': 'text/csv'
            },
            timeout=600,
            proxies={"http": None, "https": None}
        )
        
        is_success = _analyze_clickhouse_response(response)
        if is_success:
            logger.info(f"CSV文件 {os.path.basename(csv_file_path)} 导入ClickHouse成功")
            return True, "导入成功", {"response_text": response.text}
        else:
            error_message = f"导入失败: ClickHouse返回错误"
            try:
                resp_json = json.loads(response.text)
                if isinstance(resp_json, dict) and 'error' in resp_json:
                    error_message = f"导入失败: {resp_json['error']}"
                else:
                    error_message = f"导入失败: {response.text}"
            except json.JSONDecodeError:
                error_message = f"导入失败: {response.text}"
            
            logger.error(f"CSV文件导入ClickHouse失败: {response.text}")
            return False, error_message, None
            
    except requests.exceptions.ConnectionError:
        error_message = "无法连接到ClickHouse服务，请检查服务是否运行"
        logger.exception(f"调用ClickHouse导入服务时发生连接异常")
        return False, error_message, None
    except requests.exceptions.Timeout:
        error_message = "连接ClickHouse服务超时，请检查服务响应情况"
        logger.exception(f"调用ClickHouse导入服务时发生超时异常")
        return False, error_message, None
    except requests.exceptions.RequestException as e:
        logger.exception(f"调用ClickHouse导入服务时发生请求异常")
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
    请求体: {"code": "000001", "path": "data/daily","table_name":"stock_daily"}
    """
    def post(self, request):
        try:
            data = json.loads(request.body)
            stock_code = data.get('code')
            stock_path = data.get('path')
            table_name = data.get('table_name')
            
            if not table_name:
                return JsonResponse({
                    'success': False,
                    'message': '缺少表名参数'
                }, status=400)
            
            if not (
                (stock_code.isdigit() and len(stock_code) == 6) or
                (stock_code.endswith('.SI') and stock_code[:-3].isdigit() and len(stock_code[:-3]) == 6)
            ):
                return JsonResponse({
                    'success': False,
                    'message': '无效的股票代码格式'
                }, status=400)
            
            if not stock_path:
                return JsonResponse({
                    'success': False,
                    'message': '缺少路径参数'
                }, status=400)
            
            logger.info(f"接收到股票代码 {stock_code} 的处理请求，路径: {stock_path}，表名: {table_name}")
            
            base_dir = getattr(settings, 'BASE_DIR', os.getcwd())
            allowed_base = join_path(base_dir, 'data')
            
            if normalize_path(stock_path).startswith('data/'):
                full_path = join_path(base_dir, stock_path)
            else:
                full_path = join_path(allowed_base, stock_path)
            
            full_path = safe_join(allowed_base, full_path.replace(allowed_base, '').lstrip('/'))
            
            csv_file_path = join_path(full_path, f"{stock_code}.csv")
            
            if not os.path.exists(csv_file_path):
                return JsonResponse({
                    'success': False,
                    'message': f'文件不存在: {csv_file_path}'
                }, status=404)
            
            if table_name == 'stock_daily_all' or table_name == 'stock_daily':
                stock_schema = "(code, date, open, close, high, low, volume, amount, turnover, outstanding_share)"
                timestamp_col = "date"
            elif table_name == 'sw_index':
                stock_schema = "(IndustryCode,date,lyrPe,lyrPeQuantile,ttmPe,ttmPeQuantile,pb,pbQuantile,dvRatio,dvRatioQuantile,dvTtm,dvTtmQuantile,addLyrPe,addLyrPeQuantile,addTtmPe,addTtmPeQuantile,addPb,addPbQuantile,addDvRatio,addDvTtm,turnoverRate,turnoverRateF,addTurnoverRate,addTurnoverRateF,turnoverRateFQuantile,totalMv,close,addClose,middleLyrPe,middleLyrPeQuantile,middleTtmPe,middleTtmPeQuantile,middlePb,middlePbQuantile,belowNetAssetPercent,belowNetAssetCount,total,value5,value10,value20,value60,indexClose,amount,amountCongestion,amountCongestionQuantile)"
                timestamp_col = "date"
            elif table_name == 'fq_factor':
                stock_schema = "(code, date, hfq, qfq)"
                timestamp_col = "date"
            elif table_name == 'stock_fin':
                stock_schema = "(code, date, CM_NPAS, CM_TOR, CM_OC, CM_NP, CM_NRNP, CM_TSE_NA, CM_GW, CM_NOCF, CM_BEPS, CM_NAPS, CM_CFPS, CM_ROE, CM_ROA, CM_GM, CM_NPM, CM_PER, CM_ALR, PSI_BEPS, PSI_DEPS, PSI_DEPS_LSC, PSI_DNAPS_PSC, PSI_ANAPS_PSC, PSI_NAPS_LSC, PSI_OCFPS, PSI_NCFPS, PSI_FCFFPS, PSI_FCFEPS, PSI_UPPS, PSI_CRPS, PSI_SRPS, PSI_REPS, PSI_ORPS, PSI_TORPS, PSI_EBITPS, PCP_ROE, PCP_DROE, PCP_AROE, PCP_AROE_ENR, PCP_DROE_ENR, PCP_EBITM, PCP_ROA, PCP_ROTC, PCP_ROIC, PCP_AROAAt_EI, PCP_GM, PCP_NPM, PCP_CEPR, PCP_OPM, PCP_ANPMTA, PCP_ANPMTA_IMI, GCP_NPAS, GCP_TOR, GCP_NP, GCP_NRNP, GCP_TORGR, GCP_GRNPAPC, EQL_NOCF_SR, EQL_NOCF_TOR, EQL_CER, EQL_PER, EQL_CSR, EQL_NOCF_NPAPC, EQL_IT_TP, FR_CR, FR_QR, FR_CQR, FR_ALR, FR_EM, FR_EM_IMINA, FR_DER, FR_CashR, OCP_ART, OCP_ARTD, OCP_IT, OCP_ITD, OCP_TAT, OCP_TATD, OCP_CAT, OCP_CATD, OCP_APT)"
                timestamp_col = "date"
            elif table_name == 'stock_index':
                stock_schema = "(code, name, index_code, index_name)"
                timestamp_col = "code"
            
            success, message, data = import_csv_to_database(
                csv_file_path=csv_file_path,
                table_name=table_name,
                schema=stock_schema,
                timestamp_col=timestamp_col,
                partition_by="DAY",
                delimiter=",",
                force_header=True,
                atomic=True
            )
            
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
            data = json.loads(request.body)
            main_path = data.get('main_path')
            append_path = data.get('append_path')
            
            if not main_path or not append_path:
                return JsonResponse({
                    'success': False,
                    'message': '缺少必要的路径参数'
                }, status=400)
            
            base_dir = getattr(settings, 'BASE_DIR', os.getcwd())
            allowed_base = join_path(base_dir, 'data')
            
            def normalize_and_validate_path(path):
                if normalize_path(path).startswith('data/'):
                    full_path = join_path(base_dir, path)
                else:
                    full_path = join_path(allowed_base, path)
                full_path = safe_join(allowed_base, full_path.replace(allowed_base, '').lstrip('/'))
                
                if not os.path.exists(full_path) or not os.path.isdir(full_path):
                    raise ValueError(f'无效的路径: {path}')
                
                return full_path
            
            try:
                main_full_path = normalize_and_validate_path(main_path)
                append_full_path = normalize_and_validate_path(append_path)
            except ValueError as e:
                return JsonResponse({
                    'success': False,
                    'message': str(e)
                }, status=400)
            
            def get_stock_codes_from_dir(dir_path):
                stock_codes = []
                try:
                    for filename in os.listdir(dir_path):
                        code = os.path.splitext(filename)[0]
                        if code.isdigit() and len(code) == 6:
                            stock_codes.append(code)
                except Exception as e:
                    logger.error(f"读取目录 {dir_path} 时发生错误: {str(e)}")
                    raise ValueError(f"读取目录失败: {str(e)}")
                
                return sorted(stock_codes)
            
            main_stock_codes = get_stock_codes_from_dir(main_full_path)
            append_stock_codes = get_stock_codes_from_dir(append_full_path)
            
            all_stock_codes = append_stock_codes
            
            logger.info(f"使用追加目录的股票代码：追加目录共有({len(all_stock_codes)}个)股票代码")
            
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
            data = json.loads(request.body)
            main_path = data.get('main_path')
            append_path = data.get('append_path')
            
            if not main_path or not append_path:
                return JsonResponse({
                    'success': False,
                    'message': '缺少必要的路径参数'
                }, status=400)
            
            base_dir = getattr(settings, 'BASE_DIR', os.getcwd())
            allowed_base = join_path(base_dir, 'data')
            
            def normalize_and_validate_path(path):
                if normalize_path(path).startswith('data/'):
                    full_path = join_path(base_dir, path)
                else:
                    full_path = join_path(allowed_base, path)
                full_path = safe_join(allowed_base, full_path.replace(allowed_base, '').lstrip('/'))
                
                if not os.path.exists(full_path) or not os.path.isdir(full_path):
                    raise ValueError(f'无效的路径: {path}')
                
                return full_path
            
            try:
                main_full_path = normalize_and_validate_path(main_path)
                append_full_path = normalize_and_validate_path(append_path)
            except ValueError as e:
                return JsonResponse({
                    'success': False,
                    'message': str(e)
                }, status=400)
            
            def get_sw_codes_from_dir(dir_path):
                sw_codes = []
                try:
                    for filename in os.listdir(dir_path):
                        code = os.path.splitext(filename)[0]
                        sw_codes.append(code)
                except Exception as e:
                    logger.error(f"读取目录 {dir_path} 时发生错误: {str(e)}")
                    raise ValueError(f"读取目录失败: {str(e)}")
                
                return sorted(sw_codes)
            
            main_sw_codes = get_sw_codes_from_dir(main_full_path)
            append_sw_codes = get_sw_codes_from_dir(append_full_path)
            
            all_sw_codes = append_sw_codes
            
            logger.info(f"获取申万指数代码列表：追加目录({len(append_sw_codes)}个)")
            
            return JsonResponse({
                'success': True,
                'message': f'成功获取追加目录的申万指数代码列表，共{len(all_sw_codes)}个指数代码',
                'stock_codes': all_sw_codes,
                'main_path_count': len(main_sw_codes),
                'append_path_count': len(append_sw_codes),
                'total_count': len(all_sw_codes),
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'main_dir_file_count': len(main_sw_codes),
                'append_dir_file_count': len(append_sw_codes),
                'merged_file_count': len(all_sw_codes),
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
            data = json.loads(request.body)
            main_path = data.get('main_path')
            append_path = data.get('append_path')
            stock_code = data.get('stock_code')
            
            if not all([main_path, append_path, stock_code]):
                return JsonResponse({
                    'success': False,
                    'message': '请提供完整的参数：main_path, append_path, stock_code'
                }, status=400)
            
            if not stock_code.isdigit() or len(stock_code) != 6:
                return JsonResponse({
                    'success': False,
                    'message': '股票代码必须是6位数字'
                }, status=400)
            
            allowed_base = str(settings.BASE_DIR)
            
            def normalize_and_validate_path(path):
                if path.startswith('/') or path.startswith('\\'):
                    raise ValueError(f'不允许使用绝对路径: {path}')
                
                full_path = join_path(allowed_base, path)
                full_path = safe_join(allowed_base, full_path.replace(allowed_base, '').lstrip('/'))
                
                if not os.path.exists(full_path) or not os.path.isdir(full_path):
                    raise ValueError(f'无效的路径: {path}')
                
                return full_path
            
            try:
                main_full_path = normalize_and_validate_path(main_path)
                append_full_path = normalize_and_validate_path(append_path)
            except ValueError as e:
                return JsonResponse({
                    'success': False,
                    'message': str(e)
                }, status=400)
            
            main_csv_path = join_path(main_full_path, f"{stock_code}.csv")
            append_csv_path = join_path(append_full_path, f"{stock_code}.csv")
            
            if not os.path.exists(append_csv_path):
                return JsonResponse({
                    'success': False,
                    'message': f'追加目录中未找到股票{stock_code}的CSV文件'
                }, status=404)
            
            main_data = []
            main_file_exists = os.path.exists(main_csv_path)
            
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
            
            append_data = []
            try:
                with open(append_csv_path, 'r', encoding='utf-8') as f:
                    append_data = f.readlines()
            except Exception as e:
                logger.error(f"读取追加目录CSV文件失败: {str(e)}")
                return JsonResponse({
                    'success': False,
                    'message': f'读取追加目录CSV文件失败: {str(e)}'
                }, status=500)
            
            if len(main_data) == 0:
                merged_data = append_data.copy()
                new_lines_count = len(append_data) - 1 if len(append_data) > 1 else 0
                logger.info(f"主目录数据为空，直接使用追加目录的{len(append_data)}行数据")
            else:
                if len(append_data) > 0:
                    if main_data[0].strip() != append_data[0].strip():
                        logger.warning(f"股票{stock_code}的两个CSV文件表头不一致")
                
                merged_data = main_data.copy()
                
                if len(append_data) > 1:
                    main_data_set = set(main_data[1:]) if len(main_data) > 1 else set()
                    new_lines_count = 0
                    for line in append_data[1:]:
                        if line not in main_data_set:
                            merged_data.append(line)
                            new_lines_count += 1
                else:
                    new_lines_count = 0
            
            if len(merged_data) > 1:
                header = merged_data[0]
                data_lines = merged_data[1:]
                
                header_parts = header.strip().split(',')
                date_idx = -1
                for i, col in enumerate(header_parts):
                    if 'date' in col.lower():
                        date_idx = i
                
                if date_idx >= 0:
                    def sort_key(line):
                        parts = line.strip().split(',')
                        if len(parts) > date_idx:
                            date_key = parts[date_idx] if date_idx < len(parts) else ''
                            return (date_key,)
                        return ('',)
                    
                    data_lines.sort(key=sort_key)
                    merged_data = [header] + data_lines
                    logger.info(f"已对股票{stock_code}的合并数据进行排序")
            
            try:
                backup_created = False
                if main_file_exists:
                    backup_path = join_path(main_full_path, f"{stock_code}_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
                    with open(backup_path, 'w', encoding='utf-8') as f:
                        f.writelines(main_data)
                    backup_created = True
                
                with open(main_csv_path, 'w', encoding='utf-8') as f:
                    f.writelines(merged_data)
                
                if backup_created:
                    try:
                        if os.path.exists(backup_path):
                            os.remove(backup_path)
                            logger.info(f"已删除股票{stock_code}的备份文件: {backup_path}")
                            backup_created = False
                    except Exception as e:
                        logger.warning(f"删除备份文件失败: {str(e)}")
            except Exception as e:
                logger.error(f"保存合并后的CSV文件失败: {str(e)}")
                return JsonResponse({
                    'success': False,
                    'message': f'保存合并后的CSV文件失败: {str(e)}'
                }, status=500)
            
            if main_file_exists:
                logger.info(f"成功合并股票{stock_code}的数据：主目录({len(main_data)}行) + 追加目录({len(append_data)-1}行) - 重复行 = 合并后({len(merged_data)}行)，新增{new_lines_count}行")
            else:
                logger.info(f"成功创建股票{stock_code}的CSV文件：从追加目录复制{len(append_data)}行数据")
            
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
            data = json.loads(request.body)
            main_path = data.get('main_path')
            append_path = data.get('append_path')
            code = data.get('code')
            
            if not all([main_path, append_path, code]):
                return JsonResponse({
                    'success': False,
                    'message': '请提供完整的参数：main_path, append_path, code'
                }, status=400)
            
            allowed_base = str(settings.BASE_DIR)
            
            def normalize_and_validate_path(path):
                if path.startswith('/') or path.startswith('\\'):
                    raise ValueError(f'不允许使用绝对路径: {path}')
                
                full_path = join_path(allowed_base, path)
                full_path = safe_join(allowed_base, full_path.replace(allowed_base, '').lstrip('/'))
                
                if not os.path.exists(full_path) or not os.path.isdir(full_path):
                    raise ValueError(f'无效的路径: {path}')
                
                return full_path
            
            try:
                main_full_path = normalize_and_validate_path(main_path)
                append_full_path = normalize_and_validate_path(append_path)
            except ValueError as e:
                return JsonResponse({
                    'success': False,
                    'message': str(e)
                }, status=400)
            
            main_csv_path = join_path(main_full_path, f"{code}.csv")
            append_csv_path = join_path(append_full_path, f"{code}.csv")
            
            if not os.path.exists(append_csv_path):
                return JsonResponse({
                    'success': False,
                    'message': f'追加目录中未找到申万指数{code}的CSV文件'
                }, status=404)
            
            if not os.path.exists(main_csv_path):
                logger.info(f"主目录中未找到申万指数{code}的CSV文件，将直接复制追加目录文件")
                
                try:
                    backup_dir = join_path(allowed_base, 'data', 'backup')
                    if not os.path.exists(backup_dir):
                        os.makedirs(backup_dir)
                    
                    merged_data = []
                    with open(append_csv_path, 'r', encoding='utf-8') as f:
                        merged_data = f.readlines()
                    
                    with open(main_csv_path, 'w', encoding='utf-8') as f:
                        f.writelines(merged_data)
                    
                    return JsonResponse({
                        'success': True,
                        'message': f'申万指数{code}数据已从追加目录直接复制到主目录',
                        'data': {
                            'total_rows': len(merged_data),
                            'new_rows': len(merged_data) - 1 if len(merged_data) > 0 else 0,
                            'duplicate_rows': 0
                        }
                    })
                except Exception as e:
                    logger.error(f"复制申万指数{code}文件失败: {str(e)}")
                    return JsonResponse({
                        'success': False,
                        'message': f'复制文件失败: {str(e)}'
                    }, status=500)
            
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
            
            if not main_data:
                return JsonResponse({
                    'success': False,
                    'message': '主目录CSV文件为空'
                }, status=400)
            
            append_data = []
            try:
                with open(append_csv_path, 'r', encoding='utf-8') as f:
                    append_data = f.readlines()
            except Exception as e:
                logger.error(f"读取追加目录CSV文件失败: {str(e)}")
                return JsonResponse({
                    'success': False,
                    'message': f'读取追加目录CSV文件失败: {str(e)}'
                }, status=500)
            
            if len(main_data) > 0 and len(append_data) > 0:
                if main_data[0].strip() != append_data[0].strip():
                    logger.warning(f"申万指数{code}的两个CSV文件表头不一致")
            
            merged_data = main_data.copy()
            
            if len(append_data) > 1:
                main_data_set = set(main_data[1:]) if len(main_data) > 1 else set()
                new_lines_count = 0
                for line in append_data[1:]:
                    if line not in main_data_set:
                        merged_data.append(line)
                        new_lines_count += 1
            else:
                new_lines_count = 0
            
            if len(merged_data) > 1:
                header = merged_data[0]
                data_lines = merged_data[1:]
                
                header_parts = header.strip().split(',')
                date_idx = -1
                for i, col in enumerate(header_parts):
                    if 'date' in col.lower() or 'date' in col.lower():
                        date_idx = i
                        break
                
                if date_idx >= 0:
                    def sort_key(line):
                        parts = line.strip().split(',')
                        if len(parts) > date_idx:
                            date_key = parts[date_idx] if date_idx < len(parts) else ''
                            return date_key
                        return ''
                    
                    data_lines.sort(key=sort_key)
                    merged_data = [header] + data_lines
                    logger.info(f"已对申万指数{code}的合并数据进行排序")
            
            try:
                backup_path = join_path(main_full_path, f"{code}_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
                with open(backup_path, 'w', encoding='utf-8') as f:
                    f.writelines(main_data)
                
                with open(main_csv_path, 'w', encoding='utf-8') as f:
                    f.writelines(merged_data)
                
                try:
                    if os.path.exists(backup_path):
                        os.remove(backup_path)
                        logger.info(f"已删除申万指数{code}的备份文件: {backup_path}")
                except Exception as e:
                    logger.warning(f"删除备份文件失败: {str(e)}")
            except Exception as e:
                logger.error(f"保存合并后的CSV文件失败: {str(e)}")
                return JsonResponse({
                    'success': False,
                    'message': f'保存合并后的CSV文件失败: {str(e)}'
                }, status=500)
            
            logger.info(f"成功合并申万指数{code}的数据：主目录({len(main_data)}行) + 追加目录({len(append_data)-1}行) - 重复行 = 合并后({len(merged_data)}行)，新增{new_lines_count}行")
            
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
