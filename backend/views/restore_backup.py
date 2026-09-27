from django.http import JsonResponse
from rest_framework.views import APIView
import os
import json
import csv
import tempfile
import threading
import uuid
from datetime import datetime
import logging
from django.conf import settings
import requests

# 导入全局路径处理工具
from stocks.utils import normalize_path, join_path, safe_join, get_backup_dir

# 配置日志记录器
logger = logging.getLogger(__name__)

# 批量合并任务只保存在当前进程内，避免网页请求长时间占用连接。
_merge_jobs = {}
_merge_jobs_lock = threading.RLock()
_active_merge_job_id = None

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


def import_csv_to_database_partitioned(csv_file_path, table_name, schema=None, timestamp_col='date', partition_column='code', batch_partition_limit=500):
    """
    对按高基数维度分区的表进行分批导入，避免单次 INSERT 命中过多分区。
    当前主要用于 fq_factor 这类按 code 分区的表。
    """
    source_dir = os.path.dirname(csv_file_path) or '/private/tmp'
    batch_index = 0
    imported_batches = 0
    imported_rows = 0

    def start_batch(fieldnames):
        temp_file = tempfile.NamedTemporaryFile(
            mode='w',
            encoding='utf-8',
            newline='',
            suffix='.csv',
            prefix=f'{table_name}_batch_',
            dir=source_dir,
            delete=False
        )
        writer = csv.DictWriter(temp_file, fieldnames=fieldnames)
        writer.writeheader()
        return temp_file, writer

    def flush_batch(temp_file, row_count, partition_count, current_batch_index):
        nonlocal imported_batches, imported_rows
        if not temp_file or row_count == 0:
            return True, '空批次，跳过', None

        temp_file.close()
        try:
            success, message, data = import_csv_to_database(
                csv_file_path=temp_file.name,
                table_name=table_name,
                schema=schema,
                timestamp_col=timestamp_col,
                partition_by='DAY',
                delimiter=',',
                force_header=True,
                atomic=True
            )
            if not success:
                return False, f'第 {current_batch_index} 批导入失败（{partition_count} 个分区，{row_count} 行）: {message}', None

            imported_batches += 1
            imported_rows += row_count
            logger.info(
                "分批导入成功，表名: %s，批次: %s，分区数: %s，行数: %s",
                table_name,
                current_batch_index,
                partition_count,
                row_count
            )
            return True, message, data
        finally:
            try:
                os.remove(temp_file.name)
            except OSError:
                logger.warning("删除临时分批CSV失败: %s", temp_file.name)

    with open(csv_file_path, 'r', encoding='utf-8-sig', newline='') as source_file:
        reader = csv.DictReader(source_file)
        fieldnames = reader.fieldnames

        if not fieldnames:
            return False, 'CSV 文件为空，无法分批导入', None

        if partition_column not in fieldnames:
            return False, f'CSV 缺少分区字段 {partition_column}，无法分批导入', None

        temp_file = None
        writer = None
        current_partitions = set()
        current_rows = 0
        last_partition_value = None

        try:
            for row in reader:
                if not row or not any(str(value).strip() for value in row.values()):
                    continue

                partition_value = str(row.get(partition_column) or '').strip()
                if not partition_value:
                    return False, f'存在缺少 {partition_column} 的数据行，无法分批导入', None

                if temp_file is None:
                    batch_index += 1
                    temp_file, writer = start_batch(fieldnames)

                is_new_partition = partition_value != last_partition_value
                if is_new_partition and partition_value not in current_partitions and len(current_partitions) >= batch_partition_limit:
                    success, message, data = flush_batch(temp_file, current_rows, len(current_partitions), batch_index)
                    if not success:
                        return success, message, data

                    batch_index += 1
                    temp_file, writer = start_batch(fieldnames)
                    current_partitions = set()
                    current_rows = 0

                if partition_value not in current_partitions:
                    current_partitions.add(partition_value)

                writer.writerow(row)
                current_rows += 1
                last_partition_value = partition_value

            if temp_file is not None:
                success, message, data = flush_batch(temp_file, current_rows, len(current_partitions), batch_index)
                if not success:
                    return success, message, data
        finally:
            if temp_file is not None and not temp_file.closed:
                temp_file.close()
                try:
                    os.remove(temp_file.name)
                except OSError:
                    pass

    return True, f'分批导入成功，共导入 {imported_batches} 批，{imported_rows} 行数据', {
        'batch_count': imported_batches,
        'row_count': imported_rows,
        'partition_column': partition_column,
        'batch_partition_limit': batch_partition_limit
    }


def _resolve_restore_dir(path):
    """将恢复目录限制在 backend/data 下，并返回安全路径。"""
    if not path:
        raise ValueError('缺少路径参数')

    base_dir = getattr(settings, 'BASE_DIR', os.getcwd())
    allowed_base = join_path(base_dir, 'data')

    if normalize_path(path).startswith('data/'):
        full_path = join_path(base_dir, path)
    else:
        full_path = join_path(allowed_base, path)

    full_path = safe_join(allowed_base, full_path.replace(allowed_base, '').lstrip('/'))
    return full_path


def _get_restore_schema(table_name):
    if table_name == 'stock_daily_all' or table_name == 'stock_daily':
        return "(code, date, open, close, high, low, volume, amount, turnover, outstanding_share)", "date"
    if table_name == 'sw_index':
        return "(IndustryCode,date,lyrPe,lyrPeQuantile,ttmPe,ttmPeQuantile,pb,pbQuantile,dvRatio,dvRatioQuantile,dvTtm,dvTtmQuantile,addLyrPe,addLyrPeQuantile,addTtmPe,addTtmPeQuantile,addPb,addPbQuantile,addDvRatio,addDvTtm,turnoverRate,turnoverRateF,addTurnoverRate,addTurnoverRateF,turnoverRateFQuantile,totalMv,close,addClose,middleLyrPe,middleLyrPeQuantile,middleTtmPe,middleTtmPeQuantile,middlePb,middlePbQuantile,belowNetAssetPercent,belowNetAssetCount,total,value5,value10,value20,value60,indexClose,amount,amountCongestion,amountCongestionQuantile)", "date"
    if table_name == 'fq_factor':
        return "(code, date, hfq, qfq)", "date"
    if table_name == 'stock_fin':
        return "(code, date, CM_NPAS, CM_TOR, CM_OC, CM_NP, CM_NRNP, CM_TSE_NA, CM_GW, CM_NOCF, CM_BEPS, CM_NAPS, CM_CFPS, CM_ROE, CM_ROA, CM_GM, CM_NPM, CM_PER, CM_ALR, PSI_BEPS, PSI_DEPS, PSI_DEPS_LSC, PSI_DNAPS_PSC, PSI_ANAPS_PSC, PSI_NAPS_LSC, PSI_OCFPS, PSI_NCFPS, PSI_FCFFPS, PSI_FCFEPS, PSI_UPPS, PSI_CRPS, PSI_SRPS, PSI_REPS, PSI_ORPS, PSI_TORPS, PSI_EBITPS, PCP_ROE, PCP_DROE, PCP_AROE, PCP_AROE_ENR, PCP_DROE_ENR, PCP_EBITM, PCP_ROA, PCP_ROTC, PCP_ROIC, PCP_AROAAt_EI, PCP_GM, PCP_NPM, PCP_CEPR, PCP_OPM, PCP_ANPMTA, PCP_ANPMTA_IMI, GCP_NPAS, GCP_TOR, GCP_NP, GCP_NRNP, GCP_TORGR, GCP_GRNPAPC, EQL_NOCF_SR, EQL_NOCF_TOR, EQL_CER, EQL_PER, EQL_CSR, EQL_NOCF_NPAPC, EQL_IT_TP, FR_CR, FR_QR, FR_CQR, FR_ALR, FR_EM, FR_EM_IMINA, FR_DER, FR_CashR, OCP_ART, OCP_ARTD, OCP_IT, OCP_ITD, OCP_TAT, OCP_TATD, OCP_CAT, OCP_CATD, OCP_APT)", "date"
    if table_name == 'stock_index':
        return "(code, name, index_code, index_name)", "code"
    raise ValueError(f'不支持的表名: {table_name}')


def _is_stock_restore_file(filename):
    code = os.path.splitext(filename)[0]
    return (
        (code.isdigit() and len(code) == 6) or
        (code.endswith('.SI') and code[:-3].isdigit() and len(code[:-3]) == 6)
    )


def _should_include_restore_csv(filename, table_name):
    if not filename.lower().endswith('.csv'):
        return False
    if filename.lower() == 'all.csv':
        return False

    if table_name in ('stock_daily', 'stock_daily_all', 'fq_factor', 'stock_fin', 'sw_index'):
        return _is_stock_restore_file(filename)

    if table_name == 'stock_index':
        return True

    return False


def _normalize_daily_row(row_dict, filename):
    """将不同格式的日线CSV统一映射到标准列。"""
    normalized = {
        'code': (row_dict.get('code') or row_dict.get('股票代码') or '').strip(),
        'date': (row_dict.get('date') or '').strip(),
        'open': (row_dict.get('open') or '').strip(),
        'close': (row_dict.get('close') or '').strip(),
        'high': (row_dict.get('high') or '').strip(),
        'low': (row_dict.get('low') or '').strip(),
        'volume': (row_dict.get('volume') or '').strip(),
        'amount': (row_dict.get('amount') or '').strip(),
        'turnover': (row_dict.get('turnover') or '').strip(),
        # 旧格式里没有 outstanding_share，回填 0，避免全量恢复被单个文件卡住
        'outstanding_share': (row_dict.get('outstanding_share') or '0').strip() or '0',
    }
    required_fields = ['code', 'date', 'open', 'close', 'high', 'low', 'volume', 'amount', 'turnover']
    missing_fields = [field for field in required_fields if normalized[field] == '']
    if missing_fields:
        raise ValueError(f'文件 {filename} 缺少必要列值: {", ".join(missing_fields)}')
    return normalized


def _coerce_sw_index_epoch_datetime_string(value, field_name, filename, row_dict):
    """兼容历史脏数据：某些 totalMv 会被错误写成 ISO 时间字符串。"""
    raw_value = (value or '').strip()
    if field_name != 'totalMv' or raw_value == '':
        return raw_value
    if 'T' not in raw_value or not raw_value.endswith('Z'):
        return raw_value

    try:
        numeric_value = datetime.fromisoformat(raw_value.replace('Z', '+00:00')).timestamp()
    except ValueError:
        return raw_value

    normalized_value = str(int(numeric_value)) if float(numeric_value).is_integer() else format(float(numeric_value), '.15g')
    logger.warning(
        "sw_index 文件 %s 的 %s 被错误保存为时间字符串，IndustryCode: %s，date: %s，原值: %s，已还原为 %s",
        filename,
        field_name,
        row_dict.get('IndustryCode') or row_dict.get('industryCode') or '',
        row_dict.get('date') or '',
        raw_value,
        normalized_value
    )
    return normalized_value


def _normalize_float_string(value, field_name, filename, row_dict, default='0'):
    """将浮点字段规范化；遇到脏值时回退默认值，避免整批恢复失败。"""
    raw_value = _coerce_sw_index_epoch_datetime_string(value, field_name, filename, row_dict)
    if raw_value == '':
        return default
    try:
        float(raw_value)
        return raw_value
    except (TypeError, ValueError):
        logger.warning(
            "sw_index 文件 %s 存在非法浮点值，字段: %s，IndustryCode: %s，date: %s，原值: %s，已回填为 %s",
            filename,
            field_name,
            row_dict.get('IndustryCode') or row_dict.get('industryCode') or '',
            row_dict.get('date') or '',
            raw_value,
            default
        )
        return default


def _normalize_int_string(value, field_name, filename, row_dict, default='0'):
    """将整数字段规范化；遇到脏值时回退默认值，避免整批恢复失败。"""
    raw_value = (value or '').strip()
    if raw_value == '':
        return default
    try:
        int(raw_value)
        return raw_value
    except (TypeError, ValueError):
        logger.warning(
            "sw_index 文件 %s 存在非法整数值，字段: %s，IndustryCode: %s，date: %s，原值: %s，已回填为 %s",
            filename,
            field_name,
            row_dict.get('IndustryCode') or row_dict.get('industryCode') or '',
            row_dict.get('date') or '',
            raw_value,
            default
        )
        return default


def _normalize_sw_index_row(row_dict, filename):
    """将不同格式的申万指数 CSV 统一映射到 ClickHouse 旧表结构。"""
    normalized = {
        'IndustryCode': (row_dict.get('IndustryCode') or row_dict.get('industryCode') or '').strip(),
        'date': (row_dict.get('date') or '').strip(),
        'lyrPe': _normalize_float_string(row_dict.get('lyrPe'), 'lyrPe', filename, row_dict),
        'lyrPeQuantile': _normalize_float_string(row_dict.get('lyrPeQuantile'), 'lyrPeQuantile', filename, row_dict),
        'ttmPe': _normalize_float_string(row_dict.get('ttmPe'), 'ttmPe', filename, row_dict),
        'ttmPeQuantile': _normalize_float_string(row_dict.get('ttmPeQuantile'), 'ttmPeQuantile', filename, row_dict),
        'pb': _normalize_float_string(row_dict.get('pb'), 'pb', filename, row_dict),
        'pbQuantile': _normalize_float_string(row_dict.get('pbQuantile'), 'pbQuantile', filename, row_dict),
        'dvRatio': _normalize_float_string(row_dict.get('dvRatio'), 'dvRatio', filename, row_dict),
        'dvRatioQuantile': _normalize_float_string(row_dict.get('dvRatioQuantile'), 'dvRatioQuantile', filename, row_dict),
        'dvTtm': _normalize_float_string(row_dict.get('dvTtm'), 'dvTtm', filename, row_dict),
        'dvTtmQuantile': _normalize_float_string(row_dict.get('dvTtmQuantile'), 'dvTtmQuantile', filename, row_dict),
        'addLyrPe': _normalize_float_string(row_dict.get('addLyrPe'), 'addLyrPe', filename, row_dict),
        'addLyrPeQuantile': _normalize_float_string(row_dict.get('addLyrPeQuantile'), 'addLyrPeQuantile', filename, row_dict),
        'addTtmPe': _normalize_float_string(row_dict.get('addTtmPe'), 'addTtmPe', filename, row_dict),
        'addTtmPeQuantile': _normalize_float_string(row_dict.get('addTtmPeQuantile'), 'addTtmPeQuantile', filename, row_dict),
        'addPb': _normalize_float_string(row_dict.get('addPb'), 'addPb', filename, row_dict),
        'addPbQuantile': _normalize_float_string(row_dict.get('addPbQuantile'), 'addPbQuantile', filename, row_dict),
        'addDvRatio': _normalize_float_string(row_dict.get('addDvRatio'), 'addDvRatio', filename, row_dict),
        'addDvTtm': _normalize_float_string(row_dict.get('addDvTtm'), 'addDvTtm', filename, row_dict),
        'turnoverRate': _normalize_float_string(row_dict.get('turnoverRate'), 'turnoverRate', filename, row_dict),
        'turnoverRateF': _normalize_float_string(row_dict.get('turnoverRateF'), 'turnoverRateF', filename, row_dict),
        'addTurnoverRate': _normalize_float_string(row_dict.get('addTurnoverRate'), 'addTurnoverRate', filename, row_dict),
        'addTurnoverRateF': _normalize_float_string(row_dict.get('addTurnoverRateF'), 'addTurnoverRateF', filename, row_dict),
        'turnoverRateFQuantile': _normalize_float_string(row_dict.get('turnoverRateFQuantile'), 'turnoverRateFQuantile', filename, row_dict),
        'totalMv': _normalize_float_string(row_dict.get('totalMv'), 'totalMv', filename, row_dict),
        'close': _normalize_float_string(row_dict.get('close'), 'close', filename, row_dict),
        'addClose': _normalize_float_string(row_dict.get('addClose'), 'addClose', filename, row_dict),
        'middleLyrPe': _normalize_float_string(row_dict.get('middleLyrPe'), 'middleLyrPe', filename, row_dict),
        'middleLyrPeQuantile': _normalize_float_string(row_dict.get('middleLyrPeQuantile'), 'middleLyrPeQuantile', filename, row_dict),
        'middleTtmPe': _normalize_float_string(row_dict.get('middleTtmPe'), 'middleTtmPe', filename, row_dict),
        'middleTtmPeQuantile': _normalize_float_string(row_dict.get('middleTtmPeQuantile'), 'middleTtmPeQuantile', filename, row_dict),
        'middlePb': _normalize_float_string(row_dict.get('middlePb'), 'middlePb', filename, row_dict),
        'middlePbQuantile': _normalize_float_string(row_dict.get('middlePbQuantile'), 'middlePbQuantile', filename, row_dict),
        'belowNetAssetPercent': _normalize_float_string(row_dict.get('belowNetAssetPercent'), 'belowNetAssetPercent', filename, row_dict),
        'belowNetAssetCount': _normalize_int_string(row_dict.get('belowNetAssetCount'), 'belowNetAssetCount', filename, row_dict),
        'total': _normalize_int_string(row_dict.get('total'), 'total', filename, row_dict),
        'value5': (row_dict.get('value5') or '0').strip() or '0',
        'value10': (row_dict.get('value10') or '0').strip() or '0',
        'value20': (row_dict.get('value20') or '0').strip() or '0',
        'value60': (row_dict.get('value60') or '0').strip() or '0',
        'indexClose': _normalize_float_string(row_dict.get('indexClose'), 'indexClose', filename, row_dict),
        'amount': (row_dict.get('amount') or '0').strip() or '0',
        'amountCongestion': (row_dict.get('amountCongestion') or '0').strip() or '0',
        'amountCongestionQuantile': _normalize_int_string(row_dict.get('amountCongestionQuantile'), 'amountCongestionQuantile', filename, row_dict),
    }
    required_fields = [
        'IndustryCode', 'date', 'lyrPe', 'lyrPeQuantile', 'ttmPe', 'ttmPeQuantile',
        'pb', 'pbQuantile', 'dvRatio', 'dvRatioQuantile', 'dvTtm', 'dvTtmQuantile',
        'addLyrPe', 'addLyrPeQuantile', 'addTtmPe', 'addTtmPeQuantile', 'addPb',
        'addPbQuantile', 'addDvRatio', 'addDvTtm', 'turnoverRate', 'turnoverRateF',
        'addTurnoverRate', 'addTurnoverRateF', 'turnoverRateFQuantile', 'totalMv',
        'close', 'addClose', 'middleLyrPe', 'middleLyrPeQuantile', 'middleTtmPe',
        'middleTtmPeQuantile', 'middlePb', 'middlePbQuantile', 'belowNetAssetPercent',
        'belowNetAssetCount', 'total'
    ]
    missing_fields = [field for field in required_fields if normalized[field] == '']
    if missing_fields:
        raise ValueError(f'文件 {filename} 缺少必要列值: {", ".join(missing_fields)}')
    return normalized


def _rewrite_csv_with_header(csv_file_path, target_header, row_normalizer, temp_prefix):
    """将 CSV 重写为统一表头的临时文件，便于后续按固定 schema 导入。"""
    source_dir = os.path.dirname(csv_file_path) or '/private/tmp'
    temp_file = tempfile.NamedTemporaryFile(
        mode='w',
        encoding='utf-8',
        newline='',
        suffix='.csv',
        prefix=temp_prefix,
        dir=source_dir,
        delete=False
    )
    temp_path = temp_file.name

    try:
        writer = csv.writer(temp_file)
        writer.writerow(target_header)

        with open(csv_file_path, 'r', encoding='utf-8-sig', newline='') as source_file:
            reader = csv.DictReader(source_file)
            if not reader.fieldnames:
                raise ValueError(f'CSV 文件为空，无法导入: {csv_file_path}')

            for row_dict in reader:
                if not row_dict or not any(str(cell).strip() for cell in row_dict.values()):
                    continue
                normalized_row = row_normalizer(row_dict, os.path.basename(csv_file_path))
                writer.writerow([normalized_row[column] for column in target_header])

        temp_file.close()
        return temp_path
    except Exception:
        temp_file.close()
        try:
            os.remove(temp_path)
        except OSError:
            pass
        raise


SW_INDEX_TARGET_HEADER = [
    'IndustryCode', 'date', 'lyrPe', 'lyrPeQuantile', 'ttmPe', 'ttmPeQuantile',
    'pb', 'pbQuantile', 'dvRatio', 'dvRatioQuantile', 'dvTtm', 'dvTtmQuantile',
    'addLyrPe', 'addLyrPeQuantile', 'addTtmPe', 'addTtmPeQuantile', 'addPb',
    'addPbQuantile', 'addDvRatio', 'addDvTtm', 'turnoverRate', 'turnoverRateF',
    'addTurnoverRate', 'addTurnoverRateF', 'turnoverRateFQuantile', 'totalMv',
    'close', 'addClose', 'middleLyrPe', 'middleLyrPeQuantile', 'middleTtmPe',
    'middleTtmPeQuantile', 'middlePb', 'middlePbQuantile', 'belowNetAssetPercent',
    'belowNetAssetCount', 'total', 'value5', 'value10', 'value20', 'value60',
    'indexClose', 'amount', 'amountCongestion', 'amountCongestionQuantile'
]


def _merge_stock_csvs_to_all(full_path, table_name='stock_daily', output_filename='all.csv'):
    """将目录下全部股票 CSV 合并成一个 all.csv。"""
    csv_files = []
    for filename in sorted(os.listdir(full_path)):
        if _should_include_restore_csv(filename, table_name):
            csv_files.append(filename)

    if not csv_files:
        raise ValueError(f'目录下未找到可用于 {table_name} 全量恢复的 CSV 文件')

    output_path = join_path(full_path, output_filename)
    total_rows = 0
    header = None
    normalized_file_count = 0

    if table_name in ('stock_daily', 'stock_daily_all'):
        target_header = ['code', 'date', 'open', 'close', 'high', 'low', 'volume', 'amount', 'turnover', 'outstanding_share']
    elif table_name == 'sw_index':
        target_header = SW_INDEX_TARGET_HEADER
    else:
        target_header = None

    with open(output_path, 'w', encoding='utf-8', newline='') as outfile:
        writer = csv.writer(outfile)
        for filename in csv_files:
            csv_path = join_path(full_path, filename)
            with open(csv_path, 'r', encoding='utf-8-sig', newline='') as infile:
                if target_header:
                    reader = csv.DictReader(infile)
                    file_header = reader.fieldnames
                else:
                    reader = csv.reader(infile)
                    file_header = next(reader, None)

                if not file_header:
                    logger.warning("跳过空CSV文件: %s", csv_path)
                    continue

                if table_name in ('stock_daily', 'stock_daily_all'):
                    if header is None:
                        header = target_header
                        writer.writerow(header)
                    if list(file_header) != target_header:
                        normalized_file_count += 1

                    for row_dict in reader:
                        if not row_dict or not any(str(cell).strip() for cell in row_dict.values()):
                            continue
                        normalized_row = _normalize_daily_row(row_dict, filename)
                        writer.writerow([normalized_row[column] for column in target_header])
                        total_rows += 1
                elif table_name == 'sw_index':
                    if header is None:
                        header = target_header
                        writer.writerow(header)
                    if list(file_header) != target_header:
                        normalized_file_count += 1

                    for row_dict in reader:
                        if not row_dict or not any(str(cell).strip() for cell in row_dict.values()):
                            continue
                        normalized_row = _normalize_sw_index_row(row_dict, filename)
                        writer.writerow([normalized_row[column] for column in target_header])
                        total_rows += 1
                else:
                    if header is None:
                        header = file_header
                        writer.writerow(header)
                    elif file_header != header:
                        raise ValueError(f'文件表头不一致，无法全量恢复: {filename}')

                    for row in reader:
                        if not row or not any(str(cell).strip() for cell in row):
                            continue
                        writer.writerow(row)
                        total_rows += 1

    if header is None:
        raise ValueError('目录中的CSV文件均为空，无法生成 all.csv')

    return output_path, len(csv_files), total_rows, normalized_file_count


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
            
            full_path = _resolve_restore_dir(stock_path)
            
            csv_file_path = join_path(full_path, f"{stock_code}.csv")
            
            if not os.path.exists(csv_file_path):
                return JsonResponse({
                    'success': False,
                    'message': f'文件不存在: {csv_file_path}'
                }, status=404)
            
            stock_schema, timestamp_col = _get_restore_schema(table_name)

            import_path = csv_file_path
            temp_import_path = None
            try:
                if table_name == 'sw_index':
                    temp_import_path = _rewrite_csv_with_header(
                        csv_file_path=csv_file_path,
                        target_header=SW_INDEX_TARGET_HEADER,
                        row_normalizer=_normalize_sw_index_row,
                        temp_prefix='sw_index_restore_'
                    )
                    import_path = temp_import_path

                success, message, data = import_csv_to_database(
                    csv_file_path=import_path,
                    table_name=table_name,
                    schema=stock_schema,
                    timestamp_col=timestamp_col,
                    partition_by="DAY",
                    delimiter=",",
                    force_header=True,
                    atomic=True
                )
            finally:
                if temp_import_path:
                    try:
                        os.remove(temp_import_path)
                    except OSError:
                        logger.warning("删除 sw_index 临时导入文件失败: %s", temp_import_path)
            
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


class RestoreStockAllData(APIView):
    """
    全量恢复股票数据：先合并目录下的股票CSV为 all.csv，再一次性导入数据库。
    URL: /api/restore/process_all/
    方法: POST
    请求体: {"path": "data/daily","table_name":"stock_daily"}
    """
    def post(self, request):
        try:
            data = json.loads(request.body)
            stock_path = data.get('path')
            table_name = data.get('table_name')

            if not table_name:
                return JsonResponse({
                    'success': False,
                    'message': '缺少表名参数'
                }, status=400)

            full_path = _resolve_restore_dir(stock_path)
            if not os.path.exists(full_path) or not os.path.isdir(full_path):
                return JsonResponse({
                    'success': False,
                    'message': f'提供的路径不是目录: {stock_path}'
                }, status=400)

            logger.info("开始全量恢复股票数据，路径: %s，表名: %s", stock_path, table_name)
            merged_csv_path, merged_file_count, merged_row_count, normalized_file_count = _merge_stock_csvs_to_all(full_path, table_name=table_name)
            stock_schema, timestamp_col = _get_restore_schema(table_name)

            if table_name == 'fq_factor':
                success, message, import_data = import_csv_to_database_partitioned(
                    csv_file_path=merged_csv_path,
                    table_name=table_name,
                    schema=stock_schema,
                    timestamp_col=timestamp_col,
                    partition_column='code',
                    batch_partition_limit=500
                )
            else:
                success, message, import_data = import_csv_to_database(
                    csv_file_path=merged_csv_path,
                    table_name=table_name,
                    schema=stock_schema,
                    timestamp_col=timestamp_col,
                    partition_by="DAY",
                    delimiter=",",
                    force_header=True,
                    atomic=True
                )

            if not success:
                logger.error("全量恢复导入失败: %s", message)
                return JsonResponse({
                    'success': False,
                    'message': message,
                    'merged_file': os.path.basename(merged_csv_path),
                    'merged_file_count': merged_file_count,
                    'merged_row_count': merged_row_count,
                    'normalized_file_count': normalized_file_count
                }, status=500)

            logger.info("全量恢复成功，合并文件: %s，文件数: %s，数据行数: %s", merged_csv_path, merged_file_count, merged_row_count)
            return JsonResponse({
                'success': True,
                'message': f'全量恢复成功，已合并 {merged_file_count} 个文件并导入 {table_name}',
                'merged_file': os.path.basename(merged_csv_path),
                'merged_file_count': merged_file_count,
                'merged_row_count': merged_row_count,
                'normalized_file_count': normalized_file_count,
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'import_result': import_data
            })

        except ValueError as e:
            logger.error("全量恢复参数或数据校验失败: %s", str(e))
            return JsonResponse({
                'success': False,
                'message': str(e)
            }, status=400)
        except json.JSONDecodeError:
            return JsonResponse({
                'success': False,
                'message': '无效的JSON请求体'
            }, status=400)
        except Exception as e:
            logger.exception("全量恢复股票数据时发生异常")
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
            
            logger.info(f"[路径验证] base_dir: {base_dir}, allowed_base: {allowed_base}")
            
            def normalize_and_validate_path(path):
                logger.info(f"[路径验证] 输入路径: {path}")
                normalized_input = normalize_path(path)
                logger.info(f"[路径验证] 规范化后路径: {normalized_input}")
                
                # 如果路径以 'data/' 开头，去掉这个前缀，避免重复
                if normalized_input.startswith('data/'):
                    normalized_input = normalized_input[len('data/'):]
                    logger.info(f"[路径验证] 去掉 data/ 前缀后: {normalized_input}")
                
                # 直接在 allowed_base 基础上连接路径
                # 这样无论输入是 data/daily 还是 daily 都可以正确处理
                full_path = safe_join(allowed_base, normalized_input)
                logger.info(f"[路径验证] 安全验证后路径: {full_path}")
                
                if not os.path.exists(full_path) or not os.path.isdir(full_path):
                    logger.error(f"[路径验证] 路径不存在或不是目录: {full_path}")
                    raise ValueError(f'无效的路径: {path} (完整路径: {full_path})')
                
                logger.info(f"[路径验证] 路径验证成功: {full_path}")
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
            
            logger.info(f"[路径验证] base_dir: {base_dir}, allowed_base: {allowed_base}")
            
            def normalize_and_validate_path(path):
                logger.info(f"[路径验证] 输入路径: {path}")
                normalized_input = normalize_path(path)
                logger.info(f"[路径验证] 规范化后路径: {normalized_input}")
                
                # 如果路径以 'data/' 开头，去掉这个前缀，避免重复
                if normalized_input.startswith('data/'):
                    normalized_input = normalized_input[len('data/'):]
                    logger.info(f"[路径验证] 去掉 data/ 前缀后: {normalized_input}")
                
                # 直接在 allowed_base 基础上连接路径
                # 这样无论输入是 data/daily 还是 daily 都可以正确处理
                full_path = safe_join(allowed_base, normalized_input)
                logger.info(f"[路径验证] 安全验证后路径: {full_path}")
                
                if not os.path.exists(full_path) or not os.path.isdir(full_path):
                    logger.error(f"[路径验证] 路径不存在或不是目录: {full_path}")
                    raise ValueError(f'无效的路径: {path} (完整路径: {full_path})')
                
                logger.info(f"[路径验证] 路径验证成功: {full_path}")
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


def _resolve_merge_directory(path):
    """Resolve a restore directory while keeping it below BASE_DIR/data."""
    if not path or path.startswith('/') or path.startswith('\\'):
        raise ValueError(f'不允许使用绝对路径: {path}')

    allowed_base = str(settings.BASE_DIR)
    normalized_path = normalize_path(path)
    full_path = join_path(allowed_base, normalized_path)
    full_path = safe_join(allowed_base, full_path.replace(allowed_base, '').lstrip('/'))
    if not os.path.exists(full_path) or not os.path.isdir(full_path):
        raise ValueError(f'无效的路径: {path}')
    return full_path


def _get_merge_stock_codes(append_path):
    append_full_path = _resolve_merge_directory(append_path)
    stock_codes = []
    for filename in os.listdir(append_full_path):
        code = os.path.splitext(filename)[0]
        if code.isdigit() and len(code) == 6:
            stock_codes.append(code)
    return sorted(stock_codes)


def _run_merge_job(job_id, main_path, append_path, stock_codes):
    """Run the existing single-stock merge endpoint for each stock in a worker thread."""
    global _active_merge_job_id
    with _merge_jobs_lock:
        job = _merge_jobs.get(job_id)
        if not job:
            return
        job['status'] = 'running'
        job['started_at'] = datetime.now().isoformat(timespec='seconds')

    for stock_code in stock_codes:
        with _merge_jobs_lock:
            job = _merge_jobs.get(job_id)
            if not job:
                return
            job['current_code'] = stock_code

        try:
            request = type('MergeRequest', (), {
                'body': json.dumps({
                    'main_path': main_path,
                    'append_path': append_path,
                    'stock_code': stock_code
                }).encode('utf-8')
            })()
            response = MergeStockItem().post(request)
            result = json.loads(response.content.decode('utf-8'))
            succeeded = response.status_code < 400 and result.get('success') is True
        except Exception as exc:
            logger.exception("批量合并股票%s时发生异常", stock_code)
            result = {'success': False, 'message': str(exc)}
            succeeded = False

        with _merge_jobs_lock:
            job = _merge_jobs.get(job_id)
            if not job:
                return
            job['completed'] += 1
            if succeeded:
                job['success_count'] += 1
                job['total_new_lines'] += int(result.get('new_lines_added') or 0)
                job['total_duplicate_lines'] += int(result.get('duplicate_lines_filtered') or 0)
            else:
                job['failed_count'] += 1
                job['failed_codes'].append({
                    'stock_code': stock_code,
                    'message': result.get('message', '未知错误')
                })
            job['percent'] = round(job['completed'] * 100 / job['total'], 2) if job['total'] else 100

    with _merge_jobs_lock:
        job = _merge_jobs.get(job_id)
        if job:
            job['status'] = 'completed'
            job['current_code'] = None
            job['percent'] = 100
            job['ended_at'] = datetime.now().isoformat(timespec='seconds')
        if _active_merge_job_id == job_id:
            _active_merge_job_id = None


class MergeStockMergeStart(APIView):
    """Start one background job that merges every stock in the append directory."""
    def post(self, request):
        global _active_merge_job_id
        try:
            data = json.loads(request.body)
            main_path = data.get('main_path')
            append_path = data.get('append_path')
            if not main_path or not append_path:
                return JsonResponse({'success': False, 'message': '缺少必要的路径参数'}, status=400)

            _resolve_merge_directory(main_path)
            stock_codes = _get_merge_stock_codes(append_path)

            with _merge_jobs_lock:
                if _active_merge_job_id:
                    active_job = _merge_jobs.get(_active_merge_job_id)
                    if active_job and active_job['status'] in ('queued', 'running'):
                        return JsonResponse({'success': True, 'message': '已有批量合并任务正在执行', 'job': active_job})
                    _active_merge_job_id = None

                job_id = uuid.uuid4().hex
                job = {
                    'job_id': job_id,
                    'status': 'queued',
                    'total': len(stock_codes),
                    'completed': 0,
                    'success_count': 0,
                    'failed_count': 0,
                    'percent': 100 if not stock_codes else 0,
                    'current_code': None,
                    'total_new_lines': 0,
                    'total_duplicate_lines': 0,
                    'failed_codes': [],
                    'started_at': None,
                    'ended_at': None,
                    'main_path': main_path,
                    'append_path': append_path
                }
                _merge_jobs[job_id] = job
                _active_merge_job_id = job_id

            if not stock_codes:
                with _merge_jobs_lock:
                    now = datetime.now().isoformat(timespec='seconds')
                    job['status'] = 'completed'
                    job['started_at'] = now
                    job['ended_at'] = now
                    _active_merge_job_id = None
            else:
                threading.Thread(
                    target=_run_merge_job,
                    args=(job_id, main_path, append_path, stock_codes),
                    daemon=True,
                    name=f'merge-stock-{job_id[:8]}'
                ).start()

            return JsonResponse({'success': True, 'message': f'已启动批量合并，共{len(stock_codes)}个股票', 'job': job}, status=202)
        except json.JSONDecodeError:
            return JsonResponse({'success': False, 'message': '无效的JSON请求体'}, status=400)
        except ValueError as exc:
            return JsonResponse({'success': False, 'message': str(exc)}, status=400)
        except Exception as exc:
            logger.exception("启动批量合并任务时发生异常")
            return JsonResponse({'success': False, 'message': f'处理失败: {str(exc)}'}, status=500)


class MergeStockMergeStatus(APIView):
    """Return progress for a background stock merge job."""
    def get(self, request):
        job_id = request.GET.get('job_id')
        if not job_id:
            return JsonResponse({'success': False, 'message': '缺少job_id参数'}, status=400)
        with _merge_jobs_lock:
            job = _merge_jobs.get(job_id)
            if not job:
                return JsonResponse({'success': False, 'message': '任务不存在或服务已重启'}, status=404)
            return JsonResponse({'success': True, 'job': dict(job)})


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
            
            try:
                backup_path = join_path(main_full_path, f"{code}_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
                with open(backup_path, 'w', encoding='utf-8') as f:
                    f.writelines(main_data)
                
                with open(main_csv_path, 'w', encoding='utf-8') as f:
                    f.writelines(merged_data)
                
                try:
                    if os.path.exists(backup_path):
                        os.remove(backup_path)
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
