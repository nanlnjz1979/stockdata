import csv
import json
import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import akshare as ak
from django.conf import settings
from django.core.management.base import BaseCommand


DEFAULT_CODES = [
    "600000", "600519", "600036", "600276", "601318",
    "000001", "000333", "000651", "002415", "300750",
    "300124", "002594", "601398", "600030", "600887",
]

DEFAULT_ROUNDS = [
    {"name": "safe-1", "workers": 1, "delay_min": 4.0, "delay_max": 8.0},
    {"name": "safe-2", "workers": 2, "delay_min": 3.0, "delay_max": 6.0},
    {"name": "current", "workers": 2, "delay_min": 1.5, "delay_max": 4.0},
]


def classify_error(exc):
    text = str(exc or "")
    if "RemoteDisconnected" in text:
        return "remote_disconnected"
    if "ProxyError" in text:
        return "proxy_error"
    if "No value to decode" in text:
        return "empty_or_non_json"
    if "Forbidden" in text or "403" in text:
        return "forbidden"
    if "Too Many Requests" in text or "429" in text:
        return "rate_limited"
    if "timeout" in text.lower():
        return "timeout"
    return exc.__class__.__name__


class Command(BaseCommand):
    help = "保守测试 akshare 日K接口稳定性，记录失败率；不会做压力探测。"

    def add_arguments(self, parser):
        parser.add_argument("--sample-size", type=int, default=10, help="每轮测试股票数量，默认 10")
        parser.add_argument("--timeout", type=float, default=12.0, help="单次请求超时秒数，默认 12")
        parser.add_argument("--abort-failure-rate", type=float, default=0.3, help="单轮失败率达到该值后停止后续轮次，默认 0.3")
        parser.add_argument("--start-date", default="20260701", help="测试开始日期，默认 20260701")
        parser.add_argument("--end-date", default="20260729", help="测试结束日期，默认 20260729")
        parser.add_argument("--codes", default="", help="逗号分隔股票代码；不传则使用内置样本")
        parser.add_argument("--output-dir", default="", help="结果输出目录，默认 backend/data/akshare_probe")

    def handle(self, *args, **options):
        sample_size = max(1, int(options["sample_size"]))
        timeout = float(options["timeout"])
        abort_failure_rate = max(0.0, min(1.0, float(options["abort_failure_rate"])))
        start_date = options["start_date"]
        end_date = options["end_date"]
        codes = [code.strip() for code in options["codes"].split(",") if code.strip()] or DEFAULT_CODES
        output_dir = Path(options["output_dir"] or Path(settings.BASE_DIR) / "data" / "akshare_probe")
        output_dir.mkdir(parents=True, exist_ok=True)

        run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_path = output_dir / f"akshare_probe_{run_id}.csv"
        summary_path = output_dir / f"akshare_probe_{run_id}.json"

        self.stdout.write(self.style.WARNING(
            "开始保守稳定性测试：默认目标是寻找安全阈值，不做封禁边界压力探测。"
        ))
        self.stdout.write(f"输出文件: {csv_path}")

        rows = []
        summaries = []

        with csv_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=[
                    "run_id", "round", "workers", "delay_min", "delay_max",
                    "code", "ok", "rows", "elapsed", "error_type", "error",
                ],
            )
            writer.writeheader()

            for round_cfg in DEFAULT_ROUNDS:
                round_codes = codes[:]
                random.shuffle(round_codes)
                round_codes = round_codes[:sample_size]
                round_rows = self._run_round(
                    run_id=run_id,
                    round_cfg=round_cfg,
                    codes=round_codes,
                    timeout=timeout,
                    start_date=start_date,
                    end_date=end_date,
                )

                for row in round_rows:
                    writer.writerow(row)
                    rows.append(row)

                total = len(round_rows)
                failed = sum(1 for row in round_rows if not row["ok"])
                succeeded = total - failed
                avg_elapsed = round(sum(float(row["elapsed"]) for row in round_rows) / total, 3) if total else 0
                failure_rate = round(failed / total, 4) if total else 0
                summary = {
                    **round_cfg,
                    "total": total,
                    "success": succeeded,
                    "failed": failed,
                    "failure_rate": failure_rate,
                    "avg_elapsed": avg_elapsed,
                }
                summaries.append(summary)

                self.stdout.write(
                    f"[{round_cfg['name']}] total={total} success={succeeded} failed={failed} "
                    f"failure_rate={failure_rate:.2%} avg_elapsed={avg_elapsed}s"
                )

                if failure_rate >= abort_failure_rate:
                    self.stdout.write(self.style.WARNING(
                        f"失败率 {failure_rate:.2%} 已达到熔断阈值 {abort_failure_rate:.2%}，停止后续轮次。"
                    ))
                    break

        result = {
            "run_id": run_id,
            "start_date": start_date,
            "end_date": end_date,
            "sample_size": sample_size,
            "timeout": timeout,
            "abort_failure_rate": abort_failure_rate,
            "rounds": summaries,
            "csv_path": str(csv_path),
        }
        summary_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
        self.stdout.write(self.style.SUCCESS(f"测试完成，摘要: {summary_path}"))

    def _run_round(self, run_id, round_cfg, codes, timeout, start_date, end_date):
        workers = max(1, int(round_cfg["workers"]))
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = [
                executor.submit(self._fetch_one, run_id, round_cfg, code, timeout, start_date, end_date)
                for code in codes
            ]
            return [future.result() for future in as_completed(futures)]

    def _fetch_one(self, run_id, round_cfg, code, timeout, start_date, end_date):
        delay = random.uniform(round_cfg["delay_min"], round_cfg["delay_max"])
        time.sleep(delay)
        started = time.perf_counter()
        try:
            df = ak.stock_zh_a_hist(
                symbol=code,
                period="daily",
                start_date=start_date,
                end_date=end_date,
                adjust="",
                timeout=timeout,
            )
            elapsed = round(time.perf_counter() - started, 3)
            return {
                "run_id": run_id,
                "round": round_cfg["name"],
                "workers": round_cfg["workers"],
                "delay_min": round_cfg["delay_min"],
                "delay_max": round_cfg["delay_max"],
                "code": code,
                "ok": True,
                "rows": len(df) if df is not None else 0,
                "elapsed": elapsed,
                "error_type": "",
                "error": "",
            }
        except Exception as exc:
            elapsed = round(time.perf_counter() - started, 3)
            return {
                "run_id": run_id,
                "round": round_cfg["name"],
                "workers": round_cfg["workers"],
                "delay_min": round_cfg["delay_min"],
                "delay_max": round_cfg["delay_max"],
                "code": code,
                "ok": False,
                "rows": 0,
                "elapsed": elapsed,
                "error_type": classify_error(exc),
                "error": str(exc)[:500],
            }
