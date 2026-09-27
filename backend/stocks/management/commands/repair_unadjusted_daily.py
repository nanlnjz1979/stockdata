"""Re-download unadjusted bars for dates stored on the wrong price scale."""

import time
from datetime import date, datetime

from django.core.management.base import BaseCommand

from backend.global_config.data_fetch import AkshareFetcher, format_fetch_error
from db.db_pool import get_conn, put_conn
from db.mongodb_pool import get_mongo_conn, put_mongo_conn
from stocks.price_scale import repair_ranges_from_breaks
from stocks.tasks.download_daily import _insert_daily


BREAK_SQL = """
WITH breaks AS (
    SELECT code,
           toDate(date) AS day,
           toDate(prev_date) AS prev_day,
           close,
           prev_close,
           close / prev_close AS ratio
    FROM (
        SELECT code,
               date,
               close,
               lagInFrame(date, 1) OVER (PARTITION BY code ORDER BY date) AS prev_date,
               lagInFrame(close, 1) OVER (PARTITION BY code ORDER BY date) AS prev_close
        FROM stock_daily FINAL
    )
    WHERE prev_close > 0
      AND (close / prev_close > 3 OR prev_close / close > 3)
)
SELECT code, day, prev_day, close, prev_close
FROM breaks
WHERE ratio < 1.0 / 3
   OR (
        ratio > 3
        AND abs(ratio / nullIf((
            SELECT argMax(hfq, date)
            FROM fq_factor
            WHERE fq_factor.code = breaks.code
              AND toDate(fq_factor.date) <= breaks.day
        ), 0) - 1) < if(ratio > 8, 0.15, 0.05)
   )
ORDER BY code, day
"""


def _as_date(value):
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return datetime.strptime(str(value)[:10], "%Y-%m-%d").date()


class Command(BaseCommand):
    help = "用不再复权的行情覆盖 stock_daily 里被后复权价盖住的区间，并合并受影响分区。"

    def add_arguments(self, parser):
        parser.add_argument("--codes", default="", help="只修复这些代码，逗号分隔")
        parser.add_argument("--sleep", type=float, default=0.8)
        parser.add_argument("--dry-run", action="store_true")

    def handle(self, *args, **options):
        self._store_unadjusted_schedule()
        conn = get_conn()
        try:
            rows = conn.execute(BREAK_SQL)
            wanted = {item.strip() for item in options["codes"].split(",") if item.strip()}
            breaks = []
            for code, day, prev_day, close, prev_close in rows:
                if wanted and code not in wanted:
                    continue
                breaks.append((code, _as_date(day), _as_date(prev_day), close, prev_close))
            ranges = repair_ranges_from_breaks(breaks)
            open_codes = [code for code, _, end in ranges if end is None]
            latest = self._latest_dates(conn, open_codes)
            ranges = [
                (code, start, end if end is not None else latest.get(code, start))
                for code, start, end in ranges
            ]
            self.stdout.write(f"待修复区间 {len(ranges)} 个")
            if options["dry_run"]:
                longest = sorted(ranges, key=lambda item: (item[2] - item[1]).days, reverse=True)[:10]
                for code, start, end in longest:
                    self.stdout.write(f"{code} {start} {end} {(end - start).days + 1}天")
                return
            partitions = set()
            failed = []
            fetcher = AkshareFetcher()
            for index, (code, start, end) in enumerate(ranges, start=1):
                partitions.update(self._months(start, end))
                saved = self._repair_one(fetcher, conn, code, start, end)
                if saved <= 0:
                    failed.append(code)
                if index % 50 == 0 or index == len(ranges):
                    self.stdout.write(f"已处理 {index}/{len(ranges)}，失败 {len(failed)}")
                time.sleep(options["sleep"])
            for partition in sorted(partitions):
                conn.execute(f"OPTIMIZE TABLE stock_daily PARTITION '{partition}' FINAL")
                self.stdout.write(f"已合并分区 {partition}")
            if failed:
                self.stdout.write(f"未写入: {','.join(failed[:30])}")
        finally:
            put_conn(conn)

    def _store_unadjusted_schedule(self):
        conn = get_mongo_conn()
        try:
            result = conn["stockdata"]["schedule_configs"].update_one(
                {"_id": "STOCK_Update"},
                {"$set": {"params": '{"market":"CN","adjust":""}'}},
            )
            self.stdout.write(f"STOCK_Update 已改为不复权 matched={result.matched_count}")
        finally:
            put_mongo_conn(conn)

    def _latest_dates(self, conn, codes):
        if not codes:
            return {}
        listed = ",".join(f"'{code}'" for code in codes if str(code).isalnum())
        rows = conn.execute(
            f"SELECT code, max(toDate(date)) FROM stock_daily WHERE code IN ({listed}) GROUP BY code"
        )
        return {code: _as_date(day) for code, day in rows}

    def _months(self, start, end):
        months = set()
        year, month = start.year, start.month
        while (year, month) <= (end.year, end.month):
            months.add(f"{year}{month:02d}")
            month += 1
            if month == 13:
                year += 1
                month = 1
        return months

    def _repair_one(self, fetcher, conn, code, start, end):
        start_text = start.strftime("%Y%m%d")
        end_text = end.strftime("%Y%m%d")
        last_error = None
        for _ in range(2):
            try:
                frame = fetcher.fetch_stock_daily(code, start_text, end_text, adjust="")
                return _insert_daily(code, frame, conn)
            except Exception as exc:
                last_error = format_fetch_error(exc)
                time.sleep(2)
        self.stderr.write(f"{code} {start} {end} 下载失败: {last_error}")
        return 0
