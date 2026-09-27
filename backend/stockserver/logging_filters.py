import logging
import re


class SuppressPollingAccessLog(logging.Filter):
    """Hide noisy frontend polling endpoints from Django's access log."""

    SUPPRESSED_PATHS = (
        "/api/tasks/schedules",
        "/api/tasks/monitor",
        "/api/tasks/recent",
    )
    SUPPRESS_SUCCESS_PATHS = (
        "/api/restore/mergeItem/",
        "/api/restore/sw_mergeItem/",
    )
    SUPPRESSED_PATTERNS = (
        re.compile(r'"GET /api/stocks/update(?:/[^ ?"]+)?/status(?:[ ?"][^"]*)? HTTP/'),
    )

    def filter(self, record):
        message = record.getMessage()
        if any(path in message for path in self.SUPPRESSED_PATHS):
            return False
        if any(path in message for path in self.SUPPRESS_SUCCESS_PATHS):
            status_code = getattr(record, 'status_code', None)
            if status_code is None:
                match = re.search(r'HTTP/\d(?:\.\d)?"\s+(\d{3})', message)
                status_code = int(match.group(1)) if match else None
            if status_code is not None and status_code < 400:
                return False
        return not any(pattern.search(message) for pattern in self.SUPPRESSED_PATTERNS)
