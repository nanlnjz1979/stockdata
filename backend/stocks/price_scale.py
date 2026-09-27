"""Decide whether a downloaded daily bar is still on the unadjusted price scale."""

from datetime import date, datetime


AMPLIFY_LIMIT = 3.0


def _as_date(value):
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value)[:10]
    return datetime.strptime(text, "%Y-%m-%d").date()


def frame_first_close(frame):
    """Return the earliest close, accepting either AkShare column layout."""
    if frame is None or getattr(frame, "empty", True):
        return None
    date_column = "date" if "date" in frame.columns else "日期" if "日期" in frame.columns else None
    close_column = "close" if "close" in frame.columns else "收盘" if "收盘" in frame.columns else None
    if date_column is None or close_column is None:
        return None
    ordered = frame.sort_values(date_column)
    close = ordered.iloc[0][close_column]
    try:
        close = float(close)
    except (TypeError, ValueError):
        return None
    if close != close or close <= 0:  # NaN or non-positive
        return None
    return close


def is_amplified(close, previous_close, limit=AMPLIFY_LIMIT):
    """A 3x jump or drop means this bar is not on the same scale as the stored history."""
    if previous_close is None or close is None:
        return False
    try:
        previous = float(previous_close)
        current = float(close)
    except (TypeError, ValueError):
        return False
    if previous <= 0 or current <= 0:
        return False
    ratio = current / previous
    return ratio > limit or ratio < (1.0 / limit)


def select_unadjusted_frame(primary, alternate, previous_close, limit=AMPLIFY_LIMIT):
    """Keep a download only when its first close still matches the stored scale.

    When the first source is amplified, the other source is used if it matches.
    Both amplified, or no usable alternate, means the batch must not be stored.
    """
    if primary is None or getattr(primary, "empty", True):
        return None
    if not is_amplified(frame_first_close(primary), previous_close, limit):
        return primary
    if not is_amplified(frame_first_close(alternate), previous_close, limit):
        return alternate
    return None


def repair_ranges(points, limit=AMPLIFY_LIMIT):
    """Return inclusive ranges whose prices jumped onto another scale and later came back.

    ``points`` is ``(code, date, close)`` sorted by code and date. The repaired
    span starts on the jump and ends on the last amplified day, before the drop.
    """
    ranges = []
    current_code = None
    previous_close = None
    previous_day = None
    span_start = None
    for code, day, close in points:
        day = _as_date(day)
        close = float(close)
        if code != current_code:
            if span_start is not None and previous_day is not None:
                ranges.append((current_code, span_start, previous_day))
            current_code = code
            previous_close = None
            previous_day = None
            span_start = None
        if previous_close is not None and previous_close > 0 and close > 0:
            ratio = close / previous_close
            if span_start is None and ratio > limit:
                span_start = day
            elif span_start is not None and ratio < (1.0 / limit):
                ranges.append((code, span_start, previous_day))
                span_start = None
        previous_close = close
        previous_day = day
    if span_start is not None and previous_day is not None:
        ranges.append((current_code, span_start, previous_day))
    return ranges


def repair_ranges_from_breaks(breaks, limit=AMPLIFY_LIMIT):
    """Pair jump-up and jump-down boundaries into ranges.

    Each break is ``(code, day, previous_day, close, previous_close)``.
    A jump down ends the range on ``previous_day``. A jump that never comes
    back is returned with ``end=None`` so the caller can extend it to the
    stock's latest stored date.
    """
    ranges = []
    current_code = None
    span_start = None
    for code, day, previous_day, close, previous_close in breaks:
        day = _as_date(day)
        previous_day = _as_date(previous_day)
        if code != current_code:
            if span_start is not None:
                ranges.append((current_code, span_start, None))
            current_code = code
            span_start = None
        if previous_close is None or float(previous_close) <= 0 or float(close) <= 0:
            continue
        ratio = float(close) / float(previous_close)
        if span_start is None and ratio > limit:
            span_start = day
        elif span_start is not None and ratio < (1.0 / limit):
            ranges.append((code, span_start, previous_day))
            span_start = None
    if span_start is not None:
        ranges.append((current_code, span_start, None))
    return ranges
