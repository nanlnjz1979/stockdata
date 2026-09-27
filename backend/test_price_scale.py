#!/usr/bin/env python3
import sys
import unittest
from datetime import date
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent))

from stocks.price_scale import repair_ranges, repair_ranges_from_breaks, select_unadjusted_frame


def frame(close, day="2026-07-02"):
    return pd.DataFrame([{"date": day, "close": close}])


class SelectUnadjustedFrameTests(unittest.TestCase):
    def test_keeps_primary_when_it_matches_the_previous_close(self):
        primary = frame(10.28)
        chosen = select_unadjusted_frame(primary, frame(1549), 10.16)
        self.assertEqual(float(chosen.iloc[0]["close"]), 10.28)

    def test_uses_the_other_source_when_the_first_is_scaled_like_hfq(self):
        chosen = select_unadjusted_frame(frame(1549.46), frame(10.28), 10.16)
        self.assertEqual(float(chosen.iloc[0]["close"]), 10.28)

    def test_rejects_when_both_sources_are_amplified(self):
        self.assertIsNone(select_unadjusted_frame(frame(1549), frame(1600), 10.16))

    def test_rejects_when_the_other_source_is_missing(self):
        self.assertIsNone(select_unadjusted_frame(frame(1549), None, 10.16))

    def test_keeps_primary_for_a_new_stock_with_no_previous_close(self):
        primary = frame(10.28)
        chosen = select_unadjusted_frame(primary, None, None)
        self.assertIs(chosen, primary)


class RepairRangeTests(unittest.TestCase):
    def test_repairs_the_span_between_the_jump_up_and_the_jump_down(self):
        points = [
            ("000001", date(2026, 7, 1), 10.16),
            ("000001", date(2026, 7, 2), 1549.46),
            ("000001", date(2026, 7, 3), 1550.97),
            ("000001", date(2026, 7, 30), 1749.93),
            ("000001", date(2026, 7, 31), 11.63),
            ("000006", date(2026, 7, 1), 7.53),
            ("000006", date(2026, 7, 2), 380.55),
            ("000006", date(2026, 7, 31), 337.57),
            ("000006", date(2026, 8, 3), 6.62),
        ]
        self.assertEqual(
            repair_ranges(points),
            [
                ("000001", date(2026, 7, 2), date(2026, 7, 30)),
                ("000006", date(2026, 7, 2), date(2026, 7, 31)),
            ],
        )

    def test_open_span_ends_unknown_when_only_the_jump_boundaries_are_known(self):
        breaks = [
            ("000001", date(2026, 7, 2), date(2026, 7, 1), 1549.46, 10.16),
            ("000001", date(2026, 7, 31), date(2026, 7, 30), 11.63, 1749.93),
            ("000002", date(2026, 7, 2), date(2026, 7, 1), 517.58, 3.05),
        ]
        self.assertEqual(
            repair_ranges_from_breaks(breaks),
            [
                ("000001", date(2026, 7, 2), date(2026, 7, 30)),
                ("000002", date(2026, 7, 2), None),
            ],
        )


if __name__ == "__main__":
    unittest.main()
