import unittest
from dataclasses import dataclass, field

from valentine.algorithms.match import ColumnPair
from valentine.algorithms.matcher_results import MatcherResults
from valentine.metrics import (
    METRICS_ALL,
    F1Score,
    Metric,
    Precision,
    PrecisionTopNPercent,
    Recall,
    RecallAtSizeofGroundTruth,
)
from valentine.metrics.metric_helpers import get_fp, get_tp_fn


@dataclass(eq=True, frozen=True)
class _BogusMetric(Metric):
    def apply(self, matches, ground_truth):
        return self.return_format(0.0)


class TestMetrics(unittest.TestCase):
    def setUp(self) -> None:
        # Scores chosen so that the highest-confidence pairs are the true matches,
        # and "Title" has two competing candidates (DUMMY1, DUMMY2) to exercise 1-1 logic.
        self.matches = MatcherResults(
            {
                ColumnPair("table_1", "Cited by", "table_2", "Cited by"): 0.8374313,
                ColumnPair("table_1", "Authors", "table_2", "Authors"): 0.83498037,
                ColumnPair("table_1", "EID", "table_2", "EID"): 0.8214057,
                ColumnPair("table_1", "Title", "table_2", "DUMMY1"): 0.8214057,
                ColumnPair("table_1", "Title", "table_2", "DUMMY2"): 0.8114057,
            }
        )

        # Five GT pairs: 4 correct, 1 extra (DUMMY3) to influence recall.
        self.ground_truth = [
            ("Cited by", "Cited by"),
            ("Authors", "Authors"),
            ("EID", "EID"),
            ("Title", "Title"),
            ("DUMMY3", "DUMMY3"),
        ]

        # Handy expected values
        self.expected_precision_1to1 = 0.75  # 3/4 due to Title conflict
        self.expected_recall_1to1 = 0.6  # 3/5
        # Harmonic mean of P=0.75 and R=0.6  ->  0.666666...
        self.expected_f1_1to1 = (2 * self.expected_precision_1to1 * self.expected_recall_1to1) / (
            self.expected_precision_1to1 + self.expected_recall_1to1
        )

    def test_precision(self) -> None:
        with self.subTest(one_to_one=True):
            precision = self.matches.get_metrics(self.ground_truth, metrics={Precision()})
            self.assertIn("Precision", precision)
            self.assertAlmostEqual(precision["Precision"], self.expected_precision_1to1, places=6)

        with self.subTest(one_to_one=False):
            precision_n11 = self.matches.get_metrics(
                self.ground_truth, metrics={Precision(one_to_one=False)}
            )
            self.assertIn("Precision(one_to_one=False)", precision_n11)
            self.assertAlmostEqual(precision_n11["Precision(one_to_one=False)"], 0.6, places=6)

    def test_recall(self) -> None:
        with self.subTest(one_to_one=True):
            recall = self.matches.get_metrics(self.ground_truth, metrics={Recall()})
            self.assertIn("Recall", recall)
            self.assertAlmostEqual(recall["Recall"], self.expected_recall_1to1, places=6)

        with self.subTest(one_to_one=False):
            recall_n11 = self.matches.get_metrics(
                self.ground_truth, metrics={Recall(one_to_one=False)}
            )
            self.assertIn("Recall(one_to_one=False)", recall_n11)
            self.assertAlmostEqual(recall_n11["Recall(one_to_one=False)"], 0.6, places=6)

    def test_f1(self) -> None:
        with self.subTest(one_to_one=True):
            f1 = self.matches.get_metrics(self.ground_truth, metrics={F1Score()})
            self.assertIn("F1Score", f1)
            self.assertAlmostEqual(f1["F1Score"], self.expected_f1_1to1, places=6)

        with self.subTest(one_to_one=False):
            f1_n11 = self.matches.get_metrics(
                self.ground_truth, metrics={F1Score(one_to_one=False)}
            )
            self.assertIn("F1Score(one_to_one=False)", f1_n11)
            self.assertAlmostEqual(f1_n11["F1Score(one_to_one=False)"], 0.6, places=6)

    def test_precision_top_n_percent(self) -> None:
        # n=0 -> empty selection
        p0 = self.matches.get_metrics(self.ground_truth, metrics={PrecisionTopNPercent(n=0)})
        self.assertIn("PrecisionTop0Percent", p0)
        self.assertEqual(p0["PrecisionTop0Percent"], 0)

        # n=50 -> top half of candidates are all correct here
        p50 = self.matches.get_metrics(self.ground_truth, metrics={PrecisionTopNPercent(n=50)})
        self.assertIn("PrecisionTop50Percent", p50)
        self.assertEqual(p50["PrecisionTop50Percent"], 1.0)

        # n=100 -> equals overall precision
        overall_p = self.matches.get_metrics(self.ground_truth, metrics={Precision()})
        p100 = self.matches.get_metrics(self.ground_truth, metrics={PrecisionTopNPercent(n=100)})
        self.assertIn("PrecisionTop100Percent", p100)
        self.assertAlmostEqual(p100["PrecisionTop100Percent"], overall_p["Precision"], places=6)

        # n=70 and not one-to-one (allows multiple matches per column)
        p70_n11 = self.matches.get_metrics(
            self.ground_truth, metrics={PrecisionTopNPercent(n=70, one_to_one=False)}
        )
        self.assertIn("PrecisionTop70Percent(one_to_one=False)", p70_n11)
        self.assertAlmostEqual(p70_n11["PrecisionTop70Percent(one_to_one=False)"], 0.75, places=6)

    def test_recall_at_size_of_ground_truth(self) -> None:
        r = self.matches.get_metrics(self.ground_truth, metrics={RecallAtSizeofGroundTruth()})
        self.assertIn("RecallAtSizeofGroundTruth", r)
        self.assertAlmostEqual(r["RecallAtSizeofGroundTruth"], 0.6, places=6)

    def test_metric_helpers(self) -> None:
        limit = 2
        tp, fn = get_tp_fn(self.matches, self.ground_truth, n=limit)
        self.assertLessEqual(tp, len(self.ground_truth))
        self.assertLessEqual(fn, len(self.ground_truth))

        fp = get_fp(self.matches, self.ground_truth, n=limit)
        self.assertLessEqual(fp, limit)

        # With n=2, top-2 predictions should be true matches; GT size is 5
        self.assertEqual(tp, 2)
        self.assertEqual(fn, 3)
        self.assertEqual(fp, 0)

    def test_ground_truth_column_pair_table_aware(self) -> None:
        """ColumnPair ground truth distinguishes identically-named columns
        across different tables."""
        matches = MatcherResults(
            {
                ColumnPair("t1", "id", "t2", "id"): 0.9,
                ColumnPair("t1", "id", "t3", "id"): 0.8,
            }
        )
        # With ColumnPair GT, only (t1, t2) pair should count as TP
        gt = [ColumnPair("t1", "id", "t2", "id")]
        tp, fn = get_tp_fn(matches, gt)
        fp = get_fp(matches, gt)
        self.assertEqual(tp, 1)
        self.assertEqual(fn, 0)
        self.assertEqual(fp, 1)  # (t1, t3) pair is FP

        # With column-name GT, both matches count as TP (table names ignored)
        gt_names = [("id", "id")]
        tp2, _ = get_tp_fn(matches, gt_names)
        self.assertEqual(tp2, 1)  # still one GT entry, satisfied by first match

    def test_metrics_all(self) -> None:
        """METRICS_ALL is a stable, explicit set (no __subclasses__ magic)."""
        names = {m.name() for m in METRICS_ALL}
        self.assertIn("Precision", names)
        self.assertIn("Recall", names)
        self.assertIn("F1Score", names)
        self.assertIn("RecallAtSizeofGroundTruth", names)
        # User-defined Metric subclasses must not bleed into METRICS_ALL
        self.assertNotIn("_BogusMetric", names)

    def test_metric_equals(self) -> None:
        a = PrecisionTopNPercent(n=10, one_to_one=False)
        b = PrecisionTopNPercent(n=10, one_to_one=False)
        c = PrecisionTopNPercent(n=10, one_to_one=True)

        self.assertEqual(a, b)
        self.assertNotEqual(a, c)
        self.assertNotEqual(a, Precision())

    def test_metric_names(self) -> None:
        # Default instances keep the plain class name.
        self.assertEqual(F1Score().name(), "F1Score")
        self.assertEqual(F1Score(one_to_one=True).name(), "F1Score")
        self.assertEqual(RecallAtSizeofGroundTruth().name(), "RecallAtSizeofGroundTruth")
        self.assertEqual(PrecisionTopNPercent().name(), "PrecisionTop10Percent")
        self.assertEqual(PrecisionTopNPercent(n=70).name(), "PrecisionTop70Percent")
        # Non-default fields are appended; ``n`` is already part of the name.
        self.assertEqual(F1Score(one_to_one=False).name(), "F1Score(one_to_one=False)")
        self.assertEqual(
            RecallAtSizeofGroundTruth(one_to_one=True).name(),
            "RecallAtSizeofGroundTruth(one_to_one=True)",
        )
        self.assertEqual(
            PrecisionTopNPercent(n=70, one_to_one=False).name(),
            "PrecisionTop70Percent(one_to_one=False)",
        )

    def test_custom_metric_configurations_kept_apart(self) -> None:
        # A user-defined metric in the style of docs/metrics.md, with a required
        # field and a field whose default comes from a factory.
        @dataclass(eq=True, frozen=True)
        class SupportAtK(Metric):
            k: int
            sources: tuple = field(default_factory=tuple)

            def apply(self, matches, ground_truth, one_to_one_method="hungarian"):
                top_k = matches.take_top_n(self.k)
                hits = sum(
                    1 for cp in top_k if not self.sources or cp.source_column in self.sources
                )
                return self.return_format(hits / self.k)

        res = self.matches.get_metrics(
            self.ground_truth,
            metrics=[SupportAtK(k=2), SupportAtK(k=10), SupportAtK(k=2, sources=("Authors",))],
        )
        self.assertEqual(
            res,
            {
                "SupportAtK(k=2)": 1.0,
                "SupportAtK(k=10)": 0.5,
                "SupportAtK(k=2, sources=('Authors',))": 0.5,
            },
        )

    def test_custom_metric_hidden_field_collision_raises(self) -> None:
        # Fields with repr=False are left out of the name, so two instances that
        # differ only there can't be told apart: raise instead of dropping a score.
        @dataclass(eq=True, frozen=True)
        class Seeded(Metric):
            seed: int = field(default=0, repr=False)

            def apply(self, matches, ground_truth, one_to_one_method="hungarian"):
                return self.return_format(float(self.seed))

        self.assertEqual(Seeded(seed=1).name(), "Seeded")
        with self.assertRaisesRegex(ValueError, "Seeded"):
            self.matches.get_metrics(self.ground_truth, metrics=[Seeded(seed=1), Seeded(seed=2)])

    def test_metric_variants_do_not_overwrite(self) -> None:
        # https://github.com/delftdata/valentine/issues/105
        correct = ColumnPair("source", "a", "target", "a")
        incorrect = ColumnPair("source", "a", "target", "b")
        matches = MatcherResults({incorrect: 0.75, correct: 0.5})

        res = matches.get_metrics(
            [correct], metrics=[F1Score(one_to_one=False), F1Score(one_to_one=True)]
        )
        self.assertEqual(res, {"F1Score(one_to_one=False)": 2 / 3, "F1Score": 0.0})

    def test_metrics_all_reports_every_metric(self) -> None:
        res = self.matches.get_metrics(self.ground_truth, metrics=METRICS_ALL)
        self.assertEqual(set(res), {m.name() for m in METRICS_ALL})
        self.assertEqual(len(res), len(METRICS_ALL))
        self.assertAlmostEqual(res["Precision"], self.expected_precision_1to1, places=6)
        self.assertAlmostEqual(res["Precision(one_to_one=False)"], 0.6, places=6)

    def test_duplicate_metrics_computed_once(self) -> None:
        res = self.matches.get_metrics(self.ground_truth, metrics=[F1Score(), F1Score()])
        self.assertEqual(list(res), ["F1Score"])

    def test_metric_name_collision_raises(self) -> None:
        @dataclass(eq=True, frozen=True)
        class _Shadow(Metric):
            def name(self):
                return "F1Score"

            def apply(self, matches, ground_truth, one_to_one_method="hungarian"):
                return self.return_format(1.0)

        with self.assertRaisesRegex(ValueError, "F1Score"):
            self.matches.get_metrics(self.ground_truth, metrics=[F1Score(), _Shadow()])


if __name__ == "__main__":
    unittest.main()
