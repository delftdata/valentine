import numpy as np
import pandas as pd

from tests import df1, df2
from valentine import valentine_match
from valentine.algorithms import DistributionBased
from valentine.metrics import F1Score, Precision, Recall


class TestDistributionBasedBenchmark:
    """
    Accuracy benchmarks for the DistributionBased algorithm.
    These tests capture baseline metrics to detect regressions
    when aligning the implementation with the paper.
    """

    def test_authors_accuracy(self):
        """Baseline accuracy on the authors dataset."""
        matches = valentine_match(df1, df2, DistributionBased())
        ground_truth = [
            ("Cited by", "Cited by"),
            ("Authors", "Authors"),
            ("EID", "EID"),
        ]
        metrics = matches.get_metrics(ground_truth, metrics={Precision(), Recall(), F1Score()})

        # Baseline: P=1.0, R=1.0, F1=1.0 (all 3 matches correct, no false positives)
        assert metrics["Precision"] >= 1.0, f"Precision dropped to {metrics['Precision']}"
        assert metrics["Recall"] >= 1.0, f"Recall dropped to {metrics['Recall']}"
        assert metrics["F1Score"] >= 1.0, f"F1Score dropped to {metrics['F1Score']}"

    def test_synthetic_numeric_accuracy(self):
        """Numeric columns with known matching distributions."""
        rng = np.random.default_rng(42)
        n = 200

        source = pd.DataFrame(
            {
                "id": np.arange(1, n + 1),
                "price": rng.normal(50, 10, n).round(2),
                "quantity": rng.integers(1, 100, n),
                "rating": rng.uniform(1, 5, n).round(1),
            }
        )

        target = pd.DataFrame(
            {
                "identifier": np.arange(1, n + 1),
                "cost": rng.normal(50, 10, n).round(2),
                "amount": rng.integers(1, 100, n),
                "score": rng.uniform(1, 5, n).round(1),
                "unrelated": rng.integers(5000, 6000, n),
            }
        )

        ground_truth = [
            ("id", "identifier"),
            ("price", "cost"),
            ("quantity", "amount"),
            ("rating", "score"),
        ]

        matches = valentine_match(source, target, DistributionBased())
        metrics = matches.get_metrics(ground_truth, metrics={Precision(), Recall(), F1Score()})

        # Baseline: P=1.0, R=0.75, F1=0.857
        # The algorithm correctly finds all 4 pairs (raw Recall=1.0), but one_to_one()
        # post-processing may filter the weakest match below the median threshold.
        assert metrics["Precision"] >= 1.0, f"Precision dropped to {metrics['Precision']}"
        assert metrics["Recall"] >= 0.75, f"Recall dropped to {metrics['Recall']}"
        assert metrics["F1Score"] >= 0.85, f"F1Score dropped to {metrics['F1Score']}"

    def test_synthetic_string_accuracy(self):
        """String columns with overlapping/disjoint value sets."""
        rng = np.random.default_rng(42)
        n = 100

        cities = [
            "New York",
            "London",
            "Paris",
            "Tokyo",
            "Berlin",
            "Madrid",
            "Rome",
            "Sydney",
            "Toronto",
            "Dubai",
            "Mumbai",
            "Shanghai",
            "Seoul",
            "Bangkok",
            "Istanbul",
            "Moscow",
            "Cairo",
            "Lagos",
            "Lima",
            "Jakarta",
        ]
        names = [
            "Alice",
            "Bob",
            "Charlie",
            "Diana",
            "Eve",
            "Frank",
            "Grace",
            "Hank",
            "Iris",
            "Jack",
            "Karen",
            "Leo",
            "Mona",
            "Nick",
            "Olivia",
            "Paul",
            "Quinn",
            "Rosa",
            "Sam",
            "Tina",
        ]
        countries = [
            "USA",
            "UK",
            "France",
            "Germany",
            "Japan",
            "Australia",
            "Canada",
            "Brazil",
            "India",
            "China",
        ]

        source = pd.DataFrame(
            {
                "city": rng.choice(cities, n),
                "name": rng.choice(names, n),
            }
        )

        target = pd.DataFrame(
            {
                "location": rng.choice(cities, n),
                "person": rng.choice(names, n),
                "country": rng.choice(countries, n),
            }
        )

        ground_truth = [
            ("city", "location"),
            ("name", "person"),
        ]

        matches = valentine_match(source, target, DistributionBased())
        metrics = matches.get_metrics(ground_truth, metrics={Precision(), Recall(), F1Score()})

        # Baseline: P=1.0, R=1.0, F1=1.0 (both string pairs matched correctly)
        assert metrics["Precision"] >= 1.0, f"Precision dropped to {metrics['Precision']}"
        assert metrics["Recall"] >= 1.0, f"Recall dropped to {metrics['Recall']}"
        assert metrics["F1Score"] >= 1.0, f"F1Score dropped to {metrics['F1Score']}"

    def test_bloom_filter_accuracy(self):
        """Verify that Bloom filter mode produces comparable results to exact intersection."""
        matches_exact = valentine_match(df1, df2, DistributionBased())
        matches_bloom = valentine_match(df1, df2, DistributionBased(use_bloom_filters=True))
        ground_truth = [
            ("Cited by", "Cited by"),
            ("Authors", "Authors"),
            ("EID", "EID"),
        ]
        metrics_exact = matches_exact.get_metrics(
            ground_truth, metrics={Precision(), Recall(), F1Score()}
        )
        metrics_bloom = matches_bloom.get_metrics(
            ground_truth, metrics={Precision(), Recall(), F1Score()}
        )

        # Bloom filters may introduce false positives in intersection, so accuracy
        # can differ slightly. Ensure it doesn't drop catastrophically.
        assert metrics_bloom["Precision"] >= 0.5, (
            f"Bloom Precision too low: {metrics_bloom['Precision']}"
        )
        assert metrics_bloom["Recall"] >= metrics_exact["Recall"] * 0.8, (
            f"Bloom Recall dropped too much: {metrics_bloom['Recall']} vs {metrics_exact['Recall']}"
        )
