import unittest
from valentine.metrics import *
from valentine.algorithms.matcher_results import MatcherResults
from valentine.metrics.metric_helpers import get_fp, get_tp_fn

class TestMetrics(unittest.TestCase):
    def setUp(self):
        self.matches = MatcherResults({
            (('table_1', 'Cited by'), ('table_2', 'Cited by')): 0.8374313,
            (('table_1', 'Authors'), ('table_2', 'Authors')): 0.83498037,
            (('table_1', 'EID'), ('table_2', 'EID')): 0.8214057,
            (('table_1', 'Title'), ('table_2', 'DUMMY1')): 0.8214057,
            (('table_1', 'Title'), ('table_2', 'DUMMY2')): 0.8114057,
        })
        self.ground_truth = [
            ('Cited by', 'Cited by'),
            ('Authors', 'Authors'),
            ('EID', 'EID'),
            ('Title', 'Title'),
            ('DUMMY3', 'DUMMY3')

        ]

    def test_precision(self):
        precision = self.matches.get_metrics(self.ground_truth, metrics={Precision()})
        assert 'Precision' in precision and precision['Precision'] == 0.75

        precision_not_one_to_one = self.matches.get_metrics(self.ground_truth, metrics={Precision(one_to_one=False)})
        assert 'Precision' in precision_not_one_to_one and precision_not_one_to_one['Precision'] == 0.6

    def test_recall(self):
        recall = self.matches.get_metrics(self.ground_truth, metrics={Recall()})
        assert 'Recall' in recall and recall['Recall'] == 0.6

        recall_not_one_to_one = self.matches.get_metrics(self.ground_truth, metrics={Recall(one_to_one=False)})
        assert 'Recall' in recall_not_one_to_one and recall_not_one_to_one['Recall'] == 0.6

    def test_f1(self):
        f1 = self.matches.get_metrics(self.ground_truth, metrics={F1Score()})
        assert 'F1Score' in f1 and round(100*f1['F1Score']) == 67

        f1_not_one_to_one = self.matches.get_metrics(self.ground_truth, metrics={F1Score(one_to_one=False)})
        assert 'F1Score' in f1_not_one_to_one and f1_not_one_to_one['F1Score'] == 0.6

    def test_precision_top_n_percent(self):
        precision_0 = self.matches.get_metrics(self.ground_truth, metrics={PrecisionTopNPercent(n=0)})
        assert 'PrecisionTop0Percent' in precision_0 and precision_0['PrecisionTop0Percent'] == 0

        precision_50 = self.matches.get_metrics(self.ground_truth, metrics={PrecisionTopNPercent(n=50)})
        assert 'PrecisionTop50Percent' in precision_50 and precision_50['PrecisionTop50Percent'] == 1.0

        precision = self.matches.get_metrics(self.ground_truth, metrics={Precision()})
        precision_100 = self.matches.get_metrics(self.ground_truth, metrics={PrecisionTopNPercent(n=100)})
        assert 'PrecisionTop100Percent' in precision_100 and precision_100['PrecisionTop100Percent'] == precision['Precision']

        precision_70_not_one_to_one = self.matches.get_metrics(self.ground_truth, metrics={PrecisionTopNPercent(n=70, one_to_one=False)})
        assert 'PrecisionTop70Percent' in precision_70_not_one_to_one and precision_70_not_one_to_one['PrecisionTop70Percent'] == 0.75

    def test_recall_at_size_of_ground_truth(self):
        recall = self.matches.get_metrics(self.ground_truth, metrics={RecallAtSizeofGroundTruth()})
        assert 'RecallAtSizeofGroundTruth' in recall and recall['RecallAtSizeofGroundTruth'] == 0.6

    def test_metric_helpers(self):
        limit = 2
        tp, fn = get_tp_fn(self.matches, self.ground_truth, n=limit)
        assert tp <= len(self.ground_truth) and fn <= len(self.ground_truth)

        fp = get_fp(self.matches, self.ground_truth, n=limit)
        assert fp <= limit
        assert tp == 2 and fn == 3  # Since we limit to 2 of the matches
        assert fp == 0

    def test_metric_equals(self):
        assert PrecisionTopNPercent(n=10, one_to_one=False) == PrecisionTopNPercent(n=10, one_to_one=False)
        assert PrecisionTopNPercent(n=10, one_to_one=False) != PrecisionTopNPercent(n=10, one_to_one=True)
        assert PrecisionTopNPercent(n=10, one_to_one=False) != Precision()
