from django.contrib.auth.models import User
from django.test import Client, TestCase

from ingest.models import Dataset, DataRecord
from visualization.models import Chart


class ChartDataAPITests(TestCase):
    """Tests for the chart data API."""

    def setUp(self) -> None:
        self.user = User.objects.create_user(username="test", password="pass")
        self.client = Client()
        self.client.login(username="test", password="pass")

        # Create a minimal dataset with a single record
        self.dataset = Dataset.objects.create(name="ds", created_by=self.user)
        DataRecord.objects.create(
            dataset=self.dataset,
            row_number=1,
            data={"x": 1, "y": 2},
            data_hash="hash1",
        )

        self.chart = Chart.objects.create(
            title="test chart",
            chart_type="line",
            dataset=self.dataset,
            created_by=self.user,
            x_axis_column="x",
            y_axis_column="y",
            chart_config={},
        )

    def test_chart_data_api_returns_traces(self) -> None:
        response = self.client.get(f"/visualization/api/charts/{self.chart.id}/data/")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertIn("traces", data)


class ChartPreviewAPITests(TestCase):
    """Tests for the chart preview API."""

    def setUp(self) -> None:
        self.user = User.objects.create_user(username="preview", password="pass")
        self.client = Client()
        self.client.login(username="preview", password="pass")

        self.dataset = Dataset.objects.create(name="ds", created_by=self.user)
        DataRecord.objects.create(
            dataset=self.dataset,
            row_number=1,
            data={"x": 1, "y": 2},
            data_hash="hash2",
        )

    def test_preview_new_returns_traces(self) -> None:
        response = self.client.post(
            "/visualization/api/charts/preview/",
            {
                "dataset": self.dataset.id,
                "title": "tmp",
                "chart_type": "line",
                "x_axis_column": "x",
                "y_axis_column": "y",
            },
        )
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertTrue(data["success"])
        self.assertIn("traces", data)
