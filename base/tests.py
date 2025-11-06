from datetime import timedelta

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.utils import timezone

from ingest.models import DataQualityReport, DataSchema, Dataset

from .models import DataAsset, DataColumn, DataMetricSnapshot


User = get_user_model()


class DataBackboneSignalTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_user(username="tester", password="secret")

    def _create_dataset(self, name: str = "テストデータセット", rows: int = 0) -> Dataset:
        return Dataset.objects.create(
            name=name,
            description="サンプル説明",
            created_by=self.user,
            total_rows=rows,
            tags="sample,vehicle",
        )

    def test_catalog_asset_created_for_new_dataset(self):
        dataset = self._create_dataset(rows=120)

        self.assertTrue(hasattr(dataset, "catalog_asset"))
        asset = dataset.catalog_asset
        self.assertIsInstance(asset, DataAsset)
        self.assertEqual(asset.record_count, 120)
        self.assertEqual(asset.name, dataset.name)

        # Updating dataset should propagate row count
        dataset.total_rows = 200
        dataset.description = "更新後説明"
        dataset.save()
        asset.refresh_from_db()
        self.assertEqual(asset.record_count, 200)
        self.assertEqual(asset.summary, "更新後説明")

    def test_catalog_column_synced_from_schema(self):
        dataset = self._create_dataset()
        schema = DataSchema.objects.create(
            dataset=dataset,
            column_name="speed",
            column_type="FLOAT",
            is_nullable=False,
            column_order=1,
            min_value=0,
            max_value=240,
            unique_count=120,
        )

        column = DataColumn.objects.get(schema_field=schema)
        self.assertEqual(column.asset, dataset.catalog_asset)
        self.assertEqual(column.name, "speed")
        self.assertFalse(column.is_nullable)
        self.assertEqual(column.stat_min, 0)
        self.assertEqual(column.stat_max, 240)
        self.assertEqual(column.stat_distinct, 120)

    def test_metric_snapshot_created_from_quality_report(self):
        dataset = self._create_dataset(rows=100)
        dataset.measurement_end = timezone.now() - timedelta(hours=2)
        dataset.save()

        report = DataQualityReport.objects.create(
            dataset=dataset,
            total_records=100,
            valid_records=95,
            invalid_records=5,
            duplicate_records=2,
            quality_details={"issues": []},
        )

        snapshot = DataMetricSnapshot.objects.get(asset=dataset.catalog_asset, metric_date=report.report_date)
        self.assertEqual(snapshot.valid_records, 95)
        self.assertAlmostEqual(snapshot.completeness_percent, 95.0)
        self.assertAlmostEqual(snapshot.uniqueness_percent, 98.0)
        self.assertAlmostEqual(snapshot.quality_score, 95.0)

        asset = dataset.catalog_asset
        asset.refresh_from_db()
        self.assertEqual(asset.record_count, 100)
        self.assertAlmostEqual(float(asset.quality_score), 95.0)
        self.assertIsNotNone(asset.last_validated_at)
