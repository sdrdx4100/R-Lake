from __future__ import annotations

from django.db.models.signals import post_save
from django.dispatch import receiver
from django.utils import timezone

from ingest.models import DataQualityReport, DataSchema, Dataset

from .models import DataAsset, DataColumn, DataMetricSnapshot


@receiver(post_save, sender=Dataset)
def ensure_catalog_asset(sender, instance: Dataset, created: bool, **kwargs):
    """Ensure every dataset has a catalog asset entry."""

    asset, asset_created = DataAsset.objects.get_or_create(
        dataset=instance,
        defaults={
            "name": instance.name,
            "summary": instance.description,
            "record_count": instance.total_rows,
            "tags": instance.tags,
        },
    )

    if not asset_created:
        update_fields = ["record_count", "tags", "summary"]
        asset.record_count = instance.total_rows
        asset.summary = instance.description
        asset.tags = instance.tags
        if instance.measurement_end:
            asset.last_validated_at = instance.measurement_end
            update_fields.append("last_validated_at")
        asset.save(update_fields=update_fields)


@receiver(post_save, sender=DataSchema)
def sync_catalog_columns(sender, instance: DataSchema, **kwargs):
    """Keep the column catalog aligned with schema definition."""

    dataset = instance.dataset
    asset = getattr(dataset, "catalog_asset", None)
    if not asset:
        asset = DataAsset.objects.create(
            dataset=dataset,
            name=dataset.name,
            summary=dataset.description,
            record_count=dataset.total_rows,
            tags=dataset.tags,
        )

    column_defaults = {
        "name": instance.column_name,
        "data_type": instance.column_type,
        "is_nullable": instance.is_nullable,
        "stat_min": instance.min_value,
        "stat_max": instance.max_value,
        "stat_distinct": instance.unique_count,
    }
    DataColumn.objects.update_or_create(
        schema_field=instance,
        defaults={
            **column_defaults,
            "asset": asset,
        },
    )


@receiver(post_save, sender=DataQualityReport)
def create_metric_snapshot(sender, instance: DataQualityReport, created: bool, **kwargs):
    """Capture quality metrics whenever a report is generated."""

    if not created:
        return

    asset = getattr(instance.dataset, "catalog_asset", None)
    if not asset:
        asset = DataAsset.objects.create(
            dataset=instance.dataset,
            name=instance.dataset.name,
            summary=instance.dataset.description,
            record_count=instance.dataset.total_rows,
        )

    total = instance.total_records or 0
    valid = instance.valid_records or 0
    invalid = instance.invalid_records or 0
    duplicate = instance.duplicate_records or 0

    completeness = (valid / total * 100) if total else None
    uniqueness = ((total - duplicate) / total * 100) if total else None
    quality_score = (valid / total * 100) if total else None

    freshness_hours = None
    if instance.dataset.measurement_end:
        delta = instance.report_date - instance.dataset.measurement_end
        freshness_hours = max(delta.total_seconds() / 3600, 0)

    snapshot = DataMetricSnapshot.objects.create(
        asset=asset,
        metric_date=instance.report_date,
        total_records=total,
        valid_records=valid,
        invalid_records=invalid,
        duplicate_records=duplicate,
        completeness_percent=completeness,
        uniqueness_percent=uniqueness,
        freshness_lag_hours=freshness_hours,
        quality_score=quality_score,
        details=instance.quality_details,
    )

    asset.record_count = total
    asset.quality_score = snapshot.quality_score
    asset.last_validated_at = timezone.now()
    asset.save(update_fields=["record_count", "quality_score", "last_validated_at"])
