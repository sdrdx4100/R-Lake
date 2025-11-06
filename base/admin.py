from django.contrib import admin

from .models import (
    DataAsset,
    DataColumn,
    DataContract,
    DataDomain,
    DataLineage,
    DataMetricSnapshot,
    DataSource,
)


@admin.register(DataDomain)
class DataDomainAdmin(admin.ModelAdmin):
    list_display = ("name", "criticality", "data_steward", "business_owner", "asset_count")
    search_fields = ("name", "description")
    list_filter = ("criticality",)


@admin.register(DataSource)
class DataSourceAdmin(admin.ModelAdmin):
    list_display = ("name", "source_type", "domain", "ingestion_frequency", "last_synced_at")
    list_filter = ("source_type", "domain")
    search_fields = ("name", "description", "technical_contact")


class DataColumnInline(admin.TabularInline):
    model = DataColumn
    extra = 0
    fields = (
        "name",
        "data_type",
        "is_nullable",
        "classification",
        "stat_min",
        "stat_max",
        "stat_mean",
        "stat_distinct",
    )


@admin.register(DataAsset)
class DataAssetAdmin(admin.ModelAdmin):
    list_display = (
        "name",
        "asset_type",
        "lifecycle_state",
        "domain",
        "record_count",
        "quality_score",
        "last_validated_at",
    )
    list_filter = ("asset_type", "lifecycle_state", "domain", "data_classification")
    search_fields = ("name", "summary", "tags")
    inlines = [DataColumnInline]
    readonly_fields = ("created_at", "updated_at")


@admin.register(DataLineage)
class DataLineageAdmin(admin.ModelAdmin):
    list_display = (
        "upstream_asset",
        "downstream_asset",
        "transformation_type",
        "is_active",
        "created_at",
    )
    list_filter = ("transformation_type", "is_active")
    search_fields = ("upstream_asset__name", "downstream_asset__name")


@admin.register(DataContract)
class DataContractAdmin(admin.ModelAdmin):
    list_display = (
        "asset",
        "consumer_team",
        "is_active",
        "refresh_schedule",
    )
    list_filter = ("is_active",)
    search_fields = ("asset__name", "consumer_team")


@admin.register(DataMetricSnapshot)
class DataMetricSnapshotAdmin(admin.ModelAdmin):
    list_display = (
        "asset",
        "metric_date",
        "total_records",
        "valid_records",
        "invalid_records",
        "quality_score",
    )
    list_filter = ("metric_date", "asset")
    date_hierarchy = "metric_date"
