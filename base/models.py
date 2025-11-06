from __future__ import annotations

from uuid import uuid4

from django.contrib.auth import get_user_model
from django.db import models
from django.utils import timezone
from django.utils.text import slugify


User = get_user_model()


class TimeStampedModel(models.Model):
    """Base model providing created/updated timestamps."""

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        abstract = True


class SluggedModel(TimeStampedModel):
    """Adds automatic slug generation to timestamped models."""

    slug = models.SlugField(max_length=160, unique=True, blank=True)

    slug_source_field = "name"

    class Meta:
        abstract = True

    def _generate_unique_slug(self, base_value: str | None) -> str:
        base_slug = slugify(base_value or "")[:140]
        if not base_slug:
            base_slug = uuid4().hex[:12]
        slug_candidate = base_slug
        counter = 1
        Model = self.__class__
        while Model.objects.filter(slug=slug_candidate).exclude(pk=self.pk).exists():
            slug_candidate = f"{base_slug}-{counter}"
            counter += 1
        return slug_candidate

    def save(self, *args, **kwargs):  # noqa: D401 - documented via class docstring
        if not self.slug:
            base_attr = getattr(self, self.slug_source_field, None)
            self.slug = self._generate_unique_slug(base_attr)
        super().save(*args, **kwargs)


class DataDomain(SluggedModel):
    """Business domain that groups datasets and analytics assets."""

    CRITICALITY_CHOICES = [
        ("low", "Low"),
        ("medium", "Medium"),
        ("high", "High"),
    ]

    name = models.CharField(max_length=255, unique=True)
    description = models.TextField(blank=True)
    criticality = models.CharField(max_length=10, choices=CRITICALITY_CHOICES, default="medium")
    data_steward = models.ForeignKey(
        User,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="stewarded_domains",
    )
    business_owner = models.ForeignKey(
        User,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="owned_domains",
    )
    default_retention_days = models.PositiveIntegerField(default=365)

    class Meta:
        ordering = ["name"]
        verbose_name = "データドメイン"
        verbose_name_plural = "データドメイン"

    def __str__(self) -> str:
        return self.name

    @property
    def asset_count(self) -> int:
        return self.assets.count()


class DataSource(SluggedModel):
    """Upstream source system or ingestion channel."""

    SOURCE_TYPES = [
        ("database", "Database"),
        ("stream", "Stream"),
        ("file", "File Upload"),
        ("external", "External API"),
        ("manual", "Manual"),
    ]

    slug_source_field = "name"

    name = models.CharField(max_length=255, unique=True)
    source_type = models.CharField(max_length=20, choices=SOURCE_TYPES, default="file")
    domain = models.ForeignKey(DataDomain, on_delete=models.SET_NULL, null=True, blank=True, related_name="sources")
    description = models.TextField(blank=True)
    owner = models.ForeignKey(
        User,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="owned_sources",
    )
    technical_contact = models.CharField(max_length=255, blank=True)
    ingestion_frequency = models.CharField(max_length=100, blank=True)
    connection_details = models.JSONField(default=dict, blank=True)
    last_synced_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        ordering = ["name"]
        verbose_name = "データソース"
        verbose_name_plural = "データソース"

    def __str__(self) -> str:
        return self.name


class DataAsset(SluggedModel):
    """Central catalog entry describing an analytical dataset."""

    ASSET_TYPES = [
        ("raw", "Raw"),
        ("curated", "Curated"),
        ("analytics", "Analytics"),
        ("feature", "Feature Store"),
    ]

    LIFECYCLE_STATES = [
        ("draft", "Draft"),
        ("active", "Active"),
        ("deprecated", "Deprecated"),
        ("archived", "Archived"),
    ]

    CLASSIFICATION_CHOICES = [
        ("public", "Public"),
        ("internal", "Internal"),
        ("confidential", "Confidential"),
        ("restricted", "Restricted"),
    ]

    slug_source_field = "name"

    name = models.CharField(max_length=255)
    summary = models.TextField(blank=True)
    dataset = models.OneToOneField(
        "ingest.Dataset",
        on_delete=models.CASCADE,
        related_name="catalog_asset",
        null=True,
        blank=True,
    )
    domain = models.ForeignKey(
        DataDomain,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="assets",
    )
    source = models.ForeignKey(
        DataSource,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="assets",
    )
    asset_type = models.CharField(max_length=20, choices=ASSET_TYPES, default="curated")
    lifecycle_state = models.CharField(max_length=20, choices=LIFECYCLE_STATES, default="active")
    data_classification = models.CharField(
        max_length=20,
        choices=CLASSIFICATION_CHOICES,
        default="internal",
    )
    owner = models.ForeignKey(
        User,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="owned_assets",
    )
    steward = models.ForeignKey(
        User,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="stewarded_assets",
    )
    refresh_cadence = models.CharField(max_length=120, blank=True)
    retention_days = models.PositiveIntegerField(null=True, blank=True)
    governance_policies = models.TextField(blank=True)
    lineage_summary = models.TextField(blank=True)
    record_count = models.BigIntegerField(default=0)
    data_volume_mb = models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True)
    quality_score = models.DecimalField(max_digits=5, decimal_places=2, null=True, blank=True)
    last_validated_at = models.DateTimeField(null=True, blank=True)
    tags = models.CharField(max_length=255, blank=True)

    class Meta:
        ordering = ["name"]
        unique_together = ["name", "domain"]
        verbose_name = "データアセット"
        verbose_name_plural = "データアセット"

    def __str__(self) -> str:
        return self.name

    def sync_from_dataset(self) -> None:
        """Copy core metadata from the linked dataset if available."""
        if not self.dataset:
            return
        ds = self.dataset
        self.name = self.name or ds.name
        self.summary = self.summary or ds.description
        self.record_count = ds.total_rows
        if ds.project and not self.domain:
            self.domain = DataDomain.objects.filter(name=ds.project).first()
        self.tags = ds.tags or self.tags
        if ds.measurement_end:
            self.last_validated_at = ds.measurement_end
        self.save()

    @property
    def latest_metrics(self):  # noqa: D401
        """Return the most recent metric snapshot if present."""
        return self.metric_snapshots.order_by("-metric_date").first()


class DataColumn(TimeStampedModel):
    """Column-level metadata for a data asset."""

    DATA_CLASSIFICATIONS = [
        ("identifier", "Identifier"),
        ("sensitive", "Sensitive"),
        ("general", "General"),
        ("derived", "Derived"),
    ]

    asset = models.ForeignKey(DataAsset, on_delete=models.CASCADE, related_name="columns")
    schema_field = models.OneToOneField(
        "ingest.DataSchema",
        on_delete=models.CASCADE,
        related_name="catalog_column",
        null=True,
        blank=True,
    )
    name = models.CharField(max_length=255)
    data_type = models.CharField(max_length=50)
    description = models.TextField(blank=True)
    business_definition = models.TextField(blank=True)
    is_nullable = models.BooleanField(default=True)
    classification = models.CharField(
        max_length=20,
        choices=DATA_CLASSIFICATIONS,
        default="general",
    )
    quality_rules = models.JSONField(default=dict, blank=True)
    sample_values = models.JSONField(default=list, blank=True)
    stat_min = models.FloatField(null=True, blank=True)
    stat_max = models.FloatField(null=True, blank=True)
    stat_mean = models.FloatField(null=True, blank=True)
    stat_distinct = models.IntegerField(null=True, blank=True)

    class Meta:
        ordering = ["asset", "name"]
        unique_together = ["asset", "name"]
        verbose_name = "データカラム"
        verbose_name_plural = "データカラム"

    def __str__(self) -> str:
        return f"{self.asset.name}.{self.name}"


class DataLineage(TimeStampedModel):
    """Represents relationships between upstream/downstream assets."""

    TRANSFORMATION_TYPES = [
        ("ingest", "Ingestion"),
        ("transform", "Transformation"),
        ("join", "Join"),
        ("aggregate", "Aggregation"),
        ("manual", "Manual"),
    ]

    upstream_asset = models.ForeignKey(
        DataAsset,
        on_delete=models.CASCADE,
        related_name="downstream_links",
    )
    downstream_asset = models.ForeignKey(
        DataAsset,
        on_delete=models.CASCADE,
        related_name="upstream_links",
    )
    transformation_type = models.CharField(max_length=20, choices=TRANSFORMATION_TYPES, default="transform")
    transformation_description = models.TextField(blank=True)
    impact_notes = models.TextField(blank=True)
    is_active = models.BooleanField(default=True)

    class Meta:
        unique_together = ["upstream_asset", "downstream_asset", "transformation_type"]
        verbose_name = "データリネージ"
        verbose_name_plural = "データリネージ"

    def __str__(self) -> str:
        return f"{self.upstream_asset} -> {self.downstream_asset}"


class DataContract(TimeStampedModel):
    """Defines sharing agreements for curated assets."""

    asset = models.ForeignKey(DataAsset, on_delete=models.CASCADE, related_name="contracts")
    consumer_team = models.CharField(max_length=255)
    contract_summary = models.TextField(blank=True)
    sla_description = models.TextField(blank=True)
    refresh_schedule = models.CharField(max_length=120, blank=True)
    delivery_channel = models.CharField(max_length=255, blank=True)
    escalation_contact = models.CharField(max_length=255, blank=True)
    metadata = models.JSONField(default=dict, blank=True)
    is_active = models.BooleanField(default=True)

    class Meta:
        ordering = ["consumer_team"]
        verbose_name = "データ契約"
        verbose_name_plural = "データ契約"

    def __str__(self) -> str:
        return f"{self.asset.name} contract for {self.consumer_team}"


class DataMetricSnapshot(TimeStampedModel):
    """Stores data quality/backbone metrics for assets."""

    asset = models.ForeignKey(DataAsset, on_delete=models.CASCADE, related_name="metric_snapshots")
    metric_date = models.DateTimeField(default=timezone.now)
    total_records = models.BigIntegerField(default=0)
    valid_records = models.BigIntegerField(default=0)
    invalid_records = models.BigIntegerField(default=0)
    duplicate_records = models.BigIntegerField(default=0)
    freshness_lag_hours = models.FloatField(null=True, blank=True)
    completeness_percent = models.FloatField(null=True, blank=True)
    uniqueness_percent = models.FloatField(null=True, blank=True)
    quality_score = models.FloatField(null=True, blank=True)
    details = models.JSONField(default=dict, blank=True)

    class Meta:
        ordering = ["-metric_date"]
        verbose_name = "データメトリクス"
        verbose_name_plural = "データメトリクス"

    def __str__(self) -> str:
        return f"{self.asset.name} metrics @ {self.metric_date:%Y-%m-%d %H:%M}"

    @property
    def invalid_ratio(self) -> float | None:
        if not self.total_records:
            return None
        return self.invalid_records / self.total_records
