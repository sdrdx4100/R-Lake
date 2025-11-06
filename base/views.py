from __future__ import annotations

import json
from collections import defaultdict

from django.contrib.auth.mixins import LoginRequiredMixin
from django.db.models import Count, Max, Sum
from django.shortcuts import get_object_or_404
from django.utils import timezone
from django.views.generic import DetailView, TemplateView

from .models import DataAsset, DataDomain, DataLineage, DataMetricSnapshot, DataSource


class DataCatalogOverviewView(LoginRequiredMixin, TemplateView):
    """High level overview of the strengthened data backbone."""

    template_name = "base/catalog_overview.html"

    def get_context_data(self, **kwargs):
        ctx = super().get_context_data(**kwargs)

        domains = DataDomain.objects.prefetch_related("assets")
        assets = (
            DataAsset.objects.select_related("domain", "source", "dataset")
            .annotate(latest_metric=Max("metric_snapshots__metric_date"))
            .order_by("name")
        )
        sources = DataSource.objects.annotate(asset_total=Count("assets"))
        recent_metrics = (
            DataMetricSnapshot.objects.select_related("asset")
            .order_by("-metric_date")[:8]
        )

        totals = assets.aggregate(
            total_assets=Count("id"),
            total_records=Sum("record_count"),
            avg_quality=Sum("quality_score"),
        )
        total_assets = totals.get("total_assets") or 0
        ctx["metrics"] = {
            "asset_total": total_assets,
            "domain_total": domains.count(),
            "source_total": sources.count(),
            "record_total": totals.get("total_records") or 0,
            "avg_quality": (totals.get("avg_quality") or 0) / total_assets if total_assets else None,
        }

        # Build lineage map for quick visualisation
        lineage_groups = defaultdict(list)
        for link in DataLineage.objects.select_related("upstream_asset", "downstream_asset"):
            lineage_groups[link.upstream_asset].append(link.downstream_asset)

        ctx.update(
            {
                "domains": domains,
                "assets": assets,
                "sources": sources,
                "recent_metrics": recent_metrics,
                "lineage_groups": dict(sorted(lineage_groups.items(), key=lambda item: item[0].name)),
                "generated_at": timezone.now(),
            }
        )
        return ctx


class DataAssetDetailView(LoginRequiredMixin, DetailView):
    """Detailed view of a single data asset with lineage and metrics."""

    template_name = "base/data_asset_detail.html"
    model = DataAsset
    slug_field = "slug"
    slug_url_kwarg = "slug"
    context_object_name = "asset"

    def get_context_data(self, **kwargs):
        ctx = super().get_context_data(**kwargs)
        asset: DataAsset = ctx["asset"]
        ctx["columns"] = asset.columns.select_related("schema_field").order_by("name")
        ctx["latest_metric"] = asset.latest_metrics
        ctx["upstream"] = (
            DataLineage.objects.filter(downstream_asset=asset)
            .select_related("upstream_asset")
            .order_by("-created_at")
        )
        ctx["downstream"] = (
            DataLineage.objects.filter(upstream_asset=asset)
            .select_related("downstream_asset")
            .order_by("-created_at")
        )
        ctx["contracts"] = asset.contracts.filter(is_active=True)
        ctx["recent_snapshots"] = asset.metric_snapshots.all()[:10]
        return ctx


class DataDomainDetailView(LoginRequiredMixin, TemplateView):
    """Domain-centric view highlighting ownership and coverage."""

    template_name = "base/domain_detail.html"

    def get_context_data(self, **kwargs):
        ctx = super().get_context_data(**kwargs)
        slug = self.kwargs.get("slug")
        domain = get_object_or_404(
            DataDomain.objects.prefetch_related("assets__columns"),
            slug=slug,
        )
        ctx["domain"] = domain
        ctx["assets"] = domain.assets.select_related("source", "dataset").annotate(
            column_count=Count("columns"),
            latest_metric=Max("metric_snapshots__metric_date"),
        )
        ctx["active_contracts"] = (
            domain.assets.prefetch_related("contracts")
            .values("contracts__consumer_team")
            .annotate(contract_count=Count("contracts__id"))
        )
        ctx["sources"] = domain.sources.annotate(asset_count=Count("assets"))
        return ctx


class DataSourceDetailView(LoginRequiredMixin, TemplateView):
    """Showcase linkage between an upstream source and managed assets."""

    template_name = "base/source_detail.html"

    def get_context_data(self, **kwargs):
        ctx = super().get_context_data(**kwargs)
        slug = self.kwargs.get("slug")
        source = get_object_or_404(
            DataSource.objects.prefetch_related("assets__columns"),
            slug=slug,
        )
        ctx["source"] = source
        ctx["assets"] = source.assets.select_related("domain", "dataset").annotate(
            latest_metric=Max("metric_snapshots__metric_date"),
            column_total=Count("columns"),
        )
        ctx["lineage_links"] = DataLineage.objects.filter(upstream_asset__source=source).order_by("-created_at")
        ctx["connection_pretty"] = (
            json.dumps(source.connection_details, ensure_ascii=False, indent=2)
            if source.connection_details
            else None
        )
        return ctx
