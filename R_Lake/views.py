from django.contrib.auth.mixins import LoginRequiredMixin
from django.views.generic import TemplateView
from django.db.models import Avg, Count

from ingest.models import Dataset
from visualization.models import Chart, Dashboard
from base.models import DataAsset, DataDomain, DataMetricSnapshot


class HomeView(LoginRequiredMixin, TemplateView):
    template_name = "home.html"

    def get_context_data(self, **kwargs):
        ctx = super().get_context_data(**kwargs)
        # サマリー
        ctx["stats"] = {
            "datasets": Dataset.objects.filter(is_active=True).count(),
            "charts": Chart.objects.count(),
            "dashboards": Dashboard.objects.count(),
        }
        # 最近の項目（操作導線）
        ctx["recent_datasets"] = (
            Dataset.objects.filter(is_active=True).order_by("-created_at")[:5]
        )
        ctx["my_recent_charts"] = (
            Chart.objects.filter(created_by=self.request.user).order_by("-updated_at")[:5]
            if self.request.user.is_authenticated
            else []
        )
        ctx["my_recent_dashboards"] = (
            Dashboard.objects.filter(created_by=self.request.user).order_by("-updated_at")[:5]
            if self.request.user.is_authenticated
            else []
        )
        ctx["backbone_summary"] = {
            "assets": DataAsset.objects.count(),
            "domains": DataDomain.objects.count(),
            "average_quality": DataAsset.objects.aggregate(avg_quality=Avg("quality_score")).get("avg_quality"),
        }
        ctx["recent_asset_metrics"] = (
            DataMetricSnapshot.objects.select_related("asset")
            .order_by("-metric_date")[:3]
        )
        ctx["top_domains"] = (
            DataDomain.objects.annotate(asset_total=Count("assets"))
            .order_by("-asset_total", "name")[:3]
        )
        return ctx
