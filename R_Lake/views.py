from django.contrib.auth.mixins import LoginRequiredMixin
from django.views.generic import TemplateView
from ingest.models import Dataset
from visualization.models import Chart, Dashboard


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
        return ctx
