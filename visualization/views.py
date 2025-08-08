from django.shortcuts import render, get_object_or_404, redirect
from django.contrib.auth.decorators import login_required
from django.contrib.auth.mixins import LoginRequiredMixin
from django.views.generic import CreateView, ListView, DetailView
from django.http import JsonResponse, HttpResponse
from django.contrib import messages
from django.db import models
from rest_framework import viewsets, status
from rest_framework.decorators import action
from rest_framework.response import Response
from .models import Chart, Dashboard, AnalysisTemplate, DataComparison, UserPreference
from .chart_engine import ChartGenerator, AnalysisEngine
from .serializers import (
    ChartSerializer,
    DashboardSerializer,
    AnalysisTemplateSerializer,
    ChartCreateSerializer,
)
from ingest.models import Dataset
import json
import plotly
import plotly.graph_objects as go
import logging

logger = logging.getLogger(__name__)


@login_required
def chart_create(request):
    """グラフ作成ページ"""
    if request.method == "POST":
        try:
            dataset_id = request.POST.get("dataset")
            dataset = get_object_or_404(Dataset, id=dataset_id, is_active=True)

            create_payload = {
                "title": request.POST.get("title"),
                "chart_type": request.POST.get("chart_type"),
                "dataset": dataset.id,
                "x_axis_column": request.POST.get("x_axis_column"),
                "y_axis_column": request.POST.get("y_axis_column"),
                "z_axis_column": request.POST.get("z_axis_column", ""),
                "color_column": request.POST.get("color_column", ""),
                "size_column": request.POST.get("size_column", ""),
                "color_scheme": request.POST.get("color_scheme", "viridis"),
                "chart_config": {},
                "filters": {},
            }

            serializer = ChartCreateSerializer(data=create_payload)
            serializer.is_valid(raise_exception=True)

            chart = Chart.objects.create(
                title=serializer.validated_data["title"],
                chart_type=serializer.validated_data["chart_type"],
                dataset=dataset,
                created_by=request.user,
                x_axis_column=serializer.validated_data.get("x_axis_column", ""),
                y_axis_column=serializer.validated_data.get("y_axis_column", ""),
                z_axis_column=serializer.validated_data.get("z_axis_column", ""),
                color_column=serializer.validated_data.get("color_column", ""),
                size_column=serializer.validated_data.get("size_column", ""),
                color_scheme=serializer.validated_data.get("color_scheme", "viridis"),
                chart_config=serializer.validated_data.get("chart_config", {}),
                filters=serializer.validated_data.get("filters", {}),
            )

            messages.success(request, f'グラフ "{chart.title}" が作成されました。')
            return redirect("visualization:chart_detail", pk=chart.pk)

        except Exception as e:
            logger.error(f"グラフ作成エラー: {e}")
            messages.error(request, f"グラフ作成中にエラーが発生しました: {str(e)}")

    # データセット一覧を取得
    datasets = Dataset.objects.filter(is_active=True).order_by("-created_at")
    return render(request, "visualization/chart_create.html", {"datasets": datasets})


class ChartListView(LoginRequiredMixin, ListView):
    """グラフ一覧"""

    model = Chart
    template_name = "visualization/chart_list.html"
    context_object_name = "charts"
    paginate_by = 20

    def get_queryset(self):
        return Chart.objects.filter(created_by=self.request.user).order_by(
            "-updated_at"
        )


class DashboardListView(LoginRequiredMixin, ListView):
    """ダッシュボード一覧"""

    model = Dashboard
    template_name = "visualization/dashboard_list.html"
    context_object_name = "dashboards"
    paginate_by = 20

    def get_queryset(self):
        qs = Dashboard.objects.filter(created_by=self.request.user)
        q = self.request.GET.get("q")
        if q:
            qs = qs.filter(models.Q(name__icontains=q) | models.Q(description__icontains=q))
        return qs.order_by("-updated_at")


class ChartDetailView(LoginRequiredMixin, DetailView):
    """グラフ詳細"""

    model = Chart
    template_name = "visualization/chart_detail.html"
    context_object_name = "chart"

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        chart = self.object

        try:
            # グラフを生成
            generator = ChartGenerator()
            fig, result = generator.generate_chart(chart)

            # HTML形式で変換
            chart_html = plotly.offline.plot(
                fig, output_type="div", include_plotlyjs=False
            )

            context["chart_html"] = chart_html
            context["generation_result"] = result

        except Exception as e:
            logger.error(f"グラフ生成エラー: {e}")
            context["chart_error"] = str(e)

        # データセットの情報
        context["dataset"] = chart.dataset
        context["schema_fields"] = chart.dataset.schema_fields.all()

        return context


@login_required
def chart_edit(request, pk):
    """グラフ編集ページ"""
    chart = get_object_or_404(Chart, pk=pk)

    # 権限チェック - 作成者またはスーパーユーザーのみ編集可能
    if chart.created_by != request.user and not request.user.is_superuser:
        messages.error(request, "このグラフを編集する権限がありません。")
        return redirect("visualization:chart_detail", pk=pk)

    if request.method == "POST":
        try:
            # フォームデータを取得
            chart.title = request.POST.get("title", chart.title)
            chart.description = request.POST.get("description", chart.description)
            chart.chart_type = request.POST.get("chart_type", chart.chart_type)
            chart.x_axis_column = request.POST.get("x_axis_column", chart.x_axis_column)
            chart.y_axis_column = request.POST.get("y_axis_column", chart.y_axis_column)
            chart.color_column = request.POST.get("color_column", "")
            chart.size_column = request.POST.get("size_column", "")

            # 設定を保存
            chart.save()

            messages.success(request, "グラフが正常に更新されました。")
            return redirect("visualization:chart_detail", pk=pk)

        except Exception as e:
            logger.error(f"グラフ更新エラー: {e}")
            messages.error(request, f"グラフの更新に失敗しました: {str(e)}")

    context = {
        "chart": chart,
        "dataset": chart.dataset,
        "schema_fields": (
            chart.dataset.schema_fields.all()
            if hasattr(chart.dataset, "schema_fields")
            else []
        ),
    }

    return render(request, "visualization/chart_edit.html", context)


@login_required
def chart_data_api(request, chart_id):
    """グラフデータをJSON形式で返す"""
    try:
        chart = get_object_or_404(Chart, id=chart_id)

        # 権限チェック
        if chart.created_by != request.user and not chart.is_public:
            return JsonResponse({"error": "権限がありません"}, status=403)

        generator = ChartGenerator()
        fig, result = generator.generate_chart(chart)

        if result["success"]:
            # PlotlyのエンコーダでJSON化された文字列を一度読み戻して安全に返却
            fig_json_str = fig.to_json()
            fig_json = json.loads(fig_json_str)
            return JsonResponse({"traces": fig_json.get("data", []), "result": result})
        else:
            return JsonResponse(
                {"error": result.get("error", "不明なエラー")}, status=500
            )

    except Exception as e:
        logger.error(f"グラフデータAPI エラー: {e}")
        return JsonResponse({"error": str(e)}, status=500)


@login_required
def chart_stats_api(request, chart_id):
    """グラフの統計情報をJSON形式で返す"""
    try:
        chart = get_object_or_404(Chart, id=chart_id)

        # 権限チェック
        if chart.created_by != request.user and not chart.is_public:
            return JsonResponse({"error": "権限がありません"}, status=403)

        # ダミーの統計情報を返す（実際のデータ分析は後で実装）
        stats = {
            "データ点数": "1,000",
            "X軸範囲": "0.0 - 100.0",
            "Y軸範囲": "-50.0 - 50.0",
            "最終更新": chart.updated_at.strftime("%Y年%m月%d日 %H:%M"),
        }

        return JsonResponse(stats)

    except Exception as e:
        logger.error(f"グラフ統計API エラー: {e}")
        return JsonResponse({"error": str(e)}, status=500)


@login_required
def dataset_columns_api(request, dataset_id):
    """データセットのカラム情報をJSON形式で返す"""
    try:
        dataset = get_object_or_404(Dataset, id=dataset_id, is_active=True)
        schema_fields = dataset.schema_fields.all()

        columns = []
        for field in schema_fields:
            columns.append(
                {
                    "name": field.column_name,
                    "type": field.column_type,
                    "nullable": field.is_nullable,
                }
            )

        return JsonResponse({"columns": columns})

    except Exception as e:
        return JsonResponse({"error": str(e)}, status=500)


@login_required
def dashboard_create(request):
    """ダッシュボード作成ページ"""
    if request.method == "POST":
        try:
            name = request.POST.get("name")
            description = request.POST.get("description", "")

            dashboard = Dashboard.objects.create(
                name=name,
                description=description,
                created_by=request.user,
                layout_config={},
            )

            messages.success(request, f'ダッシュボード "{name}" が作成されました。')
            return redirect("visualization:dashboard_detail", pk=dashboard.pk)

        except Exception as e:
            logger.error(f"ダッシュボード作成エラー: {e}")
            messages.error(
                request, f"ダッシュボード作成中にエラーが発生しました: {str(e)}"
            )

    return render(request, "visualization/dashboard_create.html")


class DashboardDetailView(LoginRequiredMixin, DetailView):
    """ダッシュボード詳細"""

    model = Dashboard
    template_name = "visualization/dashboard_detail.html"
    context_object_name = "dashboard"

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        dashboard = self.object

        # ダッシュボード内のグラフを取得
        dashboard_charts = dashboard.dashboardchart_set.filter(
            is_visible=True
        ).order_by("display_order")

        charts_data = []
        generator = ChartGenerator()

        for dashboard_chart in dashboard_charts:
            chart = dashboard_chart.chart
            try:
                fig, result = generator.generate_chart(chart)
                chart_html = plotly.offline.plot(
                    fig, output_type="div", include_plotlyjs=False
                )

                charts_data.append(
                    {
                        "chart": chart,
                        "chart_html": chart_html,
                        "grid_info": {
                            "x": dashboard_chart.grid_x,
                            "y": dashboard_chart.grid_y,
                            "width": dashboard_chart.grid_width,
                            "height": dashboard_chart.grid_height,
                        },
                    }
                )

            except Exception as e:
                logger.error(f"ダッシュボードグラフ生成エラー: {e}")
                charts_data.append(
                    {
                        "chart": chart,
                        "chart_error": str(e),
                        "grid_info": {
                            "x": dashboard_chart.grid_x,
                            "y": dashboard_chart.grid_y,
                            "width": dashboard_chart.grid_width,
                            "height": dashboard_chart.grid_height,
                        },
                    }
                )

        context["charts_data"] = charts_data
        # 追加可能なグラフ（このダッシュボードに未追加の自分のグラフ）
        added_ids = [dc.chart_id for dc in dashboard.dashboardchart_set.all()]
        available = Chart.objects.filter(created_by=self.request.user)
        if added_ids:
            available = available.exclude(id__in=added_ids)
        context["available_charts"] = available.order_by("-updated_at")

        # テーブル設定（layout_config.tables に dataset_id の配列を保持）
        tables_cfg = {}
        try:
            tables_cfg = dashboard.layout_config or {}
        except Exception:
            tables_cfg = {}
        table_ids = tables_cfg.get("tables", []) if isinstance(tables_cfg, dict) else []
        table_datasets = Dataset.objects.filter(id__in=table_ids, is_active=True)
        # 表示順は table_ids の順序を維持
        table_dataset_map = {d.id: d for d in table_datasets}
        context["table_datasets"] = [table_dataset_map[i] for i in table_ids if i in table_dataset_map]

        # テーブル追加用に全データセット（簡易; 将来は権限/フィルタ適用）
        context["all_datasets"] = Dataset.objects.filter(is_active=True).order_by("-created_at")
        return context

    def post(self, request, *args, **kwargs):
        """フォーム経由のグラフ追加（モーダルPOST）"""
        self.object = self.get_object()
        chart_id = request.POST.get("chart_id")
        grid_width = int(request.POST.get("grid_width", 1))
        grid_height = int(request.POST.get("grid_height", 1))

        try:
            chart = Chart.objects.get(id=chart_id, created_by=request.user)
            # APIと同様の重複チェック
            if self.object.charts.filter(id=chart_id).exists():
                messages = __import__('django.contrib.messages').contrib.messages
                messages.error(request, "このグラフは既にダッシュボードに追加されています")
                return redirect("visualization:dashboard_detail", pk=self.object.pk)

            from .models import DashboardChart
            DashboardChart.objects.create(
                dashboard=self.object,
                chart=chart,
                grid_x=0,
                grid_y=0,
                grid_width=grid_width,
                grid_height=grid_height,
                display_order=self.object.charts.count() + 1,
            )

            messages = __import__('django.contrib.messages').contrib.messages
            messages.success(request, "グラフを追加しました")
        except Chart.DoesNotExist:
            messages = __import__('django.contrib.messages').contrib.messages
            messages.error(request, "指定されたグラフが見つかりません")
        except Exception as e:
            logger.error(f"ダッシュボード追加(フォーム)エラー: {e}")
            messages = __import__('django.contrib.messages').contrib.messages
            messages.error(request, f"追加に失敗しました: {str(e)}")

        return redirect("visualization:dashboard_detail", pk=self.object.pk)


@login_required
def analysis_correlation(request, dataset_id):
    """相関分析ページ"""
    dataset = get_object_or_404(Dataset, id=dataset_id, is_active=True)

    if request.method == "POST":
        selected_columns = request.POST.getlist("columns")

        if selected_columns:
            engine = AnalysisEngine()
            result = engine.correlation_analysis(dataset, selected_columns)

            return render(
                request,
                "visualization/analysis_correlation.html",
                {
                    "dataset": dataset,
                    "analysis_result": result,
                    "selected_columns": selected_columns,
                },
            )

    # 数値カラムのみ表示
    numeric_columns = dataset.schema_fields.filter(column_type__in=["INTEGER", "FLOAT"])

    return render(
        request,
        "visualization/analysis_correlation.html",
        {"dataset": dataset, "numeric_columns": numeric_columns},
    )


@login_required
def analysis_time_series(request, dataset_id):
    """時系列分析ページ"""
    dataset = get_object_or_404(Dataset, id=dataset_id, is_active=True)

    if request.method == "POST":
        time_column = request.POST.get("time_column")
        value_columns = request.POST.getlist("value_columns")

        if time_column and value_columns:
            engine = AnalysisEngine()
            result = engine.time_series_analysis(dataset, time_column, value_columns)

            return render(
                request,
                "visualization/analysis_time_series.html",
                {
                    "dataset": dataset,
                    "analysis_result": result,
                    "time_column": time_column,
                    "value_columns": value_columns,
                },
            )

    # 日時カラムと数値カラムを取得
    datetime_columns = dataset.schema_fields.filter(column_type="DATETIME")
    numeric_columns = dataset.schema_fields.filter(column_type__in=["INTEGER", "FLOAT"])

    return render(
        request,
        "visualization/analysis_time_series.html",
        {
            "dataset": dataset,
            "datetime_columns": datetime_columns,
            "numeric_columns": numeric_columns,
        },
    )


# REST API ViewSets
class ChartViewSet(viewsets.ModelViewSet):
    """Chart REST API"""

    serializer_class = ChartSerializer

    def get_queryset(self):
        if self.request.user.is_authenticated:
            return Chart.objects.filter(created_by=self.request.user).order_by(
                "-updated_at"
            )
        return Chart.objects.none()

    def perform_create(self, serializer):
        serializer.save(created_by=self.request.user)

    # 部分更新に対応（PUT/PATCH 両方で partial を許可）
    def update(self, request, *args, **kwargs):  # type: ignore[override]
        partial = request.method.upper() == "PATCH" or request.data.get("partial") in ("1", "true", True)
        instance = self.get_object()
        serializer = self.get_serializer(instance, data=request.data, partial=partial)
        serializer.is_valid(raise_exception=True)
        self.perform_update(serializer)
        return Response({"success": True, "chart": serializer.data})

    def destroy(self, request, *args, **kwargs):  # type: ignore[override]
        instance = self.get_object()
        self.perform_destroy(instance)
        return Response({"success": True})

    @action(detail=True, methods=["post"], url_path="preview")
    def preview_existing(self, request, pk=None):
        """現在の設定を基にプレビューを生成"""
        chart = self.get_object()

        # POSTされたデータを一時的なChartオブジェクトに反映
        tmp_chart = Chart(
            title=request.data.get("title", chart.title),
            chart_type=request.data.get("chart_type", chart.chart_type),
            dataset=chart.dataset,
            created_by=request.user,
            x_axis_column=request.data.get("x_axis")
            or request.data.get("x_axis_column")
            or chart.x_axis_column,
            y_axis_column=request.data.get("y_axis")
            or request.data.get("y_axis_column")
            or chart.y_axis_column,
            z_axis_column=request.data.get("z_axis_column", chart.z_axis_column),
            color_column=request.data.get("color_column", chart.color_column),
            size_column=request.data.get("size_column", chart.size_column),
            color_scheme=request.data.get("color_scheme", chart.color_scheme),
            chart_config=chart.chart_config,
            filters=chart.filters,
        )

        generator = ChartGenerator()
        fig, result = generator.generate_chart(tmp_chart)

        if result["success"]:
            fig_json = json.loads(fig.to_json())
            return Response({"success": True, "traces": fig_json.get("data", [])})
        else:
            return Response(
                {"success": False, "error": result.get("error", "エラーが発生しました")}
            )

    @action(detail=False, methods=["post"], url_path="preview")
    def preview_new(self, request):  # type: ignore[override]
        """新規作成時のプレビュー生成"""
        dataset_id = (
            request.data.get("dataset")
            or request.data.get("dataset_id")
            or request.query_params.get("dataset")
            or request.query_params.get("dataset_id")
        )
        if not dataset_id:
            return Response(
                {"success": False, "error": "データセットが指定されていません"},
                status=status.HTTP_400_BAD_REQUEST,
            )
        try:
            dataset = Dataset.objects.get(id=dataset_id, is_active=True)
        except Dataset.DoesNotExist:
            return Response(
                {"success": False, "error": "データセットが見つかりません"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        tmp_chart = Chart(
            title=request.data.get("title", ""),
            chart_type=request.data.get("chart_type", "line"),
            dataset=dataset,
            created_by=request.user,
            x_axis_column=request.data.get("x_axis_column"),
            y_axis_column=request.data.get("y_axis_column"),
            z_axis_column=request.data.get("z_axis_column", ""),
            color_column=request.data.get("color_column", ""),
            size_column=request.data.get("size_column", ""),
            color_scheme=request.data.get("color_scheme", "viridis"),
            chart_config={},
            filters={},
        )

        generator = ChartGenerator()
        fig, result = generator.generate_chart(tmp_chart)

        if result["success"]:
            fig_json = json.loads(fig.to_json())
            return Response({"success": True, "traces": fig_json.get("data", [])})
        else:
            return Response(
                {"success": False, "error": result.get("error", "エラーが発生しました")}
            )

    @action(detail=True, methods=["get"])
    def render(self, request, pk=None):
        """グラフを描画してJSONで返す"""
        chart = self.get_object()

        try:
            generator = ChartGenerator()
            fig, result = generator.generate_chart(chart)

            if result["success"]:
                return Response(
                    {"chart_data": json.loads(fig.to_json()), "result": result}
                )
            else:
                return Response(
                    {"error": result.get("error", "不明なエラー")},
                    status=status.HTTP_500_INTERNAL_SERVER_ERROR,
                )

        except Exception as e:
            logger.error(f"グラフレンダリングエラー: {e}")
            return Response(
                {"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class DashboardViewSet(viewsets.ModelViewSet):
    """Dashboard REST API"""

    serializer_class = DashboardSerializer

    def get_queryset(self):
        if self.request.user.is_authenticated:
            return Dashboard.objects.filter(created_by=self.request.user).order_by(
                "-updated_at"
            )
        return Dashboard.objects.none()

    def perform_create(self, serializer):
        serializer.save(created_by=self.request.user)

    @action(detail=True, methods=["post"])
    def add_chart(self, request, pk=None):
        """ダッシュボードにグラフを追加"""
        dashboard = self.get_object()
        chart_id = request.data.get("chart_id")
        grid_x = request.data.get("grid_x", 0)
        grid_y = request.data.get("grid_y", 0)
        grid_width = request.data.get("grid_width", 1)
        grid_height = request.data.get("grid_height", 1)

        try:
            chart = Chart.objects.get(id=chart_id, created_by=request.user)

            # 既存の関連を確認
            if dashboard.charts.filter(id=chart_id).exists():
                return Response(
                    {"error": "このグラフは既にダッシュボードに追加されています"},
                    status=status.HTTP_400_BAD_REQUEST,
                )

            # DashboardChartリレーションを作成
            from .models import DashboardChart

            DashboardChart.objects.create(
                dashboard=dashboard,
                chart=chart,
                grid_x=grid_x,
                grid_y=grid_y,
                grid_width=grid_width,
                grid_height=grid_height,
                display_order=dashboard.charts.count() + 1,
            )

            return Response({"message": "グラフが追加されました"})

        except Chart.DoesNotExist:
            return Response(
                {"error": "指定されたグラフが見つかりません"},
                status=status.HTTP_404_NOT_FOUND,
            )
        except Exception as e:
            logger.error(f"グラフ追加エラー: {e}")
            return Response(
                {"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


@login_required
def dashboard_remove_chart(request, pk: int, chart_id: int):
    """ダッシュボードから特定のグラフを外す"""
    dashboard = get_object_or_404(Dashboard, pk=pk)
    try:
        from .models import DashboardChart
        DashboardChart.objects.filter(dashboard=dashboard, chart_id=chart_id).delete()
        messages.success(request, "ダッシュボードからグラフを外しました")
    except Exception as e:
        logger.error(f"ダッシュボードからの除外エラー: {e}")
        messages.error(request, f"除外に失敗しました: {str(e)}")
    return redirect("visualization:dashboard_detail", pk=pk)


@login_required
def dashboard_add_table(request, pk: int):
    """ダッシュボードにデータテーブル（データセット）を追加"""
    dashboard = get_object_or_404(Dashboard, pk=pk)
    try:
        dataset_id = int(request.POST.get("dataset_id"))
        # 検証
        Dataset.objects.get(id=dataset_id, is_active=True)
        cfg = dashboard.layout_config or {}
        if not isinstance(cfg, dict):
            cfg = {}
        tables = cfg.get("tables", [])
        if dataset_id not in tables:
            tables.append(dataset_id)
        cfg["tables"] = tables
        dashboard.layout_config = cfg
        dashboard.save(update_fields=["layout_config", "updated_at"])
        messages.success(request, "データテーブルを追加しました")
    except Dataset.DoesNotExist:
        messages.error(request, "指定されたデータセットが見つかりません")
    except Exception as e:
        logger.error(f"テーブル追加エラー: {e}")
        messages.error(request, f"追加に失敗しました: {str(e)}")
    return redirect("visualization:dashboard_detail", pk=pk)


@login_required
def dashboard_remove_table(request, pk: int, dataset_id: int):
    """ダッシュボードからデータテーブルを削除"""
    dashboard = get_object_or_404(Dashboard, pk=pk)
    try:
        cfg = dashboard.layout_config or {}
        if not isinstance(cfg, dict):
            cfg = {}
        tables = cfg.get("tables", [])
        tables = [i for i in tables if i != int(dataset_id)]
        cfg["tables"] = tables
        dashboard.layout_config = cfg
        dashboard.save(update_fields=["layout_config", "updated_at"])
        messages.success(request, "データテーブルを削除しました")
    except Exception as e:
        logger.error(f"テーブル削除エラー: {e}")
        messages.error(request, f"削除に失敗しました: {str(e)}")
    return redirect("visualization:dashboard_detail", pk=pk)


class AnalysisTemplateViewSet(viewsets.ModelViewSet):
    """AnalysisTemplate REST API"""

    serializer_class = AnalysisTemplateSerializer

    def get_queryset(self):
        # 公開テンプレートまたは自分が作成したテンプレート
        if self.request.user.is_authenticated:
            return AnalysisTemplate.objects.filter(
                models.Q(is_public=True) | models.Q(created_by=self.request.user)
            ).order_by("-usage_count", "-created_at")
        else:
            return AnalysisTemplate.objects.filter(is_public=True)

    def perform_create(self, serializer):
        serializer.save(created_by=self.request.user)

    @action(detail=True, methods=["post"])
    def apply(self, request, pk=None):
        """テンプレートをデータセットに適用"""
        template = self.get_object()
        dataset_id = request.data.get("dataset_id")

        try:
            dataset = Dataset.objects.get(id=dataset_id, is_active=True)

            # テンプレートの使用回数を増加
            template.usage_count += 1
            template.save()

            # TODO: テンプレート適用ロジックを実装
            return Response(
                {
                    "message": "テンプレートが適用されました",
                    "template_name": template.name,
                }
            )

        except Dataset.DoesNotExist:
            return Response(
                {"error": "指定されたデータセットが見つかりません"},
                status=status.HTTP_404_NOT_FOUND,
            )
