from rest_framework import serializers
from .models import Chart, Dashboard, DashboardChart, AnalysisTemplate, DataComparison, UserPreference
from ingest.serializers import DatasetSerializer


class ChartSerializer(serializers.ModelSerializer):
    """グラフシリアライザー"""
    dataset_name = serializers.CharField(source='dataset.name', read_only=True)
    created_by_username = serializers.CharField(source='created_by.username', read_only=True)

    class Meta:
        model = Chart
        fields = [
            'id', 'title', 'description', 'chart_type', 'dataset', 'dataset_name',
            'created_by', 'created_by_username', 'x_axis_column',
            'y_axis_column', 'z_axis_column', 'color_column',
            'size_column', 'chart_config', 'color_scheme',
            'filters', 'created_at', 'updated_at', 'is_public'
        ]
        read_only_fields = ['created_by', 'created_at', 'updated_at']

    def validate_chart_config(self, value):
        """グラフ設定の妥当性チェック"""
        if not isinstance(value, dict):
            raise serializers.ValidationError("グラフ設定は辞書形式である必要があります。")
        return value

    def validate_filters(self, value):
        """フィルター設定の妥当性チェック"""
        if not isinstance(value, dict):
            raise serializers.ValidationError("フィルター設定は辞書形式である必要があります。")
        return value


class DashboardChartSerializer(serializers.ModelSerializer):
    """ダッシュボードグラフシリアライザー"""
    chart = ChartSerializer(read_only=True)

    class Meta:
        model = DashboardChart
        fields = [
            'id', 'chart', 'grid_x', 'grid_y', 'grid_width',
            'grid_height', 'display_order', 'is_visible'
        ]


class DashboardSerializer(serializers.ModelSerializer):
    """ダッシュボードシリアライザー"""
    charts = DashboardChartSerializer(source='dashboardchart_set', many=True, read_only=True)
    created_by_username = serializers.CharField(source='created_by.username', read_only=True)
    chart_count = serializers.SerializerMethodField()

    class Meta:
        model = Dashboard
        fields = [
            'id', 'name', 'description', 'created_by', 'created_by_username',
            'layout_config', 'auto_refresh_interval', 'created_at',
            'updated_at', 'is_public', 'charts', 'chart_count'
        ]
        read_only_fields = ['created_by', 'created_at', 'updated_at']

    def get_chart_count(self, obj):
        """ダッシュボード内のグラフ数を取得"""
        return obj.charts.count()

    def validate_layout_config(self, value):
        """レイアウト設定の妥当性チェック"""
        if not isinstance(value, dict):
            raise serializers.ValidationError("レイアウト設定は辞書形式である必要があります。")
        return value


class AnalysisTemplateSerializer(serializers.ModelSerializer):
    """分析テンプレートシリアライザー"""
    created_by_username = serializers.CharField(source='created_by.username', read_only=True)

    class Meta:
        model = AnalysisTemplate
        fields = [
            'id', 'name', 'template_type', 'description',
            'created_by', 'created_by_username', 'required_columns',
            'optional_columns', 'analysis_config', 'output_charts',
            'created_at', 'usage_count', 'is_public'
        ]
        read_only_fields = ['created_by', 'created_at', 'usage_count']

    def validate_required_columns(self, value):
        """必須カラム設定の妥当性チェック"""
        if not isinstance(value, (list, dict)):
            raise serializers.ValidationError("必須カラム設定はリストまたは辞書形式である必要があります。")
        return value

    def validate_optional_columns(self, value):
        """オプションカラム設定の妥当性チェック"""
        if not isinstance(value, list):
            raise serializers.ValidationError("オプションカラム設定はリスト形式である必要があります。")
        return value

    def validate_analysis_config(self, value):
        """分析設定の妥当性チェック"""
        if not isinstance(value, dict):
            raise serializers.ValidationError("分析設定は辞書形式である必要があります。")
        return value

    def validate_output_charts(self, value):
        """出力グラフ設定の妥当性チェック"""
        if not isinstance(value, dict):
            raise serializers.ValidationError("出力グラフ設定は辞書形式である必要があります。")
        return value


class DataComparisonSerializer(serializers.ModelSerializer):
    """データ比較シリアライザー"""
    datasets = DatasetSerializer(many=True, read_only=True)
    created_by_username = serializers.CharField(source='created_by.username', read_only=True)

    class Meta:
        model = DataComparison
        fields = [
            'id', 'name', 'description', 'created_by', 'created_by_username',
            'datasets', 'comparison_columns', 'comparison_config',
            'comparison_results', 'created_at', 'updated_at'
        ]
        read_only_fields = ['created_by', 'created_at', 'updated_at', 'comparison_results']

    def validate_comparison_columns(self, value):
        """比較対象カラム設定の妥当性チェック"""
        if not isinstance(value, dict):
            raise serializers.ValidationError("比較対象カラム設定は辞書形式である必要があります。")
        return value

    def validate_comparison_config(self, value):
        """比較設定の妥当性チェック"""
        if not isinstance(value, dict):
            raise serializers.ValidationError("比較設定は辞書形式である必要があります。")
        return value


class UserPreferenceSerializer(serializers.ModelSerializer):
    """ユーザー設定シリアライザー"""

    class Meta:
        model = UserPreference
        fields = [
            'id', 'user', 'default_chart_type', 'default_color_scheme',
            'items_per_page', 'dashboard_preferences', 'email_notifications',
            'data_quality_alerts', 'custom_theme'
        ]
        read_only_fields = ['user']

    def validate_dashboard_preferences(self, value):
        """ダッシュボード設定の妥当性チェック"""
        if not isinstance(value, dict):
            raise serializers.ValidationError("ダッシュボード設定は辞書形式である必要があります。")
        return value

    def validate_custom_theme(self, value):
        """カスタムテーマ設定の妥当性チェック"""
        if not isinstance(value, dict):
            raise serializers.ValidationError("カスタムテーマ設定は辞書形式である必要があります。")
        return value

    def validate_items_per_page(self, value):
        """ページあたり項目数の妥当性チェック"""
        if value < 5 or value > 100:
            raise serializers.ValidationError("ページあたり項目数は5から100の間である必要があります。")
        return value


class ChartCreateSerializer(serializers.ModelSerializer):
    """グラフ作成用シリアライザー"""

    class Meta:
        model = Chart
        fields = [
            'title', 'chart_type', 'dataset', 'x_axis_column',
            'y_axis_column', 'z_axis_column', 'color_column',
            'size_column', 'color_scheme', 'chart_config', 'filters'
        ]

    def validate(self, data):
        """グラフ作成時の全体的なバリデーション"""
        chart_type = data.get('chart_type')

        # 必須カラムのチェック
        if chart_type in ['line', 'bar', 'scatter', 'box', 'violin']:
            if not data.get('x_axis_column') or not data.get('y_axis_column'):
                raise serializers.ValidationError(
                    f"{chart_type}グラフにはX軸とY軸のカラムが必要です。"
                )

        if chart_type == '3d_scatter':
            if not data.get('z_axis_column'):
                raise serializers.ValidationError("3D散布図にはZ軸カラムが必要です。")

        if chart_type in ['histogram', 'pie']:
            if not data.get('x_axis_column'):
                raise serializers.ValidationError(
                    f"{chart_type}グラフにはX軸カラムが必要です。"
                )

        return data


class QuickAnalysisRequestSerializer(serializers.Serializer):
    """クイック分析リクエスト用シリアライザー"""
    dataset_id = serializers.IntegerField()
    analysis_type = serializers.ChoiceField(choices=[
        ('correlation', '相関分析'),
        ('time_series', '時系列分析'),
        ('statistical', '統計分析'),
        ('distribution', '分布分析'),
    ])
    columns = serializers.ListField(
        child=serializers.CharField(),
        required=False,
        allow_empty=True
    )
    parameters = serializers.DictField(required=False, default=dict)

    def validate_dataset_id(self, value):
        """データセットの存在確認"""
        from ingest.models import Dataset
        try:
            Dataset.objects.get(id=value, is_active=True)
            return value
        except Dataset.DoesNotExist:
            raise serializers.ValidationError("指定されたデータセットが見つかりません。")
