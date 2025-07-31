from django.db import models
from django.contrib.auth.models import User
from ingest.models import Dataset
import json


class Chart(models.Model):
    """
    動的に生成されるグラフの設定を管理
    """
    CHART_TYPES = [
        ('line', '線グラフ'),
        ('bar', '棒グラフ'),
        ('scatter', '散布図'),
        ('histogram', 'ヒストグラム'),
        ('box', 'ボックスプロット'),
        ('heatmap', 'ヒートマップ'),
        ('pie', '円グラフ'),
        ('area', 'エリアグラフ'),
        ('violin', 'バイオリンプロット'),
        ('3d_scatter', '3D散布図'),
    ]
    
    title = models.CharField(max_length=255, verbose_name="グラフタイトル")
    description = models.TextField(blank=True, verbose_name="説明")
    chart_type = models.CharField(max_length=20, choices=CHART_TYPES, verbose_name="グラフ種類")
    dataset = models.ForeignKey(Dataset, on_delete=models.CASCADE, related_name='charts')
    created_by = models.ForeignKey(User, on_delete=models.CASCADE, verbose_name="作成者")
    
    # グラフ設定（JSON形式で柔軟な設定を保存）
    x_axis_column = models.CharField(max_length=255, verbose_name="X軸カラム")
    y_axis_column = models.CharField(max_length=255, verbose_name="Y軸カラム")
    z_axis_column = models.CharField(max_length=255, blank=True, verbose_name="Z軸カラム（3D用）")
    color_column = models.CharField(max_length=255, blank=True, verbose_name="色分けカラム")
    size_column = models.CharField(max_length=255, blank=True, verbose_name="サイズカラム")
    
    # スタイリング設定
    chart_config = models.JSONField(default=dict, verbose_name="グラフ設定")
    color_scheme = models.CharField(max_length=50, default='viridis', verbose_name="カラースキーム")
    
    # フィルタリング
    filters = models.JSONField(default=dict, verbose_name="データフィルター")
    
    # メタデータ
    created_at = models.DateTimeField(auto_now_add=True, verbose_name="作成日時")
    updated_at = models.DateTimeField(auto_now=True, verbose_name="更新日時")
    is_public = models.BooleanField(default=False, verbose_name="公開設定")
    
    class Meta:
        verbose_name = "グラフ"
        verbose_name_plural = "グラフ"
        ordering = ['-updated_at']
    
    def __str__(self):
        return f"{self.title} ({self.chart_type})"


class Dashboard(models.Model):
    """
    複数のグラフをまとめたダッシュボード
    """
    name = models.CharField(max_length=255, verbose_name="ダッシュボード名")
    description = models.TextField(blank=True, verbose_name="説明")
    created_by = models.ForeignKey(User, on_delete=models.CASCADE, verbose_name="作成者")
    
    # レイアウト設定
    layout_config = models.JSONField(default=dict, verbose_name="レイアウト設定")
    charts = models.ManyToManyField(Chart, through='DashboardChart', verbose_name="含まれるグラフ")
    
    # 自動更新設定
    auto_refresh_interval = models.IntegerField(null=True, blank=True, verbose_name="自動更新間隔（秒）")
    
    # メタデータ
    created_at = models.DateTimeField(auto_now_add=True, verbose_name="作成日時")
    updated_at = models.DateTimeField(auto_now=True, verbose_name="更新日時")
    is_public = models.BooleanField(default=False, verbose_name="公開設定")
    
    class Meta:
        verbose_name = "ダッシュボード"
        verbose_name_plural = "ダッシュボード"
        ordering = ['-updated_at']
    
    def __str__(self):
        return self.name


class DashboardChart(models.Model):
    """
    ダッシュボード内でのグラフの配置情報
    """
    dashboard = models.ForeignKey(Dashboard, on_delete=models.CASCADE)
    chart = models.ForeignKey(Chart, on_delete=models.CASCADE)
    
    # グリッドレイアウト
    grid_x = models.IntegerField(verbose_name="X座標")
    grid_y = models.IntegerField(verbose_name="Y座標")
    grid_width = models.IntegerField(default=1, verbose_name="幅")
    grid_height = models.IntegerField(default=1, verbose_name="高さ")
    
    # 表示設定
    display_order = models.IntegerField(verbose_name="表示順序")
    is_visible = models.BooleanField(default=True, verbose_name="表示/非表示")
    
    class Meta:
        verbose_name = "ダッシュボードグラフ"
        verbose_name_plural = "ダッシュボードグラフ"
        ordering = ['dashboard', 'display_order']
        unique_together = ['dashboard', 'chart']


class AnalysisTemplate(models.Model):
    """
    再利用可能な分析テンプレート
    """
    TEMPLATE_TYPES = [
        ('correlation', '相関分析'),
        ('time_series', '時系列分析'),
        ('statistical', '統計分析'),
        ('ml_exploration', '機械学習探索'),
        ('vehicle_performance', '車両性能分析'),
        ('custom', 'カスタム分析'),
    ]
    
    name = models.CharField(max_length=255, verbose_name="テンプレート名")
    template_type = models.CharField(max_length=30, choices=TEMPLATE_TYPES, verbose_name="テンプレート種類")
    description = models.TextField(verbose_name="説明")
    created_by = models.ForeignKey(User, on_delete=models.CASCADE, verbose_name="作成者")
    
    # 必要なカラム要件
    required_columns = models.JSONField(verbose_name="必須カラム")
    optional_columns = models.JSONField(default=list, verbose_name="オプションカラム")
    
    # 分析設定
    analysis_config = models.JSONField(verbose_name="分析設定")
    output_charts = models.JSONField(verbose_name="出力グラフ設定")
    
    # メタデータ
    created_at = models.DateTimeField(auto_now_add=True, verbose_name="作成日時")
    usage_count = models.IntegerField(default=0, verbose_name="使用回数")
    is_public = models.BooleanField(default=False, verbose_name="公開設定")
    
    class Meta:
        verbose_name = "分析テンプレート"
        verbose_name_plural = "分析テンプレート"
        ordering = ['-usage_count', '-created_at']
    
    def __str__(self):
        return f"{self.name} ({self.template_type})"


class DataComparison(models.Model):
    """
    データセット間の比較分析
    """
    name = models.CharField(max_length=255, verbose_name="比較分析名")
    description = models.TextField(blank=True, verbose_name="説明")
    created_by = models.ForeignKey(User, on_delete=models.CASCADE, verbose_name="作成者")
    
    # 比較対象
    datasets = models.ManyToManyField(Dataset, verbose_name="比較データセット")
    comparison_columns = models.JSONField(verbose_name="比較対象カラム")
    
    # 比較設定
    comparison_config = models.JSONField(default=dict, verbose_name="比較設定")
    
    # 結果
    comparison_results = models.JSONField(null=True, blank=True, verbose_name="比較結果")
    
    # メタデータ
    created_at = models.DateTimeField(auto_now_add=True, verbose_name="作成日時")
    updated_at = models.DateTimeField(auto_now=True, verbose_name="更新日時")
    
    class Meta:
        verbose_name = "データ比較"
        verbose_name_plural = "データ比較"
        ordering = ['-updated_at']
    
    def __str__(self):
        return self.name


class UserPreference(models.Model):
    """
    ユーザーの表示設定やカスタマイズ設定
    """
    user = models.OneToOneField(User, on_delete=models.CASCADE, verbose_name="ユーザー")
    
    # UI設定
    default_chart_type = models.CharField(max_length=20, default='line', verbose_name="デフォルトグラフ種類")
    default_color_scheme = models.CharField(max_length=50, default='viridis', verbose_name="デフォルトカラースキーム")
    items_per_page = models.IntegerField(default=20, verbose_name="ページあたり項目数")
    
    # ダッシュボード設定
    dashboard_preferences = models.JSONField(default=dict, verbose_name="ダッシュボード設定")
    
    # 通知設定
    email_notifications = models.BooleanField(default=True, verbose_name="メール通知")
    data_quality_alerts = models.BooleanField(default=True, verbose_name="データ品質アラート")
    
    # カスタムテーマ
    custom_theme = models.JSONField(default=dict, verbose_name="カスタムテーマ")
    
    class Meta:
        verbose_name = "ユーザー設定"
        verbose_name_plural = "ユーザー設定"
    
    def __str__(self):
        return f"{self.user.username}の設定"
