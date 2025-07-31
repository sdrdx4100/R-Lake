from django.contrib import admin
from .models import Chart, Dashboard, DashboardChart, AnalysisTemplate, DataComparison, UserPreference


@admin.register(Chart)
class ChartAdmin(admin.ModelAdmin):
    list_display = ['title', 'chart_type', 'dataset', 'created_by', 'created_at', 'is_public']
    list_filter = ['chart_type', 'is_public', 'created_at', 'color_scheme']
    search_fields = ['title', 'dataset__name', 'created_by__username']
    readonly_fields = ['created_at', 'updated_at']
    
    fieldsets = (
        ('基本情報', {
            'fields': ('title', 'chart_type', 'dataset', 'created_by', 'is_public')
        }),
        ('軸設定', {
            'fields': ('x_axis_column', 'y_axis_column', 'z_axis_column')
        }),
        ('スタイル設定', {
            'fields': ('color_column', 'size_column', 'color_scheme')
        }),
        ('詳細設定', {
            'fields': ('chart_config', 'filters'),
            'classes': ('collapse',)
        }),
        ('メタデータ', {
            'fields': ('created_at', 'updated_at'),
            'classes': ('collapse',)
        }),
    )


class DashboardChartInline(admin.TabularInline):
    model = DashboardChart
    extra = 0
    fields = ['chart', 'grid_x', 'grid_y', 'grid_width', 'grid_height', 'display_order', 'is_visible']


@admin.register(Dashboard)
class DashboardAdmin(admin.ModelAdmin):
    list_display = ['name', 'created_by', 'created_at', 'is_public', 'chart_count']
    list_filter = ['is_public', 'created_at']
    search_fields = ['name', 'description', 'created_by__username']
    readonly_fields = ['created_at', 'updated_at']
    inlines = [DashboardChartInline]
    
    def chart_count(self, obj):
        return obj.charts.count()
    chart_count.short_description = "グラフ数"


@admin.register(AnalysisTemplate)
class AnalysisTemplateAdmin(admin.ModelAdmin):
    list_display = ['name', 'template_type', 'created_by', 'usage_count', 'is_public', 'created_at']
    list_filter = ['template_type', 'is_public', 'created_at']
    search_fields = ['name', 'description', 'created_by__username']
    readonly_fields = ['created_at', 'usage_count']
    
    fieldsets = (
        ('基本情報', {
            'fields': ('name', 'template_type', 'description', 'created_by', 'is_public')
        }),
        ('カラム要件', {
            'fields': ('required_columns', 'optional_columns')
        }),
        ('設定', {
            'fields': ('analysis_config', 'output_charts')
        }),
        ('統計', {
            'fields': ('usage_count', 'created_at'),
            'classes': ('collapse',)
        }),
    )


@admin.register(DataComparison)
class DataComparisonAdmin(admin.ModelAdmin):
    list_display = ['name', 'created_by', 'created_at', 'dataset_count']
    list_filter = ['created_at']
    search_fields = ['name', 'description', 'created_by__username']
    readonly_fields = ['created_at', 'updated_at']
    filter_horizontal = ['datasets']
    
    def dataset_count(self, obj):
        return obj.datasets.count()
    dataset_count.short_description = "データセット数"


@admin.register(UserPreference)
class UserPreferenceAdmin(admin.ModelAdmin):
    list_display = ['user', 'default_chart_type', 'default_color_scheme', 'items_per_page']
    list_filter = ['default_chart_type', 'default_color_scheme', 'email_notifications']
    search_fields = ['user__username']
