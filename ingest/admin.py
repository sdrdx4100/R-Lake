from django.contrib import admin
from .models import Dataset, DataSchema, RawDataFile, DataRecord, DataQualityReport, DataValidationRule


@admin.register(Dataset)
class DatasetAdmin(admin.ModelAdmin):
    list_display = ['name', 'created_by', 'total_rows', 'vehicle_model', 'created_at', 'is_active']
    list_filter = ['is_active', 'created_at', 'vehicle_model']
    search_fields = ['name', 'description', 'vehicle_model']
    readonly_fields = ['created_at', 'updated_at', 'total_rows']
    
    fieldsets = (
        ('基本情報', {
            'fields': ('name', 'description', 'created_by', 'is_active')
        }),
        ('車両情報', {
            'fields': ('vehicle_model', 'measurement_date', 'measurement_location')
        }),
        ('統計情報', {
            'fields': ('total_rows', 'created_at', 'updated_at'),
            'classes': ('collapse',)
        }),
    )


@admin.register(DataSchema)
class DataSchemaAdmin(admin.ModelAdmin):
    list_display = ['dataset', 'column_name', 'column_type', 'column_order', 'unique_count']
    list_filter = ['column_type', 'is_nullable']
    search_fields = ['dataset__name', 'column_name']
    ordering = ['dataset', 'column_order']


@admin.register(RawDataFile)
class RawDataFileAdmin(admin.ModelAdmin):
    list_display = ['original_filename', 'dataset', 'file_size', 'processed', 'uploaded_at']
    list_filter = ['processed', 'uploaded_at', 'encoding']
    search_fields = ['original_filename', 'dataset__name']
    readonly_fields = ['uploaded_at', 'file_size']


@admin.register(DataQualityReport)
class DataQualityReportAdmin(admin.ModelAdmin):
    list_display = ['dataset', 'report_date', 'total_records', 'valid_records', 'quality_percentage']
    list_filter = ['report_date']
    search_fields = ['dataset__name']
    readonly_fields = ['report_date']
    
    def quality_percentage(self, obj):
        if obj.total_records > 0:
            return f"{(obj.valid_records / obj.total_records * 100):.1f}%"
        return "0%"
    quality_percentage.short_description = "品質スコア"


@admin.register(DataValidationRule)
class DataValidationRuleAdmin(admin.ModelAdmin):
    list_display = ['dataset', 'column_name', 'rule_type', 'is_active', 'created_at']
    list_filter = ['rule_type', 'is_active', 'created_at']
    search_fields = ['dataset__name', 'column_name']


@admin.register(DataRecord)
class DataRecordAdmin(admin.ModelAdmin):
    list_display = ['dataset', 'row_number', 'imported_at']
    list_filter = ['imported_at', 'dataset']
    search_fields = ['dataset__name']
    readonly_fields = ['imported_at', 'data_hash']
    
    # 大量のデータを扱うため、詳細表示を制限
    def has_change_permission(self, request, obj=None):
        return False  # 変更を無効化
    
    def has_delete_permission(self, request, obj=None):
        return request.user.is_superuser  # スーパーユーザーのみ削除可能
