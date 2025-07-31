from rest_framework import serializers
from .models import Dataset, DataSchema, RawDataFile, DataRecord, DataQualityReport, DataValidationRule


class DataSchemaSerializer(serializers.ModelSerializer):
    """データスキーマシリアライザー"""
    
    class Meta:
        model = DataSchema
        fields = [
            'id', 'column_name', 'column_type', 'is_nullable',
            'default_value', 'column_order', 'min_value', 'max_value',
            'unique_count'
        ]


class RawDataFileSerializer(serializers.ModelSerializer):
    """生データファイルシリアライザー"""
    
    class Meta:
        model = RawDataFile
        fields = [
            'id', 'original_filename', 'file', 'file_size',
            'encoding', 'delimiter', 'uploaded_at', 'processed',
            'processing_error'
        ]
        read_only_fields = ['file_size', 'uploaded_at', 'processed', 'processing_error']


class DataRecordSerializer(serializers.ModelSerializer):
    """データレコードシリアライザー"""
    
    class Meta:
        model = DataRecord
        fields = ['id', 'row_number', 'data', 'imported_at', 'data_hash']
        read_only_fields = ['imported_at', 'data_hash']


class DataQualityReportSerializer(serializers.ModelSerializer):
    """データ品質レポートシリアライザー"""
    quality_score = serializers.SerializerMethodField()
    
    class Meta:
        model = DataQualityReport
        fields = [
            'id', 'report_date', 'total_records', 'valid_records',
            'invalid_records', 'duplicate_records', 'quality_details',
            'quality_score'
        ]
    
    def get_quality_score(self, obj):
        """品質スコアを計算"""
        if obj.total_records > 0:
            return (obj.valid_records / obj.total_records) * 100
        return 0


class DataValidationRuleSerializer(serializers.ModelSerializer):
    """データバリデーションルールシリアライザー"""
    
    class Meta:
        model = DataValidationRule
        fields = [
            'id', 'column_name', 'rule_type', 'rule_config',
            'is_active', 'created_at'
        ]


class DatasetSerializer(serializers.ModelSerializer):
    """データセットシリアライザー"""
    schema_fields = DataSchemaSerializer(many=True, read_only=True)
    latest_quality_report = serializers.SerializerMethodField()
    created_by_username = serializers.CharField(source='created_by.username', read_only=True)
    
    class Meta:
        model = Dataset
        fields = [
            'id', 'name', 'description', 'created_by', 'created_by_username',
            'created_at', 'updated_at', 'vehicle_model', 'measurement_date',
            'measurement_location', 'total_rows', 'is_active',
            'schema_fields', 'latest_quality_report'
        ]
        read_only_fields = ['created_by', 'created_at', 'updated_at', 'total_rows']
    
    def get_latest_quality_report(self, obj):
        """最新の品質レポートを取得"""
        latest_report = obj.quality_reports.order_by('-report_date').first()
        if latest_report:
            return DataQualityReportSerializer(latest_report).data
        return None


class DatasetDetailSerializer(DatasetSerializer):
    """データセット詳細シリアライザー（より多くの情報を含む）"""
    raw_files = RawDataFileSerializer(many=True, read_only=True)
    validation_rules = DataValidationRuleSerializer(many=True, read_only=True)
    record_count = serializers.SerializerMethodField()
    
    class Meta(DatasetSerializer.Meta):
        fields = DatasetSerializer.Meta.fields + [
            'raw_files', 'validation_rules', 'record_count'
        ]
    
    def get_record_count(self, obj):
        """実際のレコード数を取得"""
        return obj.records.count()


class DatasetCreateSerializer(serializers.ModelSerializer):
    """データセット作成用シリアライザー"""
    
    class Meta:
        model = Dataset
        fields = [
            'name', 'description', 'vehicle_model',
            'measurement_date', 'measurement_location'
        ]
    
    def validate_name(self, value):
        """データセット名の重複チェック"""
        if Dataset.objects.filter(name=value, is_active=True).exists():
            raise serializers.ValidationError("同じ名前のデータセットが既に存在します。")
        return value


class BulkDataUploadSerializer(serializers.Serializer):
    """バルクデータアップロード用シリアライザー"""
    dataset_id = serializers.IntegerField()
    data_records = serializers.ListField(
        child=serializers.DictField(),
        min_length=1,
        max_length=10000  # 一度に最大10000レコード
    )
    
    def validate_dataset_id(self, value):
        """データセットの存在確認"""
        try:
            dataset = Dataset.objects.get(id=value, is_active=True)
            return value
        except Dataset.DoesNotExist:
            raise serializers.ValidationError("指定されたデータセットが見つかりません。")
    
    def validate_data_records(self, value):
        """データレコードの妥当性チェック"""
        if not value:
            raise serializers.ValidationError("データレコードが空です。")
        
        # すべてのレコードが同じキー構造を持つかチェック
        first_record_keys = set(value[0].keys()) if value else set()
        for i, record in enumerate(value[1:], 1):
            record_keys = set(record.keys())
            if record_keys != first_record_keys:
                raise serializers.ValidationError(
                    f"レコード {i+1} のキー構造が最初のレコードと異なります。"
                )
        
        return value
