from django.db import models
from django.contrib.auth.models import User
from django.core.validators import FileExtensionValidator
import json


class Dataset(models.Model):
    """
    データセットのメタデータを管理するモデル
    車両データの計測セッションやCSVファイルの情報を保持
    """
    name = models.CharField(max_length=255, verbose_name="データセット名")
    description = models.TextField(blank=True, verbose_name="説明")
    created_by = models.ForeignKey(User, on_delete=models.CASCADE, verbose_name="作成者")
    created_at = models.DateTimeField(auto_now_add=True, verbose_name="作成日時")
    updated_at = models.DateTimeField(auto_now=True, verbose_name="更新日時")
    
    # 車両関連メタデータ
    vehicle_model = models.CharField(max_length=100, blank=True, verbose_name="車両モデル")
    measurement_date = models.DateTimeField(null=True, blank=True, verbose_name="計測日時")
    measurement_location = models.CharField(max_length=255, blank=True, verbose_name="計測場所")
    
    # データ品質
    total_rows = models.IntegerField(default=0, verbose_name="総レコード数")
    is_active = models.BooleanField(default=True, verbose_name="アクティブ")
    
    class Meta:
        verbose_name = "データセット"
        verbose_name_plural = "データセット"
        ordering = ['-created_at']
    
    def __str__(self):
        return f"{self.name} ({self.total_rows} rows)"


class DataSchema(models.Model):
    """
    動的スキーマ定義
    CSVの構造が異なっても柔軟に対応するためのスキーマ管理
    """
    COLUMN_TYPES = [
        ('INTEGER', '整数'),
        ('FLOAT', '小数'),
        ('STRING', '文字列'),
        ('DATETIME', '日時'),
        ('BOOLEAN', 'ブール値'),
    ]
    
    dataset = models.ForeignKey(Dataset, on_delete=models.CASCADE, related_name='schema_fields')
    column_name = models.CharField(max_length=255, verbose_name="カラム名")
    column_type = models.CharField(max_length=20, choices=COLUMN_TYPES, verbose_name="データ型")
    is_nullable = models.BooleanField(default=True, verbose_name="NULL許可")
    default_value = models.CharField(max_length=255, blank=True, verbose_name="デフォルト値")
    column_order = models.IntegerField(verbose_name="カラム順序")
    
    # 統計情報
    min_value = models.FloatField(null=True, blank=True, verbose_name="最小値")
    max_value = models.FloatField(null=True, blank=True, verbose_name="最大値")
    unique_count = models.IntegerField(null=True, blank=True, verbose_name="ユニーク数")
    
    class Meta:
        verbose_name = "データスキーマ"
        verbose_name_plural = "データスキーマ"
        ordering = ['dataset', 'column_order']
        unique_together = ['dataset', 'column_name']
    
    def __str__(self):
        return f"{self.dataset.name}.{self.column_name} ({self.column_type})"


class RawDataFile(models.Model):
    """
    アップロードされた生ファイルの管理
    """
    dataset = models.ForeignKey(Dataset, on_delete=models.CASCADE, related_name='raw_files')
    original_filename = models.CharField(max_length=255, verbose_name="元ファイル名")
    file = models.FileField(
        upload_to='raw_data/%Y/%m/%d/',
        validators=[FileExtensionValidator(allowed_extensions=['csv', 'xlsx', 'xls'])],
        verbose_name="ファイル"
    )
    file_size = models.BigIntegerField(verbose_name="ファイルサイズ")
    encoding = models.CharField(max_length=20, default='utf-8', verbose_name="文字エンコーディング")
    delimiter = models.CharField(max_length=5, default=',', verbose_name="区切り文字")
    
    uploaded_at = models.DateTimeField(auto_now_add=True, verbose_name="アップロード日時")
    processed = models.BooleanField(default=False, verbose_name="処理済み")
    processing_error = models.TextField(blank=True, verbose_name="処理エラー")
    
    class Meta:
        verbose_name = "生データファイル"
        verbose_name_plural = "生データファイル"
        ordering = ['-uploaded_at']
    
    def __str__(self):
        return f"{self.original_filename} ({self.dataset.name})"


class DataRecord(models.Model):
    """
    実際のデータレコードを格納
    JSONフィールドを使用して柔軟なスキーマに対応
    """
    dataset = models.ForeignKey(Dataset, on_delete=models.CASCADE, related_name='records')
    row_number = models.IntegerField(verbose_name="行番号")
    data = models.JSONField(verbose_name="データ")  # 動的なデータ構造
    
    # メタデータ
    imported_at = models.DateTimeField(auto_now_add=True, verbose_name="インポート日時")
    data_hash = models.CharField(max_length=64, verbose_name="データハッシュ")  # 重複検出用
    
    class Meta:
        verbose_name = "データレコード"
        verbose_name_plural = "データレコード"
        ordering = ['dataset', 'row_number']
        unique_together = ['dataset', 'row_number']
        indexes = [
            models.Index(fields=['dataset', 'row_number']),
            models.Index(fields=['data_hash']),
        ]
    
    def __str__(self):
        return f"{self.dataset.name} - Row {self.row_number}"


class DataValidationRule(models.Model):
    """
    データ品質管理のためのバリデーションルール
    """
    RULE_TYPES = [
        ('RANGE', '範囲チェック'),
        ('PATTERN', 'パターンマッチ'),
        ('NOT_NULL', 'NULL許可しない'),
        ('UNIQUE', 'ユニーク制約'),
        ('CUSTOM', 'カスタムルール'),
    ]
    
    dataset = models.ForeignKey(Dataset, on_delete=models.CASCADE, related_name='validation_rules')
    column_name = models.CharField(max_length=255, verbose_name="対象カラム")
    rule_type = models.CharField(max_length=20, choices=RULE_TYPES, verbose_name="ルール種類")
    rule_config = models.JSONField(verbose_name="ルール設定")  # 柔軟なルール設定
    is_active = models.BooleanField(default=True, verbose_name="アクティブ")
    
    created_at = models.DateTimeField(auto_now_add=True, verbose_name="作成日時")
    
    class Meta:
        verbose_name = "データバリデーションルール"
        verbose_name_plural = "データバリデーションルール"
    
    def __str__(self):
        return f"{self.dataset.name}.{self.column_name} - {self.rule_type}"


class DataQualityReport(models.Model):
    """
    データ品質レポート
    """
    dataset = models.ForeignKey(Dataset, on_delete=models.CASCADE, related_name='quality_reports')
    report_date = models.DateTimeField(auto_now_add=True, verbose_name="レポート日時")
    
    # 品質指標
    total_records = models.IntegerField(verbose_name="総レコード数")
    valid_records = models.IntegerField(verbose_name="有効レコード数")
    invalid_records = models.IntegerField(verbose_name="無効レコード数")
    duplicate_records = models.IntegerField(verbose_name="重複レコード数")
    
    # 詳細レポート（JSON形式）
    quality_details = models.JSONField(verbose_name="品質詳細")
    
    class Meta:
        verbose_name = "データ品質レポート"
        verbose_name_plural = "データ品質レポート"
        ordering = ['-report_date']
    
    def __str__(self):
        quality_percentage = (self.valid_records / self.total_records * 100) if self.total_records > 0 else 0
        return f"{self.dataset.name} - 品質: {quality_percentage:.1f}%"
