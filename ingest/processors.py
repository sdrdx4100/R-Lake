import pandas as pd
import numpy as np
import hashlib
import json
from typing import Dict, List, Tuple, Any, Optional
from django.core.exceptions import ValidationError
from django.db import transaction
from ingest.models import Dataset, DataSchema, RawDataFile, DataRecord, DataQualityReport
import logging
import chardet

logger = logging.getLogger(__name__)


class CSVProcessor:
    """
    動的スキーマ対応のCSVプロセッサー
    """
    
    def __init__(self):
        self.type_mapping = {
            'int64': 'INTEGER',
            'float64': 'FLOAT',
            'object': 'STRING',
            'datetime64[ns]': 'DATETIME',
            'bool': 'BOOLEAN',
        }
    
    def detect_encoding(self, file_path: str) -> str:
        """ファイルの文字エンコーディングを検出"""
        try:
            with open(file_path, 'rb') as f:
                raw_data = f.read(10000)  # 最初の10KB を読み取り
                result = chardet.detect(raw_data)
                return result['encoding'] if result['confidence'] > 0.7 else 'utf-8'
        except Exception as e:
            logger.warning(f"エンコーディング検出エラー: {e}")
            return 'utf-8'
    
    def detect_delimiter(self, file_path: str, encoding: str = 'utf-8') -> str:
        """CSVの区切り文字を検出"""
        try:
            with open(file_path, 'r', encoding=encoding) as f:
                first_line = f.readline()
                
            # 一般的な区切り文字を試す
            delimiters = [',', ';', '\t', '|']
            delimiter_counts = {}
            
            for delimiter in delimiters:
                delimiter_counts[delimiter] = first_line.count(delimiter)
            
            # 最も多く使用されている区切り文字を返す
            best_delimiter = max(delimiter_counts, key=delimiter_counts.get)
            return best_delimiter if delimiter_counts[best_delimiter] > 0 else ','
            
        except Exception as e:
            logger.warning(f"区切り文字検出エラー: {e}")
            return ','
    
    def infer_column_types(self, df: pd.DataFrame) -> Dict[str, str]:
        """データフレームからカラムの型を推論"""
        column_types = {}
        
        for column in df.columns:
            series = df[column]
            
            # NaN値を除外
            non_null_series = series.dropna()
            
            if len(non_null_series) == 0:
                column_types[column] = 'STRING'
                continue
            
            # 数値型の判定
            try:
                # 整数判定
                if pd.api.types.is_integer_dtype(non_null_series):
                    column_types[column] = 'INTEGER'
                    continue
                
                # 浮動小数点数判定
                if pd.api.types.is_float_dtype(non_null_series):
                    column_types[column] = 'FLOAT'
                    continue
                
                # 数値として解釈可能かチェック
                pd.to_numeric(non_null_series)
                # 小数点があるかチェック
                if any('.' in str(val) for val in non_null_series):
                    column_types[column] = 'FLOAT'
                else:
                    column_types[column] = 'INTEGER'
                continue
                
            except (ValueError, TypeError):
                pass
            
            # 日時型の判定
            try:
                pd.to_datetime(non_null_series)
                column_types[column] = 'DATETIME'
                continue
            except (ValueError, TypeError):
                pass
            
            # ブール型の判定
            if set(non_null_series.astype(str).str.lower()).issubset({'true', 'false', '1', '0', 'yes', 'no'}):
                column_types[column] = 'BOOLEAN'
                continue
            
            # デフォルトは文字列
            column_types[column] = 'STRING'
        
        return column_types
    
    def calculate_statistics(self, df: pd.DataFrame, column: str, column_type: str) -> Dict[str, Any]:
        """カラムの統計情報を計算"""
        series = df[column].dropna()
        stats = {}
        
        try:
            if column_type in ['INTEGER', 'FLOAT']:
                numeric_series = pd.to_numeric(series, errors='coerce').dropna()
                if len(numeric_series) > 0:
                    stats['min_value'] = float(numeric_series.min())
                    stats['max_value'] = float(numeric_series.max())
                    stats['mean_value'] = float(numeric_series.mean())
                    stats['std_value'] = float(numeric_series.std())
            
            stats['unique_count'] = len(series.unique())
            stats['null_count'] = len(df[column]) - len(series)
            stats['null_percentage'] = (stats['null_count'] / len(df[column])) * 100
            
        except Exception as e:
            logger.warning(f"統計計算エラー for {column}: {e}")
        
        return stats
    
    def generate_data_hash(self, data: Dict) -> str:
        """データのハッシュを生成（重複検出用）"""
        data_str = json.dumps(data, sort_keys=True, ensure_ascii=False)
        return hashlib.sha256(data_str.encode('utf-8')).hexdigest()
    
    @transaction.atomic
    def process_csv(self, raw_file: RawDataFile, dataset: Dataset) -> Dict[str, Any]:
        """
        CSVファイルを処理してデータベースに保存
        """
        try:
            # エンコーディングと区切り文字の検出
            file_path = raw_file.file.path
            encoding = self.detect_encoding(file_path)
            delimiter = self.detect_delimiter(file_path, encoding)
            
            # CSVファイルの読み込み
            df = pd.read_csv(file_path, encoding=encoding, delimiter=delimiter)
            
            # 空のデータフレームチェック
            if df.empty:
                raise ValidationError("CSVファイルにデータが含まれていません")
            
            # カラム型の推論
            column_types = self.infer_column_types(df)
            
            # 既存のスキーマを削除（再処理の場合）
            dataset.schema_fields.all().delete()
            
            # スキーマの作成
            for idx, (column, column_type) in enumerate(column_types.items()):
                stats = self.calculate_statistics(df, column, column_type)
                
                DataSchema.objects.create(
                    dataset=dataset,
                    column_name=column,
                    column_type=column_type,
                    column_order=idx,
                    min_value=stats.get('min_value'),
                    max_value=stats.get('max_value'),
                    unique_count=stats.get('unique_count', 0)
                )
            
            # 既存のレコードを削除（再処理の場合）
            dataset.records.all().delete()
            
            # データレコードの保存
            records_to_create = []
            duplicate_count = 0
            error_count = 0
            
            for idx, row in df.iterrows():
                try:
                    # NaN値をNoneに変換
                    data = {}
                    for col in df.columns:
                        value = row[col]
                        if pd.isna(value):
                            data[col] = None
                        elif column_types[col] == 'DATETIME':
                            try:
                                data[col] = pd.to_datetime(value).isoformat()
                            except:
                                data[col] = str(value)
                        else:
                            data[col] = value
                    
                    data_hash = self.generate_data_hash(data)
                    
                    record = DataRecord(
                        dataset=dataset,
                        row_number=idx + 1,
                        data=data,
                        data_hash=data_hash
                    )
                    records_to_create.append(record)
                    
                except Exception as e:
                    logger.error(f"行 {idx + 1} の処理エラー: {e}")
                    error_count += 1
            
            # バルクインサート
            DataRecord.objects.bulk_create(records_to_create, batch_size=1000)
            
            # データセット情報の更新
            dataset.total_rows = len(records_to_create)
            dataset.save()
            
            # 処理完了マーク
            raw_file.processed = True
            raw_file.encoding = encoding
            raw_file.delimiter = delimiter
            raw_file.save()
            
            # 品質レポートの作成
            quality_report = self.create_quality_report(dataset, df, error_count, duplicate_count)
            
            return {
                'success': True,
                'total_rows': len(df),
                'processed_rows': len(records_to_create),
                'error_rows': error_count,
                'duplicate_rows': duplicate_count,
                'columns': list(column_types.keys()),
                'quality_score': quality_report.valid_records / quality_report.total_records * 100
            }
            
        except Exception as e:
            # エラー情報を保存
            raw_file.processing_error = str(e)
            raw_file.save()
            logger.error(f"CSV処理エラー: {e}")
            raise ValidationError(f"CSV処理中にエラーが発生しました: {e}")
    
    def create_quality_report(self, dataset: Dataset, df: pd.DataFrame, 
                            error_count: int, duplicate_count: int) -> DataQualityReport:
        """データ品質レポートを作成"""
        total_records = len(df)
        valid_records = total_records - error_count
        
        # 詳細な品質情報
        quality_details = {
            'column_quality': {},
            'completeness': {},
            'data_types': {}
        }
        
        for column in df.columns:
            series = df[column]
            null_count = series.isnull().sum()
            completeness = ((len(series) - null_count) / len(series)) * 100
            
            quality_details['column_quality'][column] = {
                'completeness_percentage': completeness,
                'null_count': int(null_count),
                'unique_values': int(series.nunique()),
                'data_type': str(series.dtype)
            }
        
        report = DataQualityReport.objects.create(
            dataset=dataset,
            total_records=total_records,
            valid_records=valid_records,
            invalid_records=error_count,
            duplicate_records=duplicate_count,
            quality_details=quality_details
        )
        
        return report


class DataValidator:
    """
    データバリデーションクラス
    """
    
    def validate_range(self, value: Any, config: Dict) -> bool:
        """範囲チェック"""
        try:
            num_value = float(value)
            min_val = config.get('min')
            max_val = config.get('max')
            
            if min_val is not None and num_value < min_val:
                return False
            if max_val is not None and num_value > max_val:
                return False
            
            return True
        except (ValueError, TypeError):
            return False
    
    def validate_pattern(self, value: Any, config: Dict) -> bool:
        """パターンマッチング"""
        import re
        try:
            pattern = config.get('pattern')
            if pattern:
                return bool(re.match(pattern, str(value)))
            return True
        except Exception:
            return False
    
    def validate_not_null(self, value: Any, config: Dict) -> bool:
        """NOT NULL チェック"""
        return value is not None and value != '' and not pd.isna(value)
    
    def validate_record(self, record_data: Dict, dataset: Dataset) -> Tuple[bool, List[str]]:
        """レコード全体のバリデーション"""
        errors = []
        
        validation_rules = dataset.validation_rules.filter(is_active=True)
        
        for rule in validation_rules:
            column_name = rule.column_name
            value = record_data.get(column_name)
            
            validator_method = getattr(self, f'validate_{rule.rule_type.lower()}', None)
            if validator_method:
                if not validator_method(value, rule.rule_config):
                    errors.append(f"{column_name}: {rule.rule_type} validation failed")
        
        return len(errors) == 0, errors
