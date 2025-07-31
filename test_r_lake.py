#!/usr/bin/env python3
"""
R-Lake Datalake Test Script
車両データCSVのアップロードとデータ分析のテスト
"""
import os
import sys
import django

# Django setup
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'R_Lake.settings')
django.setup()

from django.contrib.auth.models import User
from ingest.models import Dataset, RawDataFile, DataRecord
from ingest.processors import CSVProcessor
import pandas as pd

def test_csv_upload():
    """サンプルCSVファイルのアップロードテスト"""
    print("=== R-Lake Datalake CSV Upload Test ===")
    
    # 管理者ユーザーを取得
    try:
        admin_user = User.objects.get(username='admin')
        print(f"✓ Admin user found: {admin_user.username}")
    except User.DoesNotExist:
        print("✗ Admin user not found")
        return False
    
    # サンプルCSVファイルのパス
    csv_file_path = 'sample_vehicle_data.csv'
    
    if not os.path.exists(csv_file_path):
        print(f"✗ CSV file not found: {csv_file_path}")
        return False
    
    print(f"✓ CSV file found: {csv_file_path}")
    
    # データセットの作成
    dataset = Dataset.objects.create(
        name="車両テストデータ_VH001",
        description="VH001車両の計測データ（加速・減速テスト）",
        created_by=admin_user,
        vehicle_model="テストビークル"
    )
    print(f"✓ Dataset created: {dataset.name} (ID: {dataset.id})")
    
    # CSVプロセッサーでデータを処理
    processor = CSVProcessor()
    
    try:
        # まずRawDataFileを作成
        with open(csv_file_path, 'rb') as csv_file:
            from django.core.files.base import ContentFile
            raw_file = RawDataFile.objects.create(
                dataset=dataset,
                original_filename="sample_vehicle_data.csv",
                file_size=os.path.getsize(csv_file_path),
                file=ContentFile(csv_file.read(), name="sample_vehicle_data.csv")
            )
            
            # CSVファイルを処理
            result = processor.process_csv(raw_file, dataset)
            
            print(f"✓ CSV processing successful")
            print(f"  - Raw data file: {raw_file.original_filename}")
            print(f"  - Records created: {result.get('records_created', 'N/A')}")
            print(f"  - Schema info: {result.get('schema_info', 'N/A')}")
            
            # データセット統計の更新
            dataset.refresh_from_db()
            print(f"  - Total rows in dataset: {dataset.total_rows}")
            
            return True
                
    except Exception as e:
        print(f"✗ Error during CSV processing: {str(e)}")
        return False

def test_data_analysis():
    """データ分析のテスト"""
    print("\n=== Data Analysis Test ===")
    
    # 最新のデータセットを取得
    try:
        latest_dataset = Dataset.objects.latest('created_at')
        print(f"✓ Latest dataset: {latest_dataset.name}")
        
        # データレコードを取得
        records = DataRecord.objects.filter(dataset=latest_dataset)[:5]
        print(f"✓ Found {records.count()} records (showing first 5):")
        
        for record in records:
            data = record.data
            print(f"  - Timestamp: {data.get('timestamp', 'N/A')}, "
                  f"Speed: {data.get('speed', 'N/A')}, "
                  f"RPM: {data.get('rpm', 'N/A')}")
        
        # スキーマ情報の表示
        schemas = latest_dataset.schema_fields.all()
        if schemas.exists():
            print(f"✓ Schema columns found: {schemas.count()}")
            for schema in schemas[:3]:  # 最初の3つのカラムを表示
                print(f"  - {schema.column_name}: {schema.column_type}")
        else:
            print("! No schema information found")
            
        return True
        
    except Dataset.DoesNotExist:
        print("✗ No datasets found")
        return False
    except Exception as e:
        print(f"✗ Error during data analysis: {str(e)}")
        return False

def main():
    """メイン実行関数"""
    print("R-Lake Datalake Platform Test")
    print("=" * 50)
    
    # テスト実行
    upload_success = test_csv_upload()
    analysis_success = test_data_analysis()
    
    # 結果サマリー
    print("\n" + "=" * 50)
    print("TEST RESULTS:")
    print(f"CSV Upload: {'✓ PASS' if upload_success else '✗ FAIL'}")
    print(f"Data Analysis: {'✓ PASS' if analysis_success else '✗ FAIL'}")
    
    if upload_success and analysis_success:
        print("\n🎉 All tests passed! R-Lake is ready for use.")
        print("\nNext steps:")
        print("1. Login to admin panel: http://127.0.0.1:8000/admin/")
        print("2. View datasets: http://127.0.0.1:8000/ingest/")
        print("3. Create visualizations: http://127.0.0.1:8000/visualization/")
    else:
        print("\n❌ Some tests failed. Please check the errors above.")

if __name__ == "__main__":
    main()
