from django.shortcuts import render, get_object_or_404, redirect
from django.contrib.auth.decorators import login_required
from django.contrib.auth.mixins import LoginRequiredMixin
from django.views.generic import CreateView, ListView, DetailView
from django.http import JsonResponse, HttpResponse
from django.contrib import messages
from django.db import transaction
from rest_framework import viewsets, status
from rest_framework.decorators import action
from rest_framework.response import Response
from rest_framework.parsers import MultiPartParser, FormParser
from .models import Dataset, DataSchema, RawDataFile, DataRecord, DataQualityReport
from .processors import CSVProcessor
from .serializers import DatasetSerializer, DataSchemaSerializer, RawDataFileSerializer
import json
import logging

logger = logging.getLogger(__name__)


@login_required
def upload_csv(request):
    """CSV アップロードページ"""
    if request.method == 'POST':
        try:
            # ファイルとメタデータの取得
            uploaded_file = request.FILES.get('file')
            dataset_name = request.POST.get('dataset_name')
            description = request.POST.get('description', '')
            vehicle_model = request.POST.get('vehicle_model', '')
            measurement_location = request.POST.get('measurement_location', '')
            
            if not uploaded_file or not dataset_name:
                messages.error(request, 'ファイルとデータセット名は必須です。')
                return render(request, 'ingest/upload.html')
            
            # データセット作成
            with transaction.atomic():
                dataset = Dataset.objects.create(
                    name=dataset_name,
                    description=description,
                    created_by=request.user,
                    vehicle_model=vehicle_model,
                    measurement_location=measurement_location
                )
                
                # ファイル保存
                raw_file = RawDataFile.objects.create(
                    dataset=dataset,
                    original_filename=uploaded_file.name,
                    file=uploaded_file,
                    file_size=uploaded_file.size
                )
                
                # CSV処理
                processor = CSVProcessor()
                result = processor.process_csv(raw_file, dataset)
                
                if result['success']:
                    messages.success(
                        request, 
                        f'データセット "{dataset_name}" が正常に作成されました。'
                        f'({result["processed_rows"]}行処理済み)'
                    )
                    return redirect('ingest:dataset_detail', pk=dataset.pk)
                else:
                    messages.error(request, 'CSV処理中にエラーが発生しました。')
                    return render(request, 'ingest/upload.html')
                    
        except Exception as e:
            logger.error(f"アップロードエラー: {e}")
            messages.error(request, f'エラーが発生しました: {str(e)}')
            return render(request, 'ingest/upload.html')
    
    return render(request, 'ingest/upload.html')


class DatasetListView(LoginRequiredMixin, ListView):
    """データセット一覧"""
    model = Dataset
    template_name = 'ingest/dataset_list.html'
    context_object_name = 'datasets'
    paginate_by = 20
    
    def get_queryset(self):
        return Dataset.objects.filter(is_active=True).order_by('-created_at')


class DatasetDetailView(LoginRequiredMixin, DetailView):
    """データセット詳細"""
    model = Dataset
    template_name = 'ingest/dataset_detail.html'
    context_object_name = 'dataset'
    
    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        dataset = self.object
        
        # スキーマ情報
        context['schema_fields'] = dataset.schema_fields.all()
        
        # 品質レポート
        context['quality_report'] = dataset.quality_reports.order_by('-report_date').first()
        
        # サンプルデータ
        context['sample_records'] = dataset.records.all()[:10]
        
        # 統計情報
        context['total_records'] = dataset.records.count()
        
        return context


@login_required
def dataset_data_api(request, dataset_id):
    """データセットのデータをJSON形式で返す"""
    try:
        dataset = get_object_or_404(Dataset, id=dataset_id, is_active=True)
        
        # ページネーション
        page = int(request.GET.get('page', 1))
        per_page = int(request.GET.get('per_page', 100))
        
        start = (page - 1) * per_page
        end = start + per_page
        
        records = dataset.records.all()[start:end]
        data = [record.data for record in records]
        
        return JsonResponse({
            'data': data,
            'total_records': dataset.total_rows,
            'page': page,
            'per_page': per_page,
            'has_next': end < dataset.total_rows
        })
        
    except Exception as e:
        return JsonResponse({'error': str(e)}, status=500)


@login_required
def dataset_schema_api(request, dataset_id):
    """データセットのスキーマ情報をJSON形式で返す"""
    try:
        dataset = get_object_or_404(Dataset, id=dataset_id, is_active=True)
        schema_fields = dataset.schema_fields.all()
        
        schema_data = []
        for field in schema_fields:
            schema_data.append({
                'column_name': field.column_name,
                'column_type': field.column_type,
                'is_nullable': field.is_nullable,
                'min_value': field.min_value,
                'max_value': field.max_value,
                'unique_count': field.unique_count
            })
        
        return JsonResponse({
            'schema': schema_data,
            'total_columns': len(schema_data)
        })
        
    except Exception as e:
        return JsonResponse({'error': str(e)}, status=500)


# REST API ViewSets
class DatasetViewSet(viewsets.ModelViewSet):
    """Dataset REST API"""
    serializer_class = DatasetSerializer
    
    def get_queryset(self):
        return Dataset.objects.filter(is_active=True)
    
    def perform_create(self, serializer):
        serializer.save(created_by=self.request.user)
    
    @action(detail=True, methods=['get'])
    def data(self, request, pk=None):
        """データセットのデータを取得"""
        dataset = self.get_object()
        
        # フィルタリングパラメータ
        filters = {}
        for key, value in request.query_params.items():
            if key.startswith('filter_'):
                column_name = key.replace('filter_', '')
                filters[column_name] = value
        
        # ページネーション
        page = int(request.query_params.get('page', 1))
        per_page = min(int(request.query_params.get('per_page', 100)), 1000)
        
        start = (page - 1) * per_page
        end = start + per_page
        
        records_query = dataset.records.all()
        
        # フィルタリング（簡易版）
        if filters:
            # TODO: より高度なフィルタリング実装
            pass
        
        records = records_query[start:end]
        data = [record.data for record in records]
        
        return Response({
            'data': data,
            'pagination': {
                'page': page,
                'per_page': per_page,
                'total_records': dataset.total_rows,
                'has_next': end < dataset.total_rows
            }
        })
    
    @action(detail=True, methods=['get'])
    def schema(self, request, pk=None):
        """データセットのスキーマを取得"""
        dataset = self.get_object()
        schema_fields = dataset.schema_fields.all()
        
        schema_data = DataSchemaSerializer(schema_fields, many=True).data
        
        return Response({
            'schema': schema_data,
            'total_columns': len(schema_data)
        })
    
    @action(detail=True, methods=['get'])
    def quality_report(self, request, pk=None):
        """データ品質レポートを取得"""
        dataset = self.get_object()
        latest_report = dataset.quality_reports.order_by('-report_date').first()
        
        if not latest_report:
            return Response({'error': '品質レポートが見つかりません'}, 
                          status=status.HTTP_404_NOT_FOUND)
        
        return Response({
            'total_records': latest_report.total_records,
            'valid_records': latest_report.valid_records,
            'invalid_records': latest_report.invalid_records,
            'duplicate_records': latest_report.duplicate_records,
            'quality_score': (latest_report.valid_records / latest_report.total_records * 100),
            'quality_details': latest_report.quality_details,
            'report_date': latest_report.report_date
        })


class RawDataFileViewSet(viewsets.ModelViewSet):
    """RawDataFile REST API"""
    serializer_class = RawDataFileSerializer
    parser_classes = (MultiPartParser, FormParser)
    
    def get_queryset(self):
        return RawDataFile.objects.all()
    
    @action(detail=True, methods=['post'])
    def process(self, request, pk=None):
        """ファイルを再処理"""
        raw_file = self.get_object()
        
        try:
            processor = CSVProcessor()
            result = processor.process_csv(raw_file, raw_file.dataset)
            
            return Response({
                'success': result['success'],
                'message': '処理が完了しました',
                'result': result
            })
            
        except Exception as e:
            logger.error(f"再処理エラー: {e}")
            return Response({
                'success': False,
                'error': str(e)
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
