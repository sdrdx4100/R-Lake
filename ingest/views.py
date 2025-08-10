from django.shortcuts import render, get_object_or_404, redirect
from django.contrib.auth.decorators import login_required
from django.contrib.auth.mixins import LoginRequiredMixin
from django.views.generic import CreateView, ListView, DetailView
from django.http import JsonResponse, HttpResponse
from django.contrib import messages
from django.db import transaction
from django.db.models import Q
from django.utils.dateparse import parse_datetime
from django.utils import timezone
from rest_framework import viewsets, status
from rest_framework.decorators import action
from rest_framework.response import Response
from rest_framework.parsers import MultiPartParser, FormParser
from .models import Dataset, DataSchema, RawDataFile, DataRecord, DataQualityReport, PreprocessJob, JobRun
from visualization.models import Chart
from .processors import CSVProcessor
from .serializers import DatasetSerializer, DataSchemaSerializer, RawDataFileSerializer
import json
import logging
import os
import tempfile

logger = logging.getLogger(__name__)


@login_required
def upload_csv(request):
    """CSV アップロードページ（前処理ジョブ統合・複数ファイル対応）"""
    if request.method == 'POST':
        try:
            # 複数ファイル対応（input name="file" の multiple 前提）
            files = request.FILES.getlist('file')
            dataset_name = request.POST.get('dataset_name')
            description = request.POST.get('description', '')
            vehicle_model = request.POST.get('vehicle_model', '')
            measurement_location = request.POST.get('measurement_location', '')
            # 追加メタデータ
            tags = request.POST.get('tags', '')
            source = request.POST.get('source', '')
            sensor_type = request.POST.get('sensor_type', '')
            project = request.POST.get('project', '')
            license_ = request.POST.get('license', '')
            measurement_start = request.POST.get('measurement_start')
            measurement_end = request.POST.get('measurement_end')
            notes = request.POST.get('notes', '')
            # タイムゾーン aware に変換
            ms = timezone.make_aware(parse_datetime(measurement_start)) if measurement_start else None
            me = timezone.make_aware(parse_datetime(measurement_end)) if measurement_end else None

            # 前処理ジョブ関連（全ファイル共通設定）
            use_preprocess = request.POST.get('use_preprocess') == 'on'
            existing_job_id = request.POST.get('pre_job_id')
            new_job_name = request.POST.get('pre_job_name')
            new_job_desc = request.POST.get('pre_job_desc', '')
            # Notebook/Python 両対応の入力
            new_job_nb_file = request.FILES.get('pre_job_notebook')
            pre_job_type = request.POST.get('pre_job_type', 'notebook')
            pre_job_script_file = request.FILES.get('pre_job_script')
            pre_job_script_path = request.POST.get('pre_job_script_path', '')
            pre_job_entry_function = request.POST.get('pre_job_entry_function', 'process')
            default_params_str = request.POST.get('pre_job_default_params', '')
            run_params_str = request.POST.get('pre_job_run_params', '')
            output_dataset_name = request.POST.get('pre_output_dataset_name')

            # 入力チェック
            if not files or len(files) == 0 or not dataset_name:
                messages.error(request, 'ファイル（複数可）とデータセット名は必須です。')
                return render(request, 'ingest/upload.html')

            # JSONパラメータ（ジョブ既定/実行）
            import json as _json
            try:
                default_params = _json.loads(default_params_str) if default_params_str else {}
            except Exception:
                default_params = {}
            try:
                run_params = _json.loads(run_params_str) if run_params_str else {}
            except Exception:
                run_params = {}

            # 前処理ジョブの決定は一度だけ
            job = None
            pm = None
            if use_preprocess:
                if new_job_name or new_job_nb_file or pre_job_script_file or pre_job_script_path:
                    # 新規ジョブ登録（Notebook / Python）
                    jt = pre_job_type if pre_job_type in ('notebook', 'python') else 'notebook'
                    # 名前のユニーク化
                    base_name = new_job_name or f"job_{timezone.now().strftime('%Y%m%d_%H%M%S')}"
                    unique_name = base_name
                    if PreprocessJob.objects.filter(name=unique_name).exists():
                        unique_name = f"{base_name}_{timezone.now().strftime('%H%M%S')}"
                    job = PreprocessJob.objects.create(
                        name=unique_name,
                        description=new_job_desc,
                        default_parameters=default_params,
                        created_by=request.user,
                        job_type=jt,
                        entry_function=pre_job_entry_function or 'process',
                    )
                    if jt == 'notebook':
                        if new_job_nb_file:
                            job.notebook_file.save(new_job_nb_file.name, new_job_nb_file)
                            job.save()
                        else:
                            messages.error(request, 'Notebookジョブを選択しましたが、.ipynb が指定されていません。')
                            return render(request, 'ingest/upload.html')
                    else:  # python
                        if pre_job_script_file:
                            job.script_file.save(pre_job_script_file.name, pre_job_script_file)
                            job.save()
                        elif pre_job_script_path:
                            job.script_path = pre_job_script_path
                            job.save(update_fields=['script_path'])
                        else:
                            messages.error(request, 'Pythonジョブを選択しましたが、.py または スクリプトパスが指定されていません。')
                            return render(request, 'ingest/upload.html')
                elif existing_job_id:
                    job = get_object_or_404(PreprocessJob, id=existing_job_id, is_active=True)
                else:
                    messages.error(request, '前処理を選択しましたが、ジョブが指定されていません。')
                    return render(request, 'ingest/upload.html')
                # notebook 用 papermill を一度だけ動的インポート（python スクリプト時は不要）
                if job.job_type == 'notebook':
                    import importlib
                    try:
                        pm = importlib.import_module('papermill')
                    except ImportError:
                        messages.error(request, 'papermill がインストールされていません。requirements.txt をインストールしてください。')
                        return render(request, 'ingest/upload.html')

            processor = CSVProcessor()

            created_input_ids = []
            created_output_ids = []
            total_processed_rows = 0
            errors = []

            # 各ファイルを独立トランザクションで処理
            for idx, uploaded_file in enumerate(files, start=1):
                try:
                    # ファイルごとのデータセット名（複数のときはファイル名を付加）
                    base_name = dataset_name
                    if len(files) > 1:
                        name_suffix = os.path.splitext(uploaded_file.name)[0]
                        base_name = f"{dataset_name} - {name_suffix}"

                    with transaction.atomic():
                        # 入力データセット（元データ用）
                        input_dataset = Dataset.objects.create(
                            name=base_name,
                            description=description,
                            created_by=request.user,
                            vehicle_model=vehicle_model,
                            measurement_location=measurement_location,
                            tags=tags,
                            source=source,
                            sensor_type=sensor_type,
                            project=project,
                            license=license_,
                            measurement_start=ms,
                            measurement_end=me,
                            notes=notes,
                        )

                        raw_file = RawDataFile.objects.create(
                            dataset=input_dataset,
                            original_filename=uploaded_file.name,
                            file=uploaded_file,
                            file_size=uploaded_file.size
                        )

                        # 前処理なしの場合のみ、元ファイルを即時取り込み
                        if not use_preprocess:
                            result = processor.process_csv(raw_file, input_dataset)
                            total_processed_rows += result.get('processed_rows', 0)
                            created_input_ids.append(input_dataset.pk)
                            messages.success(request, f'[{idx}/{len(files)}] データセット "{input_dataset.name}" を作成しました（{result.get("processed_rows",0)} 行）。')
                            continue

                        # ここから前処理ありの実行フロー（元CSVの直接取り込みは行わない）
                        run = JobRun.objects.create(job=job, input_file=raw_file, parameters=run_params, status='RUNNING', started_at=timezone.now())

                        with tempfile.TemporaryDirectory() as tmpd:
                            out_nb = os.path.join(tmpd, 'executed.ipynb')
                            output_csv = os.path.join(tmpd, 'output.csv')

                            # Notebook / Python で分岐
                            if job.job_type == 'notebook':
                                nb_params = {
                                    'input_csv': raw_file.file.path,
                                    'output_csv': output_csv,
                                }
                                nb_params.update(job.default_parameters or {})
                                nb_params.update(run_params or {})

                                notebook_src = job.notebook_file.path if job.notebook_file else job.notebook_path
                                if not notebook_src:
                                    raise Exception('Notebook が設定されていません')

                                pm.execute_notebook(notebook_src, out_nb, parameters=nb_params)

                                produced_csv_path = output_csv

                            else:  # python スクリプト
                                import importlib.util
                                from pathlib import Path

                                script_src = job.script_file.path if job.script_file else job.script_path
                                if not script_src:
                                    raise Exception('スクリプトが設定されていません')
                                script_src = os.path.abspath(script_src)

                                spec = importlib.util.spec_from_file_location(f"rlake_job_{job.id}", script_src)
                                if spec is None or spec.loader is None:
                                    raise Exception('スクリプトをロードできません')
                                module = importlib.util.module_from_spec(spec)
                                spec.loader.exec_module(module)  # type: ignore

                                func_name = job.entry_function or 'process'
                                if not hasattr(module, func_name):
                                    raise Exception(f'エントリ関数 {func_name} が見つかりません')
                                func = getattr(module, func_name)

                                # 簡易コンテキスト
                                class JobContext:
                                    def __init__(self, input_file, params, tmpdir):
                                        self.input_file = input_file
                                        self.input_path = input_file.file.path
                                        self.parameters = params
                                        self.tempdir = tmpdir
                                    def make_output_path(self, name='output.csv'):
                                        return os.path.join(self.tempdir, name)
                                    def logger(self, msg: str):
                                        try:
                                            run.log = (run.log or '') + f"\n{msg}"
                                            run.save(update_fields=['log'])
                                        except Exception:
                                            pass

                                ctx = JobContext(raw_file, {**(job.default_parameters or {}), **(run_params or {})}, tmpd)
                                ret = func(ctx)

                                # 返り値の解釈（第1段階: Path/str/DF/Iterable[dict]）
                                produced_csv_path = None
                                try:
                                    from collections.abc import Iterable
                                    import pandas as pd  # noqa: F401
                                except Exception:
                                    pd = None  # type: ignore
                                    Iterable = None  # type: ignore

                                if ret is None:
                                    # ユーザスクリプトが ctx.make_output_path('output.csv') に出力したとみなす
                                    produced_csv_path = output_csv
                                elif isinstance(ret, (str, Path)):
                                    produced_csv_path = str(ret)
                                elif pd is not None and hasattr(ret, 'to_csv'):
                                    # pandas DataFrame を返した場合
                                    ret.to_csv(output_csv, index=False)
                                    produced_csv_path = output_csv
                                elif Iterable is not None and isinstance(ret, Iterable):
                                    # Iterable[dict] をCSVに
                                    rows = list(ret)
                                    if not rows:
                                        raise Exception('スクリプトの出力が空です')
                                    import csv
                                    cols = sorted({k for row in rows for k in row.keys()})
                                    with open(output_csv, 'w', newline='', encoding='utf-8') as f:
                                        w = csv.DictWriter(f, fieldnames=cols)
                                        w.writeheader()
                                        for r in rows:
                                            w.writerow(r)
                                    produced_csv_path = output_csv
                                else:
                                    raise Exception('サポートされていない戻り値です（Path/str/DataFrame/Iterable[dict] を返してください）')

                                if not produced_csv_path or not os.path.exists(produced_csv_path):
                                    raise Exception('出力CSVが見つかりません')

                            # 出力データセット作成
                            from django.core.files.base import ContentFile
                            with open(produced_csv_path, 'rb') as f:
                                data_bytes = f.read()

                            out_name = (output_dataset_name or f"{base_name}_proc")
                            output_dataset = Dataset.objects.create(
                                name=out_name,
                                description=f"Job {job.name} により生成",
                                created_by=request.user,
                                project=project,
                                tags=tags,
                            )
                            new_raw = RawDataFile.objects.create(
                                dataset=output_dataset,
                                original_filename=os.path.basename(produced_csv_path),
                                file_size=os.path.getsize(produced_csv_path),
                            )
                            new_raw.file.save(os.path.basename(produced_csv_path), ContentFile(data_bytes))
                            new_raw.save()

                            # 生成CSVを処理
                            processor.process_csv(new_raw, output_dataset)

                        run.status = 'SUCCESS'
                        run.output_dataset = output_dataset
                        run.finished_at = timezone.now()
                        run.save()

                        created_output_ids.append(output_dataset.pk)
                        messages.success(request, f'[{idx}/{len(files)}] 前処理ジョブを実行し、データセット "{out_name}" を作成しました。')

                except Exception as e:
                    logger.error(f"アップロード/前処理エラー（{uploaded_file.name}）: {e}")
                    errors.append(str(e))
                    messages.error(request, f'[{idx}/{len(files)}] {uploaded_file.name}: エラーが発生しました: {e}')
                    # 続行（他ファイルは処理）
                    continue

            # 処理完了後の遷移
            if len(files) == 1:
                if use_preprocess and created_output_ids:
                    return redirect('ingest:dataset_detail', pk=created_output_ids[0])
                if not use_preprocess and created_input_ids:
                    return redirect('ingest:dataset_detail', pk=created_input_ids[0])

            # 複数 or 一部失敗時は一覧へ
            if errors:
                messages.warning(request, f'一部のファイルでエラーが発生しました（成功: {len(created_input_ids)+len(created_output_ids)} / 合計: {len(files)}）。')
            else:
                messages.info(request, f'{len(files)}件のファイルを処理しました。作成データセット: {len(created_input_ids)+len(created_output_ids)}件。')
            return redirect('ingest:dataset_list')

        except Exception as e:
            logger.error(f"アップロード/前処理エラー: {e}")
            messages.error(request, f'エラーが発生しました: {str(e)}')
            return render(request, 'ingest/upload.html')

    # GET
    jobs = PreprocessJob.objects.filter(is_active=True).order_by('name')
    return render(request, 'ingest/upload.html', { 'jobs': jobs })


class DatasetListView(LoginRequiredMixin, ListView):
    """データセット一覧"""
    model = Dataset
    template_name = 'ingest/dataset_list.html'
    context_object_name = 'datasets'
    paginate_by = 20

    def get_queryset(self):
        qs = Dataset.objects.filter(is_active=True).order_by('-created_at')
        # クエリパラメータで検索
        q = self.request.GET.get('q')
        vehicle = self.request.GET.get('vehicle')
        tag = self.request.GET.get('tag')
        project = self.request.GET.get('project')
        creator = self.request.GET.get('creator')
        date_from = self.request.GET.get('from')
        date_to = self.request.GET.get('to')

        if q:
            qs = qs.filter(Q(name__icontains=q) | Q(description__icontains=q))
        if vehicle:
            qs = qs.filter(vehicle_model__icontains=vehicle)
        if tag:
            qs = qs.filter(tags__icontains=tag)
        if project:
            qs = qs.filter(project__icontains=project)
        if creator:
            qs = qs.filter(created_by__username__icontains=creator)
        if date_from:
            qs = qs.filter(created_at__date__gte=date_from)
        if date_to:
            qs = qs.filter(created_at__date__lte=date_to)
        return qs

    def get_context_data(self, **kwargs):
        ctx = super().get_context_data(**kwargs)
        ctx['query'] = {
            'q': self.request.GET.get('q', ''),
            'vehicle': self.request.GET.get('vehicle', ''),
            'tag': self.request.GET.get('tag', ''),
            'project': self.request.GET.get('project', ''),
            'creator': self.request.GET.get('creator', ''),
            'from': self.request.GET.get('from', ''),
            'to': self.request.GET.get('to', ''),
        }
        return ctx


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
        # このデータセットを利用しているグラフ（簡易ライネージ）
        context['used_charts'] = Chart.objects.filter(dataset=dataset).order_by('-updated_at')
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

        # 簡易フィルタリング（クエリ文字列の filter_ プレフィックスを解釈）
        def match_filters(row: dict, filters: dict) -> bool:
            for key, cond in filters.items():
                val = row.get(cond['col'])
                op = cond['op']
                target = cond['value']
                try:
                    if op == 'eq' and not (val == target):
                        return False
                    if op == 'contains' and not (str(target).lower() in str(val).lower() if val is not None else False):
                        return False
                    if op == 'gte':
                        if val is None:
                            return False
                        if float(val) < float(target):
                            return False
                    if op == 'lte':
                        if val is None:
                            return False
                        if float(val) > float(target):
                            return False
                except Exception:
                    return False
            return True

        filters = {}
        for key, value in request.GET.items():
            if not key.startswith('filter_'):
                continue
            rest = key.replace('filter_', '', 1)
            if '__' in rest:
                col, op = rest.split('__', 1)
            else:
                col, op = rest, 'eq'
            if value is None or value == '':
                continue
            filters[key] = {'col': col, 'op': op, 'value': value}

        # 全件を取得してPython側でフィルタ（簡易実装）
        all_records = dataset.records.all()
        all_data = [r.data for r in all_records]
        if filters:
            all_data = [row for row in all_data if match_filters(row, filters)]

        total = len(all_data)
        data = all_data[start:end]

        return JsonResponse({
            'data': data,
            'total_records': total,
            'page': page,
            'per_page': per_page,
            'has_next': end < total
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


@login_required
def dataset_export_schema_csv(request, dataset_id):
    """スキーマをCSVダウンロード"""
    import csv
    dataset = get_object_or_404(Dataset, id=dataset_id, is_active=True)
    response = HttpResponse(content_type='text/csv; charset=utf-8')
    response['Content-Disposition'] = f'attachment; filename="schema_{dataset_id}.csv"'
    writer = csv.writer(response)
    writer.writerow(['column_name', 'column_type', 'is_nullable', 'min_value', 'max_value', 'unique_count'])
    for f in dataset.schema_fields.all().order_by('column_order'):
        writer.writerow([f.column_name, f.column_type, f.is_nullable, f.min_value, f.max_value, f.unique_count])
    return response


@login_required
def dataset_export_sample_csv(request, dataset_id):
    """サンプルデータをCSVダウンロード（先頭N行）"""
    import csv
    dataset = get_object_or_404(Dataset, id=dataset_id, is_active=True)
    limit = int(request.GET.get('limit', 100))
    records = dataset.records.all()[:limit]
    # カラム順はスキーマ順
    columns = [f.column_name for f in dataset.schema_fields.all().order_by('column_order')]
    response = HttpResponse(content_type='text/csv; charset=utf-8')
    response['Content-Disposition'] = f'attachment; filename="sample_{dataset_id}.csv"'
    writer = csv.writer(response)
    writer.writerow(['row_number'] + columns)
    for r in records:
        row = [r.row_number] + [r.data.get(col) for col in columns]
        writer.writerow(row)
    return response


# REST API ViewSets
class DatasetViewSet(viewsets.ModelViewSet):
    """Dataset REST API"""
    serializer_class = DatasetSerializer

    def get_queryset(self):
        return Dataset.objects.filter(is_active=True)

    def perform_create(self, serializer):
        serializer.save(created_by=self.request.user)

    @action(detail=False, methods=['get'], url_path='search')
    def search(self, request):
        """メタデータを使ったデータセット検索API"""
        qs = Dataset.objects.filter(is_active=True)
        q = request.query_params.get('q')
        vehicle = request.query_params.get('vehicle')
        tag = request.query_params.get('tag')
        project = request.query_params.get('project')
        creator = request.query_params.get('creator')
        source = request.query_params.get('source')
        sensor = request.query_params.get('sensor')
        date_from = request.query_params.get('from')
        date_to = request.query_params.get('to')

        if q:
            qs = qs.filter(Q(name__icontains=q) | Q(description__icontains=q))
        if vehicle:
            qs = qs.filter(vehicle_model__icontains=vehicle)
        if tag:
            qs = qs.filter(tags__icontains=tag)
        if project:
            qs = qs.filter(project__icontains=project)
        if creator:
            qs = qs.filter(created_by__username__icontains=creator)
        if source:
            qs = qs.filter(source__icontains=source)
        if sensor:
            qs = qs.filter(sensor_type__icontains=sensor)
        if date_from:
            qs = qs.filter(created_at__date__gte=date_from)
        if date_to:
            qs = qs.filter(created_at__date__lte=date_to)

        page = int(request.query_params.get('page', 1))
        per_page = min(int(request.query_params.get('per_page', 50)), 200)
        start = (page - 1) * per_page
        end = start + per_page
        total = qs.count()
        items = qs.order_by('-created_at')[start:end]
        data = DatasetSerializer(items, many=True).data
        return Response({
            'results': data,
            'page': page,
            'per_page': per_page,
            'total': total,
            'has_next': end < total
        })

    @action(detail=True, methods=['get'])
    def data(self, request, pk=None):
        """データセットのデータを取得"""
        dataset = self.get_object()

        # フィルタリングパラメータ（filter_colまたはfilter_col__op）
        filters = {}
        for key, value in request.query_params.items():
            if not key.startswith('filter_'):
                continue
            rest = key.replace('filter_', '', 1)
            if '__' in rest:
                col, op = rest.split('__', 1)
            else:
                col, op = rest, 'eq'
            if value is None or value == '':
                continue
            filters[key] = {'col': col, 'op': op, 'value': value}

        # ページネーション
        page = int(request.query_params.get('page', 1))
        per_page = min(int(request.query_params.get('per_page', 100)), 1000)

        start = (page - 1) * per_page
        end = start + per_page

        records_all = dataset.records.all()
        data_all = [r.data for r in records_all]

        def match_filters(row: dict) -> bool:
            for _, cond in filters.items():
                val = row.get(cond['col'])
                op = cond['op']
                target = cond['value']
                try:
                    if op == 'eq' and not (val == target):
                        return False
                    if op == 'contains' and not (str(target).lower() in str(val).lower() if val is not None else False):
                        return False
                    if op == 'gte':
                        if val is None or float(val) < float(target):
                            return False
                    if op == 'lte':
                        if val is None or float(val) > float(target):
                            return False
                except Exception:
                    return False
            return True

        if filters:
            data_all = [row for row in data_all if match_filters(row)]

        total = len(data_all)
        data = data_all[start:end]

        return Response({
            'data': data,
            'pagination': {
                'page': page,
                'per_page': per_page,
                'total_records': total,
                'has_next': end < total
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

    @action(detail=False, methods=['post'], url_path='jobs')
    def create_job(self, request):
        """前処理ジョブの登録"""
        try:
            name = request.data.get('name')
            notebook_path = request.data.get('notebook_path')
            description = request.data.get('description', '')
            default_parameters = request.data.get('default_parameters') or {}
            if not name or not notebook_path:
                return Response({'error': 'name と notebook_path は必須です'}, status=400)
            # 名前のユニーク化
            unique_name = name
            if PreprocessJob.objects.filter(name=unique_name).exists():
                unique_name = f"{name}_{timezone.now().strftime('%Y%m%d_%H%M%S')}"
            job = PreprocessJob.objects.create(
                name=unique_name,
                description=description,
                notebook_path=notebook_path,
                default_parameters=default_parameters,
                created_by=request.user,
            )
            return Response({'id': job.id, 'name': job.name})
        except Exception as e:
            logger.error(f"ジョブ登録エラー: {e}")
            return Response({'error': str(e)}, status=500)

    @action(detail=False, methods=['post'], url_path='jobs/(?P<job_id>[^/.]+)/run')
    def run_job(self, request, job_id=None):
        """前処理ジョブを実行して新しいデータセットを作成"""
        try:
            job = get_object_or_404(PreprocessJob, id=job_id, is_active=True)
            input_rawfile_id = request.data.get('input_rawfile_id')
            dataset_name = request.data.get('dataset_name')
            params = request.data.get('parameters') or {}
            if not input_rawfile_id or not dataset_name:
                return Response({'error': 'input_rawfile_id と dataset_name は必須です'}, status=400)
            raw_file = get_object_or_404(RawDataFile, id=input_rawfile_id)

            run = JobRun.objects.create(job=job, input_file=raw_file, parameters=params, status='PENDING')
            run.status = 'RUNNING'
            run.started_at = timezone.now()
            run.save()

            produced_csv_path = None

            if job.job_type == 'notebook':
                # papermill を動的インポート
                import importlib
                try:
                    pm = importlib.import_module('papermill')
                except ImportError:
                    run.status = 'FAILED'
                    run.log = 'papermill がインストールされていません。requirements.txt をインストールしてください。'
                    run.finished_at = timezone.now()
                    run.save()
                    return Response({'error': 'papermill not installed'}, status=503)

                with tempfile.TemporaryDirectory() as tmpd:
                    out_nb = os.path.join(tmpd, 'executed.ipynb')
                    output_csv = os.path.join(tmpd, 'output.csv')
                    # Notebookへ渡すパラメータ
                    nb_params = {
                        'input_csv': raw_file.file.path,
                        'output_csv': output_csv,
                    }
                    nb_params.update(job.default_parameters or {})
                    nb_params.update(params)
                    # 実行
                    src = job.notebook_file.path if job.notebook_file else job.notebook_path
                    pm.execute_notebook(src, out_nb, parameters=nb_params)
                    produced_csv_path = output_csv

            else:  # python script
                import importlib.util
                from pathlib import Path

                script_src = job.script_file.path if job.script_file else job.script_path
                if not script_src:
                    run.status = 'FAILED'
                    run.log = 'スクリプトが設定されていません'
                    run.finished_at = timezone.now()
                    run.save()
                    return Response({'error': 'script not set'}, status=400)
                script_src = os.path.abspath(script_src)

                spec = importlib.util.spec_from_file_location(f"rlake_job_{job.id}", script_src)
                if spec is None or spec.loader is None:
                    run.status = 'FAILED'
                    run.log = 'スクリプトをロードできません'
                    run.finished_at = timezone.now()
                    run.save()
                    return Response({'error': 'cannot load script'}, status=500)
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)  # type: ignore

                func_name = job.entry_function or 'process'
                if not hasattr(module, func_name):
                    run.status = 'FAILED'
                    run.log = f'エントリ関数 {func_name} が見つかりません'
                    run.finished_at = timezone.now()
                    run.save()
                    return Response({'error': 'entry function not found'}, status=400)
                func = getattr(module, func_name)

                with tempfile.TemporaryDirectory() as tmpd:
                    class JobContext:
                        def __init__(self, input_file, params, tmpdir):
                            self.input_file = input_file
                            self.input_path = input_file.file.path
                            self.parameters = params
                            self.tempdir = tmpdir
                        def make_output_path(self, name='output.csv'):
                            return os.path.join(self.tempdir, name)
                        def logger(self, msg: str):
                            try:
                                run.log = (run.log or '') + f"\n{msg}"
                                run.save(update_fields=['log'])
                            except Exception:
                                pass

                    ctx = JobContext(raw_file, {**(job.default_parameters or {}), **(params or {})}, tmpd)
                    ret = func(ctx)
                    try:
                        from collections.abc import Iterable
                        import pandas as pd  # noqa: F401
                    except Exception:
                        pd = None  # type: ignore
                        Iterable = None  # type: ignore

                    output_csv = os.path.join(tmpd, 'output.csv')
                    if ret is None:
                        produced_csv_path = output_csv
                    elif isinstance(ret, (str, Path)):
                        produced_csv_path = str(ret)
                    elif pd is not None and hasattr(ret, 'to_csv'):
                        ret.to_csv(output_csv, index=False)
                        produced_csv_path = output_csv
                    elif Iterable is not None and isinstance(ret, Iterable):
                        rows = list(ret)
                        if not rows:
                            run.status = 'FAILED'
                            run.log = 'スクリプトの出力が空です'
                            run.finished_at = timezone.now()
                            run.save()
                            return Response({'error': 'empty output'}, status=400)
                        import csv
                        cols = sorted({k for row in rows for k in row.keys()})
                        with open(output_csv, 'w', newline='', encoding='utf-8') as f:
                            w = csv.DictWriter(f, fieldnames=cols)
                            w.writeheader()
                            for r in rows:
                                w.writerow(r)
                        produced_csv_path = output_csv
                    else:
                        run.status = 'FAILED'
                        run.log = 'サポートされていない戻り値です（Path/str/DataFrame/Iterable[dict]）'
                        run.finished_at = timezone.now()
                        run.save()
                        return Response({'error': 'unsupported return value'}, status=400)

            # Notebook/Script 共通: 出力を保存しデータセット生成
            if not produced_csv_path or not os.path.exists(produced_csv_path):
                run.status = 'FAILED'
                run.log = '出力CSVが見つかりません'
                run.finished_at = timezone.now()
                run.save()
                return Response({'error': 'output not found'}, status=500)

            from django.core.files.base import ContentFile
            with open(produced_csv_path, 'rb') as f:
                data_bytes = f.read()
            dataset = Dataset.objects.create(
                name=dataset_name,
                description=f"Job {job.name} により生成",
                created_by=request.user,
            )
            new_raw = RawDataFile.objects.create(
                dataset=dataset,
                original_filename=os.path.basename(produced_csv_path),
                file_size=os.path.getsize(produced_csv_path),
            )
            new_raw.file.save(os.path.basename(produced_csv_path), ContentFile(data_bytes))
            new_raw.save()
            # 生成CSVを処理
            processor = CSVProcessor()
            processor.process_csv(new_raw, dataset)

            run.status = 'SUCCESS'
            run.output_dataset = dataset
            run.finished_at = timezone.now()
            run.save()
            return Response({'run_id': run.id, 'dataset_id': dataset.id})
        except Exception as e:
            logger.error(f"ジョブ実行エラー: {e}")
            try:
                run.status = 'FAILED'
                run.log = str(e)
                run.finished_at = timezone.now()
                run.save()
            except Exception:
                pass
            return Response({'error': str(e)}, status=500)


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
