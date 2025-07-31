import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional, Tuple
from ingest.models import Dataset, DataRecord
from visualization.models import Chart, Dashboard
import json
import logging

logger = logging.getLogger(__name__)


class ChartGenerator:
    """
    動的グラフ生成エンジン
    """
    
    def __init__(self):
        self.color_schemes = {
            'viridis': px.colors.sequential.Viridis,
            'plasma': px.colors.sequential.Plasma,
            'turbo': px.colors.sequential.Turbo,
            'blues': px.colors.sequential.Blues,
            'reds': px.colors.sequential.Reds,
            'greens': px.colors.sequential.Greens,
            'categorical': px.colors.qualitative.Set1,
        }
    
    def get_dataset_dataframe(self, dataset: Dataset, filters: Dict = None) -> pd.DataFrame:
        """データセットからDataFrameを生成"""
        try:
            records = dataset.records.all()
            
            if filters:
                # フィルタリングロジックを実装
                records = self.apply_filters(records, filters)
            
            if not records.exists():
                return pd.DataFrame()
            
            # DataRecordからDataFrameを構築
            data_list = []
            for record in records:
                data_list.append(record.data)
            
            df = pd.DataFrame(data_list)
            
            # データ型の変換
            schema_fields = dataset.schema_fields.all()
            for field in schema_fields:
                if field.column_name in df.columns:
                    df = self.convert_column_type(df, field.column_name, field.column_type)
            
            return df
            
        except Exception as e:
            logger.error(f"DataFrame生成エラー: {e}")
            return pd.DataFrame()
    
    def apply_filters(self, records, filters: Dict):
        """データフィルタリング"""
        # TODO: より高度なフィルタリング機能を実装
        return records
    
    def convert_column_type(self, df: pd.DataFrame, column: str, column_type: str) -> pd.DataFrame:
        """カラムのデータ型を変換"""
        try:
            if column_type == 'INTEGER':
                df[column] = pd.to_numeric(df[column], errors='coerce').astype('Int64')
            elif column_type == 'FLOAT':
                df[column] = pd.to_numeric(df[column], errors='coerce')
            elif column_type == 'DATETIME':
                df[column] = pd.to_datetime(df[column], errors='coerce')
            elif column_type == 'BOOLEAN':
                df[column] = df[column].astype('boolean')
            # STRINGはそのまま
            
        except Exception as e:
            logger.warning(f"型変換エラー {column}: {e}")
        
        return df
    
    def create_line_chart(self, df: pd.DataFrame, chart: Chart) -> go.Figure:
        """線グラフ作成"""
        fig = px.line(
            df,
            x=chart.x_axis_column,
            y=chart.y_axis_column,
            color=chart.color_column if chart.color_column else None,
            title=chart.title,
            color_discrete_sequence=self.color_schemes.get(chart.color_scheme, px.colors.qualitative.Set1)
        )
        
        return self.apply_chart_config(fig, chart)
    
    def create_bar_chart(self, df: pd.DataFrame, chart: Chart) -> go.Figure:
        """棒グラフ作成"""
        fig = px.bar(
            df,
            x=chart.x_axis_column,
            y=chart.y_axis_column,
            color=chart.color_column if chart.color_column else None,
            title=chart.title,
            color_discrete_sequence=self.color_schemes.get(chart.color_scheme, px.colors.qualitative.Set1)
        )
        
        return self.apply_chart_config(fig, chart)
    
    def create_scatter_chart(self, df: pd.DataFrame, chart: Chart) -> go.Figure:
        """散布図作成"""
        fig = px.scatter(
            df,
            x=chart.x_axis_column,
            y=chart.y_axis_column,
            color=chart.color_column if chart.color_column else None,
            size=chart.size_column if chart.size_column else None,
            title=chart.title,
            color_continuous_scale=self.color_schemes.get(chart.color_scheme, 'viridis')
        )
        
        return self.apply_chart_config(fig, chart)
    
    def create_histogram(self, df: pd.DataFrame, chart: Chart) -> go.Figure:
        """ヒストグラム作成"""
        fig = px.histogram(
            df,
            x=chart.x_axis_column,
            color=chart.color_column if chart.color_column else None,
            title=chart.title,
            color_discrete_sequence=self.color_schemes.get(chart.color_scheme, px.colors.qualitative.Set1)
        )
        
        return self.apply_chart_config(fig, chart)
    
    def create_box_plot(self, df: pd.DataFrame, chart: Chart) -> go.Figure:
        """ボックスプロット作成"""
        fig = px.box(
            df,
            x=chart.x_axis_column,
            y=chart.y_axis_column,
            color=chart.color_column if chart.color_column else None,
            title=chart.title,
            color_discrete_sequence=self.color_schemes.get(chart.color_scheme, px.colors.qualitative.Set1)
        )
        
        return self.apply_chart_config(fig, chart)
    
    def create_heatmap(self, df: pd.DataFrame, chart: Chart) -> go.Figure:
        """ヒートマップ作成"""
        # 数値カラムのみを選択してヒートマップ作成
        numeric_df = df.select_dtypes(include=[np.number])
        
        if numeric_df.empty:
            fig = go.Figure()
            fig.add_annotation(text="数値データが見つかりません", x=0.5, y=0.5)
        else:
            correlation_matrix = numeric_df.corr()
            
            fig = px.imshow(
                correlation_matrix,
                title=chart.title,
                color_continuous_scale=self.color_schemes.get(chart.color_scheme, 'viridis')
            )
        
        return self.apply_chart_config(fig, chart)
    
    def create_pie_chart(self, df: pd.DataFrame, chart: Chart) -> go.Figure:
        """円グラフ作成"""
        # データの集計
        if chart.y_axis_column:
            values_col = chart.y_axis_column
        else:
            # カウント数を使用
            value_counts = df[chart.x_axis_column].value_counts()
            df = pd.DataFrame({
                chart.x_axis_column: value_counts.index,
                'count': value_counts.values
            })
            values_col = 'count'
        
        fig = px.pie(
            df,
            names=chart.x_axis_column,
            values=values_col,
            title=chart.title,
            color_discrete_sequence=self.color_schemes.get(chart.color_scheme, px.colors.qualitative.Set1)
        )
        
        return self.apply_chart_config(fig, chart)
    
    def create_area_chart(self, df: pd.DataFrame, chart: Chart) -> go.Figure:
        """エリアグラフ作成"""
        fig = px.area(
            df,
            x=chart.x_axis_column,
            y=chart.y_axis_column,
            color=chart.color_column if chart.color_column else None,
            title=chart.title,
            color_discrete_sequence=self.color_schemes.get(chart.color_scheme, px.colors.qualitative.Set1)
        )
        
        return self.apply_chart_config(fig, chart)
    
    def create_3d_scatter(self, df: pd.DataFrame, chart: Chart) -> go.Figure:
        """3D散布図作成"""
        if not chart.z_axis_column:
            raise ValueError("3D散布図にはZ軸カラムが必要です")
        
        fig = px.scatter_3d(
            df,
            x=chart.x_axis_column,
            y=chart.y_axis_column,
            z=chart.z_axis_column,
            color=chart.color_column if chart.color_column else None,
            size=chart.size_column if chart.size_column else None,
            title=chart.title,
            color_continuous_scale=self.color_schemes.get(chart.color_scheme, 'viridis')
        )
        
        return self.apply_chart_config(fig, chart)
    
    def create_violin_plot(self, df: pd.DataFrame, chart: Chart) -> go.Figure:
        """バイオリンプロット作成"""
        fig = px.violin(
            df,
            x=chart.x_axis_column,
            y=chart.y_axis_column,
            color=chart.color_column if chart.color_column else None,
            title=chart.title,
            color_discrete_sequence=self.color_schemes.get(chart.color_scheme, px.colors.qualitative.Set1)
        )
        
        return self.apply_chart_config(fig, chart)
    
    def apply_chart_config(self, fig: go.Figure, chart: Chart) -> go.Figure:
        """グラフ設定を適用"""
        config = chart.chart_config
        
        # レイアウト設定
        layout_updates = {}
        
        if 'width' in config:
            layout_updates['width'] = config['width']
        if 'height' in config:
            layout_updates['height'] = config['height']
        
        if 'x_axis_title' in config:
            layout_updates['xaxis_title'] = config['x_axis_title']
        if 'y_axis_title' in config:
            layout_updates['yaxis_title'] = config['y_axis_title']
        
        if 'show_legend' in config:
            layout_updates['showlegend'] = config['show_legend']
        
        if 'margin' in config:
            layout_updates['margin'] = config['margin']
        
        if layout_updates:
            fig.update_layout(**layout_updates)
        
        # トレース設定
        if 'line_style' in config:
            fig.update_traces(line=config['line_style'])
        
        if 'marker_style' in config:
            fig.update_traces(marker=config['marker_style'])
        
        return fig
    
    def generate_chart(self, chart: Chart) -> Tuple[go.Figure, Dict]:
        """グラフを生成"""
        try:
            df = self.get_dataset_dataframe(chart.dataset, chart.filters)
            
            if df.empty:
                fig = go.Figure()
                fig.add_annotation(text="データが見つかりません", x=0.5, y=0.5)
                return fig, {'success': False, 'error': 'データが見つかりません'}
            
            # グラフタイプに応じてグラフを生成
            chart_methods = {
                'line': self.create_line_chart,
                'bar': self.create_bar_chart,
                'scatter': self.create_scatter_chart,
                'histogram': self.create_histogram,
                'box': self.create_box_plot,
                'heatmap': self.create_heatmap,
                'pie': self.create_pie_chart,
                'area': self.create_area_chart,
                '3d_scatter': self.create_3d_scatter,
                'violin': self.create_violin_plot,
            }
            
            chart_method = chart_methods.get(chart.chart_type)
            if not chart_method:
                raise ValueError(f"サポートされていないグラフタイプ: {chart.chart_type}")
            
            fig = chart_method(df, chart)
            
            return fig, {'success': True, 'data_points': len(df)}
            
        except Exception as e:
            logger.error(f"グラフ生成エラー: {e}")
            fig = go.Figure()
            fig.add_annotation(text=f"エラー: {str(e)}", x=0.5, y=0.5)
            return fig, {'success': False, 'error': str(e)}


class AnalysisEngine:
    """
    データ分析エンジン
    """
    
    def correlation_analysis(self, dataset: Dataset, columns: List[str] = None) -> Dict:
        """相関分析"""
        try:
            chart_generator = ChartGenerator()
            df = chart_generator.get_dataset_dataframe(dataset)
            
            if df.empty:
                return {'error': 'データが見つかりません'}
            
            # 数値カラムのみを選択
            numeric_df = df.select_dtypes(include=[np.number])
            
            if columns:
                numeric_df = numeric_df[columns]
            
            if numeric_df.empty:
                return {'error': '数値データが見つかりません'}
            
            correlation_matrix = numeric_df.corr()
            
            # 強い相関のペアを抽出
            strong_correlations = []
            for i in range(len(correlation_matrix.columns)):
                for j in range(i+1, len(correlation_matrix.columns)):
                    corr_value = correlation_matrix.iloc[i, j]
                    if abs(corr_value) > 0.7:  # 閾値0.7
                        strong_correlations.append({
                            'column1': correlation_matrix.columns[i],
                            'column2': correlation_matrix.columns[j],
                            'correlation': corr_value
                        })
            
            return {
                'correlation_matrix': correlation_matrix.to_dict(),
                'strong_correlations': strong_correlations,
                'columns_analyzed': list(numeric_df.columns)
            }
            
        except Exception as e:
            logger.error(f"相関分析エラー: {e}")
            return {'error': str(e)}
    
    def time_series_analysis(self, dataset: Dataset, time_column: str, value_columns: List[str]) -> Dict:
        """時系列分析"""
        try:
            chart_generator = ChartGenerator()
            df = chart_generator.get_dataset_dataframe(dataset)
            
            if df.empty:
                return {'error': 'データが見つかりません'}
            
            # 時間カラムをdatetimeに変換
            df[time_column] = pd.to_datetime(df[time_column])
            df = df.sort_values(time_column)
            
            analysis_results = {}
            
            for column in value_columns:
                if column in df.columns:
                    series = df[column].dropna()
                    
                    # 基本統計
                    stats = {
                        'mean': series.mean(),
                        'std': series.std(),
                        'min': series.min(),
                        'max': series.max(),
                        'trend': self.calculate_trend(df[time_column], series)
                    }
                    
                    analysis_results[column] = stats
            
            return {
                'time_column': time_column,
                'analysis_results': analysis_results,
                'data_range': {
                    'start': df[time_column].min().isoformat(),
                    'end': df[time_column].max().isoformat()
                }
            }
            
        except Exception as e:
            logger.error(f"時系列分析エラー: {e}")
            return {'error': str(e)}
    
    def calculate_trend(self, time_series: pd.Series, value_series: pd.Series) -> str:
        """トレンド計算"""
        try:
            # 線形回帰で傾きを計算
            from scipy import stats
            time_numeric = (time_series - time_series.min()).dt.total_seconds()
            slope, intercept, r_value, p_value, std_err = stats.linregress(time_numeric, value_series)
            
            if slope > 0.01:
                return "上昇"
            elif slope < -0.01:
                return "下降"
            else:
                return "横ばい"
        except:
            return "不明"
    
    def statistical_summary(self, dataset: Dataset) -> Dict:
        """統計サマリー"""
        try:
            chart_generator = ChartGenerator()
            df = chart_generator.get_dataset_dataframe(dataset)
            
            if df.empty:
                return {'error': 'データが見つかりません'}
            
            summary = {}
            
            # 数値カラムの統計
            numeric_df = df.select_dtypes(include=[np.number])
            if not numeric_df.empty:
                summary['numeric_summary'] = numeric_df.describe().to_dict()
            
            # カテゴリカルカラムの統計
            categorical_df = df.select_dtypes(include=['object'])
            if not categorical_df.empty:
                categorical_summary = {}
                for column in categorical_df.columns:
                    categorical_summary[column] = {
                        'unique_count': df[column].nunique(),
                        'most_frequent': df[column].mode().iloc[0] if not df[column].mode().empty else None,
                        'null_count': df[column].isnull().sum()
                    }
                summary['categorical_summary'] = categorical_summary
            
            summary['total_rows'] = len(df)
            summary['total_columns'] = len(df.columns)
            
            return summary
            
        except Exception as e:
            logger.error(f"統計サマリーエラー: {e}")
            return {'error': str(e)}
