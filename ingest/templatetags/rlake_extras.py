from django import template
from django.utils.safestring import mark_safe
import json

register = template.Library()


@register.filter
def get_item(dictionary, key):
    """辞書から指定したキーの値を取得"""
    if isinstance(dictionary, dict):
        return dictionary.get(key)
    return None


@register.filter
def mul(value, arg):
    """数値の掛け算"""
    try:
        return float(value) * float(arg)
    except (ValueError, TypeError):
        return 0


@register.filter
def div(value, arg):
    """数値の割り算"""
    try:
        if float(arg) == 0:
            return 0
        return float(value) / float(arg)
    except (ValueError, TypeError):
        return 0


@register.filter
def percentage(value):
    """パーセンテージ表示"""
    try:
        return f"{float(value):.1f}%"
    except (ValueError, TypeError):
        return "0.0%"


@register.filter
def json_pretty(value):
    """JSON をきれいに整形"""
    try:
        if isinstance(value, str):
            value = json.loads(value)
        return mark_safe(f"<pre>{json.dumps(value, indent=2, ensure_ascii=False)}</pre>")
    except (json.JSONDecodeError, TypeError):
        return str(value)


@register.filter
def data_type_badge(data_type):
    """データ型に応じたバッジクラスを返す"""
    badge_classes = {
        'INTEGER': 'bg-info',
        'FLOAT': 'bg-success',
        'STRING': 'bg-warning',
        'DATETIME': 'bg-danger',
        'BOOLEAN': 'bg-dark',
    }
    return badge_classes.get(data_type, 'bg-secondary')


@register.filter
def quality_score_class(score):
    """品質スコアに応じたCSSクラスを返す"""
    try:
        score = float(score)
        if score >= 90:
            return 'quality-excellent'
        elif score >= 75:
            return 'quality-good'
        elif score >= 50:
            return 'quality-fair'
        else:
            return 'quality-poor'
    except (ValueError, TypeError):
        return 'quality-poor'


@register.filter
def file_size_format(bytes_value):
    """ファイルサイズを人間が読みやすい形式に変換"""
    try:
        bytes_value = int(bytes_value)
        
        if bytes_value < 1024:
            return f"{bytes_value} B"
        elif bytes_value < 1024 * 1024:
            return f"{bytes_value / 1024:.1f} KB"
        elif bytes_value < 1024 * 1024 * 1024:
            return f"{bytes_value / (1024 * 1024):.1f} MB"
        else:
            return f"{bytes_value / (1024 * 1024 * 1024):.1f} GB"
    except (ValueError, TypeError):
        return "0 B"


@register.filter
def chart_type_icon(chart_type):
    """グラフタイプに応じたアイコンを返す"""
    icons = {
        'line': 'fas fa-chart-line',
        'bar': 'fas fa-chart-bar',
        'scatter': 'fas fa-braille',
        'histogram': 'fas fa-chart-column',
        'box': 'fas fa-box',
        'pie': 'fas fa-chart-pie',
        'area': 'fas fa-chart-area',
        'heatmap': 'fas fa-th',
        'violin': 'fas fa-music',
        '3d_scatter': 'fas fa-cube',
    }
    return icons.get(chart_type, 'fas fa-chart-simple')


@register.simple_tag
def correlation_class(correlation_value):
    """相関値に応じたCSSクラスを返す"""
    try:
        value = abs(float(correlation_value))
        if value >= 0.8:
            return 'correlation-strong-positive' if correlation_value > 0 else 'correlation-strong-negative'
        elif value >= 0.5:
            return 'correlation-moderate-positive' if correlation_value > 0 else 'correlation-moderate-negative'
        else:
            return 'correlation-weak'
    except (ValueError, TypeError):
        return 'correlation-weak'


@register.simple_tag
def trend_class(trend):
    """トレンドに応じたCSSクラスを返す"""
    trend_classes = {
        '上昇': 'trend-up',
        '下降': 'trend-down',
        '横ばい': 'trend-stable',
    }
    return trend_classes.get(trend, 'trend-stable')


@register.inclusion_tag('ingest/widgets/quality_indicator.html')
def quality_indicator(quality_report):
    """品質指標ウィジェット"""
    if quality_report and quality_report.total_records > 0:
        quality_score = (quality_report.valid_records / quality_report.total_records) * 100
        return {
            'quality_score': quality_score,
            'total_records': quality_report.total_records,
            'valid_records': quality_report.valid_records,
            'invalid_records': quality_report.invalid_records,
        }
    return {
        'quality_score': 0,
        'total_records': 0,
        'valid_records': 0,
        'invalid_records': 0,
    }
