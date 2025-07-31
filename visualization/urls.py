from django.urls import path, include
from rest_framework.routers import DefaultRouter
from . import views

# REST API ルーター
router = DefaultRouter()
router.register(r'charts', views.ChartViewSet, basename='chart')
router.register(r'dashboards', views.DashboardViewSet, basename='dashboard')
router.register(r'templates', views.AnalysisTemplateViewSet, basename='analysistemplate')

app_name = 'visualization'

urlpatterns = [
    # Web UI - Charts
    path('', views.ChartListView.as_view(), name='chart_list'),
    path('charts/create/', views.chart_create, name='chart_create'),
    path('charts/<int:pk>/', views.ChartDetailView.as_view(), name='chart_detail'),
    path('charts/<int:pk>/edit/', views.chart_edit, name='chart_edit'),
    
    # Web UI - Dashboards
    path('dashboards/create/', views.dashboard_create, name='dashboard_create'),
    path('dashboards/<int:pk>/', views.DashboardDetailView.as_view(), name='dashboard_detail'),
    
    # Web UI - Analysis
    path('analysis/correlation/<int:dataset_id>/', views.analysis_correlation, name='analysis_correlation'),
    path('analysis/time-series/<int:dataset_id>/', views.analysis_time_series, name='analysis_time_series'),
    
    # AJAX APIs
    path('api/charts/<int:chart_id>/data/', views.chart_data_api, name='chart_data_api'),
    path('api/charts/<int:chart_id>/stats/', views.chart_stats_api, name='chart_stats_api'),
    path('api/datasets/<int:dataset_id>/columns/', views.dataset_columns_api, name='dataset_columns_api'),
    
    # REST API
    path('api/', include(router.urls)),
]
