from django.urls import path, include
from rest_framework.routers import DefaultRouter
from . import views

# REST API ルーター
router = DefaultRouter()
router.register(r'datasets', views.DatasetViewSet, basename='dataset')
router.register(r'rawfiles', views.RawDataFileViewSet, basename='rawdatafile')

app_name = 'ingest'

urlpatterns = [
    # Web UI
    path('', views.DatasetListView.as_view(), name='dataset_list'),
    path('upload/', views.upload_csv, name='upload_csv'),
    path('datasets/<int:pk>/', views.DatasetDetailView.as_view(), name='dataset_detail'),
    
    # AJAX APIs
    path('api/datasets/<int:dataset_id>/data/', views.dataset_data_api, name='dataset_data_api'),
    path('api/datasets/<int:dataset_id>/schema/', views.dataset_schema_api, name='dataset_schema_api'),
    
    # REST API
    path('api/', include(router.urls)),
]
