from django.urls import path

from .views import (
    DataAssetDetailView,
    DataCatalogOverviewView,
    DataDomainDetailView,
    DataSourceDetailView,
)

app_name = "catalog"

urlpatterns = [
    path("", DataCatalogOverviewView.as_view(), name="overview"),
    path("assets/<slug:slug>/", DataAssetDetailView.as_view(), name="asset_detail"),
    path("domains/<slug:slug>/", DataDomainDetailView.as_view(), name="domain_detail"),
    path("sources/<slug:slug>/", DataSourceDetailView.as_view(), name="source_detail"),
]
