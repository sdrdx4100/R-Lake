from django.urls import path
from django.contrib.auth import views as auth_views
from django.views.generic import TemplateView
from . import views

app_name = 'accounts'

urlpatterns = [
    # 認証関連
    path('login/', views.CustomLoginView.as_view(), name='login'),
    path('logout/', views.CustomLogoutView.as_view(), name='logout'),
    path('signup/', TemplateView.as_view(template_name='accounts/signup.html'), name='signup'),
    
    # プロフィール関連
    path('profile/', views.profile_view, name='profile'),
]
