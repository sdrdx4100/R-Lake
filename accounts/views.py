from django.shortcuts import render, redirect
from django.contrib.auth import login, logout
from django.contrib.auth.decorators import login_required
from django.contrib import messages
from django.views.decorators.http import require_http_methods
from django.contrib.auth.views import LoginView, LogoutView

class CustomLoginView(LoginView):
    template_name = 'accounts/login.html'
    redirect_authenticated_user = True
    
    def get_success_url(self):
        # ログイン成功後のリダイレクト先を明確に指定
        next_url = self.get_redirect_url()
        if next_url:
            return next_url
        return '/ingest/'  # デフォルトはデータセット一覧
    
    def form_valid(self, form):
        messages.success(self.request, f'ようこそ、{form.get_user().username}さん！')
        return super().form_valid(form)

class CustomLogoutView(LogoutView):
    template_name = 'accounts/logout.html'
    
    def dispatch(self, request, *args, **kwargs):
        if request.user.is_authenticated:
            messages.info(request, f'{request.user.username}さん、ログアウトしました。')
        return super().dispatch(request, *args, **kwargs)

@login_required
def profile_view(request):
    return render(request, 'accounts/profile.html')
