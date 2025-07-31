#!/usr/bin/env python3
"""
R-Lake ログイン/ログアウト機能テストスクリプト
"""
import os
import sys
import django

# Django setup
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'R_Lake.settings')
django.setup()

from django.contrib.auth.models import User
from django.test import Client
from django.urls import reverse

def test_login_logout():
    """ログイン・ログアウト機能のテスト"""
    print("=== R-Lake ログイン/ログアウト機能テスト ===")
    
    # テストクライアントを作成
    client = Client()
    
    # 管理者ユーザーを取得
    try:
        admin_user = User.objects.get(username='admin')
        print(f"✓ Admin user found: {admin_user.username}")
    except User.DoesNotExist:
        print("✗ Admin user not found")
        return False
    
    # ログインページにアクセス
    login_url = reverse('accounts:login')
    response = client.get(login_url)
    print(f"✓ Login page accessible: {response.status_code == 200}")
    
    # ログイン試行
    login_data = {
        'username': 'admin',
        'password': 'admin123'
    }
    response = client.post(login_url, login_data, follow=True)
    
    if response.status_code == 200:
        print(f"✓ Login attempt successful: {response.status_code}")
        print(f"  - Final URL: {response.request['PATH_INFO']}")
        
        # セッションでユーザーが認証されているか確認
        if hasattr(client, 'session') and client.session.get('_auth_user_id'):
            print("✓ User authenticated in session")
        else:
            print("✗ User not authenticated in session")
    else:
        print(f"✗ Login failed: {response.status_code}")
        return False
    
    # ログアウト試行
    logout_url = reverse('accounts:logout')
    response = client.post(logout_url, follow=True)
    
    if response.status_code == 200:
        print(f"✓ Logout successful: {response.status_code}")
        print(f"  - Final URL: {response.request['PATH_INFO']}")
    else:
        print(f"✗ Logout failed: {response.status_code}")
        return False
    
    return True

def test_protected_views():
    """認証が必要なページのアクセステスト"""
    print("\n=== 認証保護されたページのテスト ===")
    
    client = Client()
    
    # 認証なしでプロフィールページにアクセス
    profile_url = reverse('accounts:profile')
    response = client.get(profile_url)
    
    if response.status_code == 302:  # リダイレクト
        print("✓ Profile page redirects unauthenticated users")
        print(f"  - Redirect location: {response.get('Location', 'N/A')}")
    else:
        print(f"✗ Profile page should redirect: {response.status_code}")
        return False
    
    # ログイン後にプロフィールページにアクセス
    login_url = reverse('accounts:login')
    login_data = {
        'username': 'admin',
        'password': 'admin123'
    }
    client.post(login_url, login_data)
    
    response = client.get(profile_url)
    if response.status_code == 200:
        print("✓ Profile page accessible after login")
    else:
        print(f"✗ Profile page not accessible after login: {response.status_code}")
        return False
    
    return True

def main():
    """メイン実行関数"""
    print("R-Lake ログイン/ログアウト機能テスト")
    print("=" * 50)
    
    # テスト実行
    login_test = test_login_logout()
    protected_test = test_protected_views()
    
    # 結果サマリー
    print("\n" + "=" * 50)
    print("TEST RESULTS:")
    print(f"Login/Logout: {'✓ PASS' if login_test else '✗ FAIL'}")
    print(f"Protected Views: {'✓ PASS' if protected_test else '✗ FAIL'}")
    
    if login_test and protected_test:
        print("\n🎉 All authentication tests passed!")
        print("\nTesting Information:")
        print("- Login URL: http://127.0.0.1:8000/accounts/login/")
        print("- Profile URL: http://127.0.0.1:8000/accounts/profile/")
        print("- Credentials: admin / admin123")
    else:
        print("\n❌ Some authentication tests failed.")

if __name__ == "__main__":
    main()
