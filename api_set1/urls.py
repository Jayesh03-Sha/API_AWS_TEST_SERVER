from django.urls import path
from rest_framework_simplejwt.views import TokenRefreshView
from .views import (
    RegisterView,
    CustomTokenObtainPairView,
    UserProfileView,
    ChangePasswordView,
    LogoutView,
    GetQuotesView,
    QuoteNewPageSubmitView,
    QuoteHistoryView,
    QuoteDetailView,
    SelectSchemeView,
    GetPolicyView
)

app_name = 'api_set1'

urlpatterns = [
    # Authentication endpoints
    path('auth/register/', RegisterView.as_view(), name='register'),
    path('auth/login/', CustomTokenObtainPairView.as_view(), name='login'),
    path('auth/token/refresh/', TokenRefreshView.as_view(), name='token_refresh'),
    path('auth/profile/', UserProfileView.as_view(), name='profile'),
    path('auth/change-password/', ChangePasswordView.as_view(), name='change_password'),
    path('auth/logout/', LogoutView.as_view(), name='logout'),
    
    # Quote endpoints
    path('quotes/get-quotes/', GetQuotesView.as_view(), name='get_quotes'),
    path(
        'quotes/get-quote-lists/',
        QuoteNewPageSubmitView.as_view(),
        name='get_quote_lists',
    ),
    path('quotes/history/', QuoteHistoryView.as_view(), name='quote_history'),
    path('quotes/<int:quote_request_id>/', QuoteDetailView.as_view(), name='quote_detail'),
    path('quotes/<int:quote_id>/select-scheme/', SelectSchemeView.as_view(), name='select_scheme'),
    path('quotes/<int:quote_id>/get-policy/', GetPolicyView.as_view(), name='get_policy'),
]
