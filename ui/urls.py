# from django.urls import path
# from . import views

# urlpatterns = [
#     path('', views.home, name='home'),
#     path('dashboard/', views.admin_dashboard, name='admin_dashboard'),
#     path('quotes/new/', views.quote_new, name='quote_new'),
#     path('quotes/bayanaty/vehicle-details/', views.bayanaty_vehicle_details, name='bayanaty_vehicle_details'),
#     path('dashboard/leads/', views.leads_list, name='leads_list'),
#     path('dashboard/leads/<int:lead_id>/', views.lead_detail, name='lead_detail'),
#     path('dashboard/deals/', views.deals_list, name='deals_list'),
#     path('dashboard/deals/<int:deal_id>/', views.deal_detail, name='deal_detail'),
#     path('dashboard/transactions/', views.transactions_list, name='transactions_list'),
#     path('dashboard/transactions/<int:txn_id>/', views.transaction_detail, name='transaction_detail'),
#     path('dashboard/customers/', views.customers_list, name='customers_list'),
#     path('dashboard/quotes/<int:quote_request_id>/proposal/', views.quote_proposal, name='quote_proposal'),
# ] 


from django.urls import path
from . import views

urlpatterns = [
    path('', views.quote_new, name='home'),  # 👈 Default now points to Get Quotes

    path('dashboard/', views.admin_dashboard, name='admin_dashboard'),

    path('quotes/new/', views.quote_new, name='quote_new'),
    path('quotes/bayanaty/vehicle-details/', views.bayanaty_vehicle_details, name='bayanaty_vehicle_details'),

    path('dashboard/leads/', views.leads_list, name='leads_list'),
    path('dashboard/leads/<int:lead_id>/', views.lead_detail, name='lead_detail'),

    path('dashboard/deals/', views.deals_list, name='deals_list'),
    path('dashboard/deals/<int:deal_id>/', views.deal_detail, name='deal_detail'),

    path('dashboard/transactions/', views.transactions_list, name='transactions_list'),
    path('dashboard/transactions/<int:txn_id>/', views.transaction_detail, name='transaction_detail'),

    path('dashboard/customers/', views.customers_list, name='customers_list'),

    path('dashboard/quotes/<int:quote_request_id>/proposal/', views.quote_proposal, name='quote_proposal'),
]