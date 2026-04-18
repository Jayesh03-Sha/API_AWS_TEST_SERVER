from django.urls import path
from .views import MockInsuranceQuoteView

urlpatterns = [
    path('<str:provider_code>/quotes/', MockInsuranceQuoteView.as_view(), name='mock-provider-quotes'),
    
    # New Standardized API Endpoints for Realtime Simulation
    path('api/v1/User/Auth', MockInsuranceQuoteView.as_view(), {'action': 'auth'}, name='mock-auth'),
    path('api/v1/Insurance/GenerateQuote', MockInsuranceQuoteView.as_view(), {'action': 'quote'}, name='mock-generate-quote'),
    path('api/v1/Insurance/ChooseScheme', MockInsuranceQuoteView.as_view(), {'action': 'choose'}, name='mock-choose-scheme'),
    # NIA / Assuretech API Standardized Endpoints
    path('Api/Auth/Login', MockInsuranceQuoteView.as_view(), {'action': 'nia_auth'}, name='nia-auth'),
    path('Api/Motor/CreateQuote', MockInsuranceQuoteView.as_view(), {'action': 'nia_quote'}, name='nia-create-quote'),
    path('Api/Motor/SaveQuoteWithPlan', MockInsuranceQuoteView.as_view(), {'action': 'nia_save_plan'}, name='nia-save-plan'),
    path('Api/Motor/SaveAddlInfo', MockInsuranceQuoteView.as_view(), {'action': 'nia_save_info'}, name='nia-save-info'),
    path('Api/Motor/SaveDocument', MockInsuranceQuoteView.as_view(), {'action': 'nia_save_doc'}, name='nia-save-doc'),
    path('Api/Motor/ProposalSummary', MockInsuranceQuoteView.as_view(), {'action': 'nia_summary'}, name='nia-summary'),
    path('Api/Motor/ApprovePolicy', MockInsuranceQuoteView.as_view(), {'action': 'nia_approve'}, name='nia-approve'),
]
