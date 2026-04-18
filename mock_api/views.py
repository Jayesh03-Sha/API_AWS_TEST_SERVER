import os
import json
from django.conf import settings
from django.http import JsonResponse, HttpResponse
from rest_framework.views import APIView
from rest_framework.permissions import AllowAny
from rest_framework import status

class MockInsuranceQuoteView(APIView):
    """
    Mock API to simulate external insurance provider responses.
    Supports both JSON and XML formats based on the 'Accept' header or 'format' query param.
    Now supports the DIN/DIC multi-step flow.
    """
    permission_classes = [AllowAny]

    def post(self, request, provider_code=None, action=None):
        requested_format = request.query_params.get('format', 'json').lower()
        
        # DIN / DIC Flow
        if action == 'auth':
            return self._handle_auth(request)
        elif action == 'quote':
            return self._handle_generate_quote(request)
        elif action == 'choose':
            return self._handle_choose_scheme(request)
        elif action == 'policy':
            return self._handle_get_policy(request)
        
        # NIA / Assuretech Flow
        elif action == 'nia_auth':
            return self._handle_nia_auth(request)
        elif action == 'nia_quote':
            return self._handle_nia_quote(request)
        elif action == 'nia_save_plan':
            return self._handle_nia_save_plan(request)
        elif action == 'nia_save_info':
            return self._handle_nia_save_info(request)
        elif action == 'nia_save_doc':
            return self._handle_nia_save_doc(request)
        elif action == 'nia_summary':
            return self._handle_nia_summary(request)
        elif action == 'nia_approve':
            return self._handle_nia_approve(request)

        # Fallback to existing logic for basic quote requests
        if provider_code:
            return self._handle_legacy_quote(request, provider_code, requested_format)
        
        return JsonResponse({"error": "Action or Provider Code required"}, status=status.HTTP_400_BAD_REQUEST)

    def get(self, request, provider_code=None, action=None):
        if action == 'policy':
            return self._handle_get_policy(request)
        return JsonResponse({"error": "Unsupported method"}, status=status.HTTP_405_METHOD_NOT_ALLOWED)

    def _handle_auth(self, request):
        username = request.data.get('userName')
        password = request.data.get('password')
        if username == "MOTOR_USER_001" and password == "123456":
            return JsonResponse({"status": 1, "data": "MOCK_JWT_TOKEN_123456789"}, status=status.HTTP_200_OK)
        return JsonResponse({"status": 0, "error": "Invalid login"}, status=status.HTTP_401_UNAUTHORIZED)

    def _handle_generate_quote(self, request):
        # Path to dic_motor_plans.json
        file_path = os.path.join(settings.BASE_DIR.parent, 'API_MOCK_DATA', 'JSON', 'dic_motor_plans.json')
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Convert internal format to the "realtime" format expected by the user
            realtime_plans = []
            for plan in data.get('plans', []):
                realtime_plans.append({
                    "prodCode": plan.get('prodCode', '1001'),
                    "prodName": plan.get('plan_name'),
                    "sumInsured": plan.get('sumInsured', plan.get('coverage')),
                    "covers": {
                        "mandatory": plan.get('benefits', [])[:2],
                        "optional": plan.get('benefits', [])[2:]
                    }
                })
            return JsonResponse(realtime_plans, safe=False, status=status.HTTP_200_OK)
        except Exception as e:
            return JsonResponse({"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    def _handle_choose_scheme(self, request):
        prod_code = request.data.get('prodCode')
        return JsonResponse({
            "quotationNo": f"QUO-{prod_code}-999",
            "grossPremium": 1100,
            "vat": 55,
            "netToCustomer": 1155,
            "paymentUrl": "http://localhost:8000/mock-api/payment-gateway/?q=QUO-999"
        }, status=status.HTTP_200_OK)

    def _handle_get_policy(self, request):
        return JsonResponse({
            "polNo": "P/13/1001/25/020/00001",
            "polStatus": "APPROVED",
            "paymentStatus": "S",
            "grossPremium": "1955",
            "documents": "BASE64_MOCK_PDF_CONTENT"
        }, status=status.HTTP_200_OK)

    # =========================================================================
    # NIA / ASSURETECH MOCK HANDLERS
    # =========================================================================

    def _handle_nia_auth(self, request):
        if request.data.get('username') == "sabir.a@nia-dubai.com":
            return JsonResponse({
                "Status": 1,
                "StatusMessage": "Login successful",
                "Data": "MOCK_NIA_JW_TOKEN_EYJHB..."
            }, status=status.HTTP_200_OK)
        return JsonResponse({"Status": 0, "StatusMessage": "Login failed"}, status=status.HTTP_401_UNAUTHORIZED)

    def _handle_nia_quote(self, request):
        return JsonResponse({
            "Status": 1,
            "Data": {
                "ReferenceNo": "R/NIA-02746",
                "SchemeCode": "1000",
                "PlanDetails": [
                    {
                        "Code": "1001", "Name": "Motor Comprehensive – Agency",
                        "Covers": [{"Code": "100001", "Description": "Loss Cover", "CoverPremFc": 1800.0}]
                    },
                    {
                        "Code": "1501", "Name": "Third Party Liability",
                        "Covers": [{"Code": "100007", "Description": "Third Party Liability", "CoverPremFc": 800.0}]
                    }
                ]
            }
        }, status=status.HTTP_200_OK)

    def _handle_nia_save_plan(self, request):
        return JsonResponse({
            "Status": 1,
            "Data": "Q/NIA-162428"
        }, status=status.HTTP_200_OK)

    def _handle_nia_save_info(self, request):
        return JsonResponse({
            "Status": 1,
            "StatusMessage": "Info saved successfully",
            "Data": "Q/NIA-162428"
        }, status=status.HTTP_200_OK)

    def _handle_nia_save_doc(self, request):
        return JsonResponse({
            "Status": 1,
            "StatusMessage": "Successfully uploaded documents"
        }, status=status.HTTP_200_OK)

    def _handle_nia_summary(self, request):
        return JsonResponse({
            "Status": 1,
            "Data": {
                "Policy": {"PolRefNo": request.data.get('PolRefNo'), "PolAssrName": "ADITHI B"},
                "Vehicle": {"VehChassisNo": "1N4SL..."}
            }
        }, status=status.HTTP_200_OK)

    def _handle_nia_approve(self, request):
        return JsonResponse({
            "Status": 1,
            "Data": {"PolicyNo": "P-NIA-2022-17082"}
        }, status=status.HTTP_200_OK)

    def _handle_legacy_quote(self, request, provider_code, requested_format):
        # Original logic preserved
        insurance_type = request.data.get('insurance_type', 'health').lower()
        provider_map = {
            'dic-broker-uae': 'dic', 'icici-uae': 'icici', 'qic-uae': 'qic',
            'dic': 'dic', 'icici': 'icici', 'qic': 'qic',
        }
        base_name = provider_map.get(provider_code.lower())
        if not base_name:
            return JsonResponse({"error": f"Provider '{provider_code}' not found"}, status=status.HTTP_404_NOT_FOUND)

        filename = f"{base_name}_{insurance_type}.{requested_format}"
        file_path = os.path.join(settings.BASE_DIR.parent, 'API_MOCK_DATA', requested_format.upper(), filename)

        if not os.path.exists(file_path):
            return JsonResponse({"error": f"Mock data file not found"}, status=status.HTTP_404_NOT_FOUND)

        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
            if requested_format == 'json':
                return JsonResponse(json.loads(content), status=status.HTTP_200_OK)
            return HttpResponse(content, content_type='application/xml', status=status.HTTP_200_OK)
