from rest_framework import status, viewsets
from rest_framework.decorators import action
from rest_framework.response import Response
from rest_framework.views import APIView
from rest_framework.permissions import AllowAny, IsAuthenticated
from rest_framework_simplejwt.views import TokenObtainPairView, TokenRefreshView
from rest_framework_simplejwt.tokens import RefreshToken
from django.contrib.auth.models import User
import logging
import time
from django.conf import settings
from django.db.utils import OperationalError

from .serializers import (
    RegisterSerializer,
    UserSerializer,
    CustomTokenObtainPairSerializer,
    ChangePasswordSerializer,
    QuoteRequestSerializer,
    QuoteSerializer,
    QuoteResponseSerializer
)
from .models import QuoteRequest, Quote
from .services.aggregator import QuoteAggregator
from .services.comparator import QuoteComparator

logger = logging.getLogger(__name__)

def _validate_motor_payload(additional_details: dict) -> dict:
    """
    Validates motor payload fields required by the DIC GenerateQuote spec.
    Returns an `errors` dict compatible with DRF-style validation errors.
    """
    ad = additional_details or {}
    errors = {}

    # Required keys per DIC PDF/Postman sample. Preserve exact casing where applicable.
    required_keys = [
        "insuredName_en",
        "nationality",
        "nationalId",
        "idExpiryDt",
        "dateOfBirth",
        "gender",
        "emirate",
        "emailAddress",
        "mobileNumber",
        "licenseNo",
        "licenseFmDt",
        "licenseToDt",
        "chassisNumber",
        "regNumber",
        "regDt",
        "plateCode",
        "PlateSource",
        "tcfNumber",
        "ncdYears",
        "trafficTranType",
        "isVehBrandNew",
        "agencyRepairYn",
        "documentLists",
    ]

    for k in required_keys:
        v = ad.get(k, None)
        if k == "documentLists":
            if v is None or not isinstance(v, list):
                errors.setdefault("additional_details", []).append(
                    "Field documentLists is required and must be a list (may be empty)."
                )
            continue
        if v is None or v == "" or v == []:
            errors.setdefault("additional_details", []).append(f"Missing required field: {k}")

    return errors


def _blank_benefits_template() -> dict:
    return {
        "loss_or_damage": False,
        "third_party_liability": "",
        "blood_money": "",
        "fire_theft": False,
        "storm_flood": False,
        "natural_perils": False,
        "repairs": "",
        "emergency_medical": False,
        "personal_belongings": False,
        "oman_cover": False,
        "off_road_cover": False,
        "guaranteed_repairs": False,
        "breakdown_recovery": False,
        "ambulance_cover": "",
        "windscreen_damage": False,
    }


def _blank_optional_covers_template() -> dict:
    return {
        "driver_cover": False,
        "passenger_cover": False,
        "hire_car_benefit": False,
    }


def _benefits_from_list(benefit_strings: list) -> tuple[dict, dict]:
    """
    Best-effort mapping from a list of benefit strings into the required structured fields.
    This is necessarily heuristic unless provider integrations explicitly normalize these keys.
    """
    benefits = _blank_benefits_template()
    optional = _blank_optional_covers_template()

    for raw in benefit_strings or []:
        s = str(raw).lower()
        if "third party" in s or "liability" in s:
            benefits["third_party_liability"] = "Included"
        if "blood" in s:
            benefits["blood_money"] = "Included"
        if "loss" in s or "damage" in s:
            benefits["loss_or_damage"] = True
        if "fire" in s or "theft" in s:
            benefits["fire_theft"] = True
        if "storm" in s or "flood" in s:
            benefits["storm_flood"] = True
        if "natural peril" in s:
            benefits["natural_perils"] = True
        if "agency" in s:
            benefits["repairs"] = "Agency"
            benefits["guaranteed_repairs"] = True
        if "roadside" in s or "breakdown" in s or "towing" in s:
            benefits["breakdown_recovery"] = True
        if "ambulance" in s:
            benefits["ambulance_cover"] = "Included"
        if "windscreen" in s:
            benefits["windscreen_damage"] = True
        if "oman" in s:
            benefits["oman_cover"] = True
        if "off road" in s:
            benefits["off_road_cover"] = True
        if "pab to driver" in s or "driver" in s:
            optional["driver_cover"] = True
        if "passenger" in s:
            optional["passenger_cover"] = True
        if "rent a car" in s or "hire car" in s:
            optional["hire_car_benefit"] = True

    return benefits, optional


class RegisterView(APIView):
    """
    API endpoint for user registration.
    POST /api/auth/register/ - Register a new user
    """
    permission_classes = [AllowAny]

    def post(self, request):
        serializer = RegisterSerializer(data=request.data)
        if serializer.is_valid():
            user = serializer.save()
            refresh = RefreshToken.for_user(user)
            return Response({
                'message': 'User registered successfully',
                'user': UserSerializer(user).data,
                'refresh': str(refresh),
                'access': str(refresh.access_token),
            }, status=status.HTTP_201_CREATED)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)


class CustomTokenObtainPairView(TokenObtainPairView):
    """
    API endpoint for user login with JWT tokens.
    POST /api/auth/login/ - Login with username and password
    Returns access and refresh tokens along with user details
    """
    serializer_class = CustomTokenObtainPairSerializer
    permission_classes = [AllowAny]


class UserProfileView(APIView):
    """
    API endpoint for retrieving current user profile.
    GET /api/auth/profile/ - Get current user profile
    PUT /api/auth/profile/ - Update current user profile
    """
    permission_classes = [IsAuthenticated]

    def get(self, request):
        """Get current user profile"""
        serializer = UserSerializer(request.user)
        return Response(serializer.data, status=status.HTTP_200_OK)

    def put(self, request):
        """Update current user profile"""
        user = request.user
        data = request.data
        
        # Update user fields
        if 'email' in data:
            if User.objects.filter(email=data['email']).exclude(id=user.id).exists():
                return Response(
                    {'email': 'Email already in use.'},
                    status=status.HTTP_400_BAD_REQUEST
                )
            user.email = data['email']
        
        if 'first_name' in data:
            user.first_name = data['first_name']
        
        if 'last_name' in data:
            user.last_name = data['last_name']
        
        user.save()

        # Update profile fields
        if hasattr(user, 'profile'):
            profile = user.profile
            if 'phone_number' in data:
                profile.phone_number = data['phone_number']
            if 'organization' in data:
                profile.organization = data['organization']
            profile.save()

        serializer = UserSerializer(user)
        return Response({
            'message': 'Profile updated successfully',
            'user': serializer.data
        }, status=status.HTTP_200_OK)


class ChangePasswordView(APIView):
    """
    API endpoint for changing password.
    POST /api/auth/change-password/ - Change password (requires old password)
    """
    permission_classes = [IsAuthenticated]

    def post(self, request):
        serializer = ChangePasswordSerializer(data=request.data)
        if serializer.is_valid():
            user = request.user
            
            # Check old password
            if not user.check_password(serializer.data['old_password']):
                return Response(
                    {'old_password': 'Wrong password.'},
                    status=status.HTTP_400_BAD_REQUEST
                )
            
            # Set new password
            user.set_password(serializer.data['new_password'])
            user.save()
            
            return Response(
                {'message': 'Password changed successfully'},
                status=status.HTTP_200_OK
            )
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)


class LogoutView(APIView):
    """
    API endpoint for user logout.
    POST /api/auth/logout/ - Logout user (invalidate refresh token)
    """
    permission_classes = [IsAuthenticated]

    def post(self, request):
        try:
            refresh_token = request.data.get('refresh')
            if refresh_token:
                token = RefreshToken(refresh_token)
                token.blacklist()
            return Response(
                {'message': 'Logout successful'},
                status=status.HTTP_200_OK
            )
        except Exception as e:
            return Response(
                {'error': str(e)},
                status=status.HTTP_400_BAD_REQUEST
            )


# ============================================================================
# Quote Management Views
# ============================================================================

class GetQuotesView(APIView):
    """
    API endpoint for getting insurance quotes from multiple providers.
    
    POST /api/quotes/get-quotes/
    
    Request body:
    {
        "insurance_type": "health",
        "age": 30,
        "sum_insured": 80000,
        "city": "Dubai",
        "members": 1,
        "additional_details": {}
    }
    
    Returns: Best quote + all quoted from all providers with comparison scores
    """
    permission_classes = [IsAuthenticated]
    
    def post(self, request):
        """Get quotes from multiple insurance providers"""
        try:
            # Validate request data
            serializer = QuoteRequestSerializer(data=request.data)
            if not serializer.is_valid():
                return Response(
                    {'errors': serializer.errors},
                    status=status.HTTP_400_BAD_REQUEST
                )

            # If motor, validate required real motor fields early (before fanout).
            insurance_type = serializer.validated_data.get("insurance_type")
            additional_details = serializer.validated_data.get("additional_details") or {}
            if insurance_type == "motor":
                motor_errors = _validate_motor_payload(additional_details)
                if motor_errors:
                    return Response({"errors": motor_errors}, status=status.HTTP_400_BAD_REQUEST)
            
            # Save quote request (SQLite can transiently lock under concurrent access)
            quote_request = None
            for attempt in range(3):
                try:
                    quote_request = serializer.save(user=request.user)
                    break
                except OperationalError as e:
                    if "database is locked" in str(e).lower() and attempt < 2:
                        time.sleep(0.2 * (attempt + 1))
                        continue
                    raise
            if quote_request is None:
                raise OperationalError("Failed to save QuoteRequest")
            
            # Prepare data for aggregator
            quote_data = {
                'age': quote_request.age,
                'sum_insured': float(quote_request.sum_insured),
                'city': quote_request.city,
                'members': quote_request.members,
                'insurance_type': quote_request.insurance_type,
                # Provider-specific fields (e.g. motor fields for DIC) should be supplied here.
                'additional_details': quote_request.additional_details or {},
            }
            
            logger.info(f"Fetching quotes for user {request.user.username} - {quote_request.insurance_type}")
            
            # Get quotes from all providers in parallel
            aggregator = QuoteAggregator()
            provider_quotes = aggregator.get_all_quotes(quote_data, parallel=True)
            
            if not provider_quotes:
                return Response(
                    {'error': 'No quotes available from providers. Please try again later.'},
                    status=status.HTTP_503_SERVICE_UNAVAILABLE
                )

            # Split successful quotes vs provider errors (so failed providers don't get recommended).
            successful_quotes = []
            provider_errors = []
            for q in provider_quotes:
                if not isinstance(q, dict):
                    continue
                if q.get("success") is False:
                    provider_errors.append(q)
                    continue
                successful_quotes.append(q)

            if not successful_quotes:
                # All providers failed; return a clear error payload.
                return Response(
                    {
                        "error": "No successful quotes returned from providers.",
                        "provider_errors": provider_errors,
                    },
                    status=status.HTTP_503_SERVICE_UNAVAILABLE,
                )
            
            # Compare quotes and get best option
            comparator = QuoteComparator()
            best_quote, sorted_quotes = comparator.compare_quotes(successful_quotes)
            
            # Save quotes to database
            for quote_data_item in sorted_quotes:
                quote_obj = Quote.objects.create(
                    quote_request=quote_request,
                    provider=quote_data_item.get('provider', ''),
                    premium=quote_data_item.get('premium', 0),
                    coverage=quote_data_item.get('coverage', 0),
                    benefits=quote_data_item.get('benefits', []),
                    comparison_score=quote_data_item.get('score', 0),
                    scoring_breakdown=quote_data_item.get('scoring_breakdown', {}),
                    competitive_advantages=quote_data_item.get('competitive_advantages', []),
                    verdict=quote_data_item.get('verdict', ''),
                    is_best=(quote_data_item == best_quote),
                    provider_metadata={
                        'reference_no': quote_data_item.get('reference_no'),
                        'prod_code': quote_data_item.get('prod_code'),
                        'plan_name': quote_data_item.get('plan_name'),
                        'provider_id': quote_data_item.get('provider_id'),
                        # Pricing + UI contract (if provider supplies structured fields)
                        'base_price': quote_data_item.get('base_price'),
                        'vehicle_details': quote_data_item.get('vehicle_details'),
                        'benefits_struct': quote_data_item.get('benefits_struct'),
                        'optional_covers_struct': quote_data_item.get('optional_covers_struct'),
                        # Preserve any provider-returned raw response for debugging.
                        'raw_response': quote_data_item.get('raw_response'),
                    }
                )
            
            # Table order: cheapest premium first (matches comparison rule).
            db_quotes = Quote.objects.filter(quote_request=quote_request).order_by("premium", "id")
            db_best_quote = Quote.objects.filter(quote_request=quote_request, is_best=True).first()

            # Prepare response in requested client contract
            customer_name = (
                f"{request.user.first_name} {request.user.last_name}".strip()
                or request.user.username
                or ""
            )

            providers_payload = []
            for q in db_quotes:
                md = q.provider_metadata or {}
                benefits_struct = md.get("benefits_struct")
                optional_struct = md.get("optional_covers_struct")
                vehicle_details = md.get("vehicle_details")
                if not isinstance(benefits_struct, dict) or not isinstance(optional_struct, dict):
                    benefits_struct, optional_struct = _benefits_from_list(q.benefits)
                if not isinstance(vehicle_details, dict):
                    vehicle_details = {
                        "excess": "TBA",
                        "ancillary_excess": "TBA",
                        "vehicle_value": "N/A",
                    }

                badge = ""
                if q.is_best:
                    badge = "Best Value"

                # Provide a stable "buy_now_url" that the UI can call to proceed.
                buy_now_url = request.build_absolute_uri(f"/api/quotes/{q.id}/select-scheme/")

                providers_payload.append(
                    {
                        "provider_name": q.provider,
                        "logo": "",
                        "plan_name": (q.provider_metadata or {}).get("plan_name")
                        or (q.provider_metadata or {}).get("prod_code")
                        or "",
                        "premium": float(q.premium),
                        "base_price": float(md.get("base_price") or q.premium),
                        "vat": 0,
                        "currency": "AED",
                        "badge": badge,
                        "buy_now_url": buy_now_url,
                        "vehicle_details": vehicle_details,
                        "benefits": benefits_struct,
                        "optional_covers": optional_struct,
                        "error": "",
                    }
                )

            # Include provider errors in the response too (so the client sees failures clearly).
            for err in provider_errors:
                providers_payload.append(
                    {
                        "provider_name": err.get("provider") or err.get("provider_name") or "",
                        "logo": "",
                        "plan_name": err.get("plan_name") or "",
                        "premium": 0,
                        "base_price": 0,
                        "vat": 0,
                        "currency": "AED",
                        "badge": "Error",
                        "buy_now_url": "",
                        "vehicle_details": {
                            "excess": "TBA",
                            "ancillary_excess": "TBA",
                            "vehicle_value": "N/A",
                        },
                        "benefits": _blank_benefits_template(),
                        "optional_covers": _blank_optional_covers_template(),
                        "error": err.get("failure_reason") or "Provider request failed",
                    }
                )

            product_label = (
                "Car Insurance"
                if (quote_request.insurance_type or "").lower() == "motor"
                else quote_request.insurance_type
            )
            response_data = {
                "customer": {
                    "name": customer_name,
                    "product": product_label,
                    "created_at": quote_request.created_at.isoformat() if quote_request.created_at else "",
                },
                "providers": providers_payload,
                "recommended_provider": (db_best_quote.provider if db_best_quote else ""),
            }

            return Response(response_data, status=status.HTTP_200_OK)
            
        except Exception as e:
            logger.error(f"Error in GetQuotesView: {str(e)}", exc_info=True)
            err_body = {
                "error": "An error occurred while fetching quotes. Please try again.",
            }
            if settings.DEBUG:
                err_body["detail"] = str(e)
            return Response(err_body, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class QuoteNewPageSubmitView(APIView):
    """
    Same behavior as POST /quotes/new/ (HTML): parses the same fields, builds the
    GetQuotesView payload, and returns the same JSON as /api/quotes/get-quotes/.

    POST /api/quotes/get-quote-lists/

    - **application/json**: flat object with the same keys as the HTML form, optional
      ``documentLists`` array of ``{ "code", "name", "type", "base64" }``.
    - **multipart/form-data** or **x-www-form-urlencoded**: same text fields as the HTML
      form; files become optimized base64 ``documentLists``. Easiest: upload each document
      under a dedicated key — ``emirate_id_front``, ``emirate_id_back``,
      ``driving_license_front``, ``driving_license_back``, ``mulkiya_id_front``,
      ``mulkiya_id_back``, ``bank_lpo`` (codes are inferred from the key).
      Alternatively use ``document_file[]`` + ``document_code[]`` / ``document_name[]``, or
      repeated ``document_file`` + ``document_code`` / ``document_name``, or indexed
      ``document_{i}_file`` + optional ``document_{i}_code`` / name / type.
    """
    permission_classes = [IsAuthenticated]

    def post(self, request):
        from rest_framework.test import APIRequestFactory, force_authenticate

        from ui.quote_payload import build_get_quotes_payload, parse_quote_new_request

        try:
            form, document_lists = parse_quote_new_request(request)
            payload = build_get_quotes_payload(form, document_lists)
        except Exception as e:
            logger.exception("QuoteNewPageSubmitView: failed to build payload")
            return Response(
                {"error": "Failed to build quote payload", "detail": str(e)},
                status=status.HTTP_400_BAD_REQUEST,
            )

        factory = APIRequestFactory()
        drf_req = factory.post(
            "/api/quotes/get-quotes/",
            payload,
            format="json",
            HTTP_HOST=request.get_host(),
            secure=request.is_secure(),
        )
        force_authenticate(drf_req, user=request.user)
        drf_resp = GetQuotesView.as_view()(drf_req)
        out = getattr(drf_resp, "data", None)
        if out is None:
            out = {"detail": "Unexpected empty response from GetQuotesView"}
        return Response(out, status=drf_resp.status_code)


class QuoteHistoryView(APIView):
    """
    API endpoint for retrieving user's quote history.
    
    GET /api/quotes/history/ - Get all quotes requested by the user
    """
    permission_classes = [IsAuthenticated]
    
    def get(self, request):
        """Get user's quote request history"""
        try:
            # Get all quote requests for the user
            quote_requests = QuoteRequest.objects.filter(user=request.user).prefetch_related('quotes')
            
            # Prepare response with quote details
            history = []
            for quote_request in quote_requests:
                quotes = quote_request.quotes.all()
                best_quote = quotes.filter(is_best=True).first()
                
                history.append({
                    'id': quote_request.id,
                    'insurance_type': quote_request.insurance_type,
                    'age': quote_request.age,
                    'sum_insured': str(quote_request.sum_insured),
                    'city': quote_request.city,
                    'members': quote_request.members,
                    'quotes_count': quotes.count(),
                    'best_quote': QuoteSerializer(best_quote).data if best_quote else None,
                    'all_quotes': QuoteSerializer(quotes.order_by('-comparison_score'), many=True).data,
                    'created_at': quote_request.created_at
                })
            
            return Response({
                'count': len(history),
                'history': history
            }, status=status.HTTP_200_OK)
            
        except Exception as e:
            logger.error(f"Error in QuoteHistoryView: {str(e)}", exc_info=True)
            return Response(
                {'error': 'An error occurred while retrieving quote history.'},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class QuoteDetailView(APIView):
    """
    API endpoint for retrieving a specific quote request with all details.
    
    GET /api/quotes/{quote_request_id}/ - Get specific quote request with all provider quotes
    """
    permission_classes = [IsAuthenticated]
    
    def get(self, request, quote_request_id):
        """Get detailed information about a specific quote request"""
        try:
            # Get quote request, ensure it belongs to the user
            quote_request = QuoteRequest.objects.get(
                id=quote_request_id,
                user=request.user
            )
            
            # Get all quotes for this request
            quotes = Quote.objects.filter(quote_request=quote_request).order_by('-comparison_score')
            
            if not quotes.exists():
                return Response(
                    {'error': 'No quotes found for this request.'},
                    status=status.HTTP_404_NOT_FOUND
                )
            
            best_quote = quotes.filter(is_best=True).first()
            
            return Response({
                'quote_request': QuoteRequestSerializer(quote_request).data,
                'best_quote': QuoteSerializer(best_quote).data if best_quote else None,
                'all_quotes': QuoteSerializer(quotes, many=True).data,
                'comparison_summary': {
                    'count': quotes.count(),
                    'avg_premium': round(sum(float(q.premium) for q in quotes) / quotes.count(), 2),
                    'min_premium': round(min(float(q.premium) for q in quotes), 2),
                    'max_premium': round(max(float(q.premium) for q in quotes), 2),
                    'premium_range': round(max(float(q.premium) for q in quotes) - min(float(q.premium) for q in quotes), 2),
                    'avg_score': round(sum(float(q.comparison_score) for q in quotes) / quotes.count(), 2),
                }
            }, status=status.HTTP_200_OK)
            
        except QuoteRequest.DoesNotExist:
            return Response(
                {'error': 'Quote request not found.'},
                status=status.HTTP_404_NOT_FOUND
            )
        except Exception as e:
            logger.error(f"Error in QuoteDetailView: {str(e)}", exc_info=True)
            return Response(
                {'error': 'An error occurred while retrieving quote details.'},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


def get_provider_instance(provider_name):
    import importlib
    from .models import InsuranceProvider
    provider_data = InsuranceProvider.objects.filter(name=provider_name, is_active=True).first()
    if not provider_data:
        return None
    try:
        module_path, class_name = provider_data.provider_class_path.rsplit('.', 1)
        module = importlib.import_module(module_path)
        provider_class = getattr(module, class_name)
        instance = provider_class(
            api_key=provider_data.api_key,
            base_url=provider_data.api_base_url
        )
        instance.provider_name = provider_name
        return instance
    except Exception as e:
        logger.error(f"Error loading provider {provider_name}: {str(e)}")
        return None


class SelectSchemeView(APIView):
    """
    Step 3: Select Scheme for a quote.
    POST /api/quotes/{quote_id}/select-scheme/
    """
    permission_classes = [IsAuthenticated]

    def post(self, request, quote_id):
        from django.shortcuts import get_object_or_404
        quote = get_object_or_404(Quote, id=quote_id)
        
        provider_instance = get_provider_instance(quote.provider)
        if not provider_instance:
            return Response({'error': 'Provider not available'}, status=status.HTTP_400_BAD_REQUEST)
        
        provider_id = quote.provider_metadata.get('provider_id')
        ref_no = quote.provider_metadata.get('reference_no')
        prod_code = quote.provider_metadata.get('prod_code')
        
        if provider_id == 'nia_online':
            covers = request.data.get('covers', [])
            result = provider_instance.save_quote_with_plan(ref_no, prod_code, covers)
            if result:
                provider_instance.save_additional_info({"ReferenceNo": ref_no})
                summary = provider_instance.get_proposal_summary(ref_no)
                return Response({
                    "message": "Scheme selected successfully",
                    "quotation_no": result,
                    "summary": summary,
                    "payment_url": f"https://mock-payment-gateway.com/pay/{result}"
                })
        elif provider_id == 'dic_broker_uae':
            covers = request.data.get('covers', {})
            result = provider_instance.choose_scheme(prod_code, covers)
            if result:
                return Response({
                    "message": "Scheme selected successfully",
                    "payment_url": f"https://mock-payment-gateway.com/dic-pay/{prod_code}"
                })
                
        return Response({'error': 'Failed to select scheme'}, status=status.HTTP_400_BAD_REQUEST)


class GetPolicyView(APIView):
    """
    Step 5: Process payment and get final policy.
    POST /api/quotes/{quote_id}/get-policy/
    """
    permission_classes = [IsAuthenticated]

    def post(self, request, quote_id):
        from django.shortcuts import get_object_or_404
        quote = get_object_or_404(Quote, id=quote_id)
        provider_instance = get_provider_instance(quote.provider)
        
        if not provider_instance:
            return Response({'error': 'Provider not available'}, status=status.HTTP_400_BAD_REQUEST)
            
        provider_id = quote.provider_metadata.get('provider_id')
        ref_no = quote.provider_metadata.get('reference_no')
        quotation_no = request.data.get('quotation_no', '')

        if provider_id == 'nia_online':
            policy_no = provider_instance.approve_policy(ref_no)
            if policy_no:
                return Response({
                    "message": "Policy generated successfully",
                    "policy_no": policy_no,
                    "status": "Active"
                })
        elif provider_id == 'dic_broker_uae':
            policy_info = provider_instance.get_policy(quotation_no)
            if policy_info:
                return Response({
                    "message": "Policy generated successfully",
                    "policy_info": policy_info,
                    "status": "Active"
                })

        return Response({'error': 'Failed to generate policy'}, status=status.HTTP_400_BAD_REQUEST)
