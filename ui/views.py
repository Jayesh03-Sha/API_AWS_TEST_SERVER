from django.shortcuts import render, redirect, get_object_or_404
from django.contrib.auth.decorators import login_required
from django.contrib.auth.forms import UserCreationForm
from django.contrib import messages
from django.contrib.auth.models import User
from django.db.models import Count, Sum, Q
from django.utils import timezone
import json
import requests
import os
import logging
import re
from typing import Dict, List, Optional, Tuple

from rest_framework.test import APIRequestFactory, force_authenticate
from django.http import JsonResponse
from django.views.decorators.http import require_POST
from django.views.decorators.csrf import csrf_exempt

from api_set1.views import GetQuotesView

from .quote_payload import DEFAULT_QUOTE_NEW_FORM, build_get_quotes_payload, parse_quote_new_request
from django.db import connection

from api_set1.models import (
    Lead, Deal, DealDocument,
    IndividualCustomer, CorporateCustomer, UBODetail,
    Transaction, InsurerReference, Attachment, StatusOverview,
    QuoteRequest, Quote, InsuranceProvider,
)

api_logger = logging.getLogger("api_providers")

_SAFE_IDENT_RE = re.compile(r"^[a-z0-9_]+$")

MASTERDATA_DROPDOWNS: Dict[str, str] = {
    # context_key_in_template: sqlite_table_name
    # Nationality: use NIA "PolAssrNation" masterdata (code/description).
    "nationality_codes": "nia_polassrnation",
    "emirate_codes": "dic_emirites",
    "gender_codes": "dic_gender",
    "ncd_codes": "dic_ncdyears",
    "plate_code_codes": "dic_platecode",
    "plate_source_codes": "dic_platesource",
    "traffic_trans_codes": "dic_traffictranstype",
    "bank_codes": "dic_bank_name",
    # Anoudapps/QIC tariff codes (motor)
    "vehicle_usage_codes": "nia_vehusage",
}

_ANNOUDAPPS_MAPPING_TABLE_CACHE: Optional[str] = None
_ANNOUDAPPS_CYLINDER_TABLE_CACHE: Optional[str] = None
_ANNOUDAPPS_BODYTYPE_TABLE_CACHE: Optional[str] = None


def load_dropdown(table_name: str, code_column: str = "code", desc_column: str = "description") -> List[Tuple[str, str]]:
    """
    Load dropdown options from a dynamic masterdata table.
    Returns: [(code, description), ...]
    Safe fallback: returns [] if table/columns are missing.
    """
    if not (
        _SAFE_IDENT_RE.fullmatch(table_name or "")
        and _SAFE_IDENT_RE.fullmatch(code_column or "")
        and _SAFE_IDENT_RE.fullmatch(desc_column or "")
    ):
        api_logger.warning(
            "Dropdown load blocked (unsafe identifier) | table=%s code_col=%s desc_col=%s",
            table_name,
            code_column,
            desc_column,
        )
        return []
    try:
        with connection.cursor() as cur:
            cur.execute(
                f'SELECT "{code_column}", "{desc_column}" FROM "{table_name}" '
                f'WHERE "{code_column}" IS NOT NULL AND TRIM(CAST("{code_column}" AS TEXT)) <> "" '
                f'ORDER BY "{code_column}"'
            )
            rows = cur.fetchall()
        out: List[Tuple[str, str]] = []
        for code, desc in rows:
            code_s = str(code).strip() if code is not None else ""
            if not code_s:
                continue
            desc_s = str(desc).strip() if desc is not None else ""
            out.append((code_s, desc_s))
        return out
    except Exception as e:
        api_logger.warning("Dropdown load failed | table=%s | error=%s", table_name, e)
        return []


def _find_annoudapps_make_model_mapping_table() -> Optional[str]:
    """
    Discover which annoudapps_* dynamic table contains the Bayanaty->tariff mapping.
    We look for a table name starting with 'annoudapps_' having columns:
      - bayanaty_make_code
      - bayanaty_model_code
      - make_code
      - model_code
    Returns the table name, or None if not found.
    Cached for the process lifetime.
    """
    global _ANNOUDAPPS_MAPPING_TABLE_CACHE
    if _ANNOUDAPPS_MAPPING_TABLE_CACHE is not None:
        return _ANNOUDAPPS_MAPPING_TABLE_CACHE or None

    required = {"bayanaty_make_code", "bayanaty_model_code", "make_code", "model_code"}
    try:
        with connection.cursor() as cur:
            cur.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'annoudapps\\_%' ESCAPE '\\' ORDER BY name"
            )
            tables = [r[0] for r in cur.fetchall() if r and isinstance(r[0], str)]

        for t in tables:
            if not _SAFE_IDENT_RE.fullmatch(t or ""):
                continue
            try:
                with connection.cursor() as cur:
                    cur.execute(f'PRAGMA table_info("{t}")')
                    cols = {str(r[1]).strip().lower() for r in cur.fetchall() if r and len(r) > 1 and r[1]}
                if required.issubset(cols):
                    _ANNOUDAPPS_MAPPING_TABLE_CACHE = t
                    api_logger.info("Detected Annoudapps make/model mapping table | table=%s", t)
                    return t
            except Exception:
                api_logger.exception("Failed inspecting annoudapps table columns | table=%s", t)
                continue

        api_logger.warning(
            "No Annoudapps make/model mapping table found. Need annoudapps_* table with columns: %s",
            ", ".join(sorted(required)),
        )
        _ANNOUDAPPS_MAPPING_TABLE_CACHE = ""
        return None
    except Exception:
        api_logger.exception("Failed searching SQLite schema for annoudapps_* tables")
        _ANNOUDAPPS_MAPPING_TABLE_CACHE = ""
        return None


def _find_annoudapps_no_of_cylinder_mapping_table() -> Optional[str]:
    """
    Discover which annoudapps_* dynamic table contains Bayanaty->cylinder mapping.
    We look for a table starting with 'annoudapps_' having columns:
      - cylinder_code
      - cylinder_desc
    Returns the table name, or None if not found.
    Cached for the process lifetime.
    """
    global _ANNOUDAPPS_CYLINDER_TABLE_CACHE
    if _ANNOUDAPPS_CYLINDER_TABLE_CACHE is not None:
        return _ANNOUDAPPS_CYLINDER_TABLE_CACHE or None

    required = {"cylinder_code", "cylinder_desc"}
    try:
        with connection.cursor() as cur:
            cur.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'annoudapps\\_%' ESCAPE '\\' ORDER BY name"
            )
            tables = [r[0] for r in cur.fetchall() if r and isinstance(r[0], str)]

        for t in tables:
            if not _SAFE_IDENT_RE.fullmatch(t or ""):
                continue
            try:
                with connection.cursor() as cur:
                    cur.execute(f'PRAGMA table_info("{t}")')
                    cols = {str(r[1]).strip().lower() for r in cur.fetchall() if r and len(r) > 1 and r[1]}
                if required.issubset(cols):
                    _ANNOUDAPPS_CYLINDER_TABLE_CACHE = t
                    api_logger.info("Detected Annoudapps cylinder mapping table | table=%s", t)
                    return t
            except Exception:
                api_logger.exception("Failed inspecting annoudapps cylinder table columns | table=%s", t)
                continue

        api_logger.warning(
            "No Annoudapps cylinder mapping table found. Need annoudapps_* table with columns: %s",
            ", ".join(sorted(required)),
        )
        _ANNOUDAPPS_CYLINDER_TABLE_CACHE = ""
        return None
    except Exception:
        api_logger.exception("Failed searching SQLite schema for annoudapps_* cylinder tables")
        _ANNOUDAPPS_CYLINDER_TABLE_CACHE = ""
        return None


def _find_annoudapps_body_type_mapping_table() -> Optional[str]:
    """
    Discover which annoudapps_* dynamic table maps Bayanaty bodyType -> Anoudapps vehicleType.
    Expected columns (from imported masterdata): body_type_code, bayanaty_body_type_code.
    Cached for the process lifetime.
    """
    global _ANNOUDAPPS_BODYTYPE_TABLE_CACHE
    if _ANNOUDAPPS_BODYTYPE_TABLE_CACHE is not None:
        return _ANNOUDAPPS_BODYTYPE_TABLE_CACHE or None

    required = {"body_type_code", "bayanaty_body_type_code"}
    try:
        with connection.cursor() as cur:
            cur.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'annoudapps\\_%' ESCAPE '\\' ORDER BY name"
            )
            tables = [r[0] for r in cur.fetchall() if r and isinstance(r[0], str)]

        for t in tables:
            if not _SAFE_IDENT_RE.fullmatch(t or ""):
                continue
            try:
                with connection.cursor() as cur:
                    cur.execute(f'PRAGMA table_info("{t}")')
                    cols = {str(r[1]).strip().lower() for r in cur.fetchall() if r and len(r) > 1 and r[1]}
                if required.issubset(cols):
                    _ANNOUDAPPS_BODYTYPE_TABLE_CACHE = t
                    api_logger.info("Detected Annoudapps bodyType mapping table | table=%s", t)
                    return t
            except Exception:
                api_logger.exception("Failed inspecting annoudapps bodyType table columns | table=%s", t)
                continue

        api_logger.warning(
            "No Annoudapps bodyType mapping table found. Need annoudapps_* table with columns: %s",
            ", ".join(sorted(required)),
        )
        _ANNOUDAPPS_BODYTYPE_TABLE_CACHE = ""
        return None
    except Exception:
        api_logger.exception("Failed searching SQLite schema for annoudapps_* bodyType tables")
        _ANNOUDAPPS_BODYTYPE_TABLE_CACHE = ""
        return None


def home(request):
    if request.user.is_authenticated and request.user.is_staff:
        return redirect('admin_dashboard')
    return render(request, 'ui/home.html')


@login_required
def admin_dashboard(request):
    if not request.user.is_staff:
        return redirect('home')

    # --- Stats ---
    total_leads = Lead.objects.count()
    total_deals = Deal.objects.count()
    total_individual = IndividualCustomer.objects.count()
    total_corporate = CorporateCustomer.objects.count()
    total_transactions = Transaction.objects.count()
    total_quotes = QuoteRequest.objects.count()
    total_providers = InsuranceProvider.objects.filter(is_active=True).count()
    total_users = User.objects.count()

    # Revenue
    revenue = Transaction.objects.aggregate(
        total=Sum('customer_net_due')
    )['total'] or 0
    commission = Transaction.objects.aggregate(
        total=Sum('commission_amount')
    )['total'] or 0

    # Lead pipeline
    lead_stages = {}
    for stage_code, stage_label in Lead.STAGE_CHOICES:
        lead_stages[stage_label] = Lead.objects.filter(stage=stage_code).count()

    # Recent items
    recent_leads = Lead.objects.select_related('responsible').order_by('-created_at')[:5]
    recent_deals = Deal.objects.select_related('lead').order_by('-created_at')[:5]
    recent_transactions = Transaction.objects.select_related(
        'individual_customer', 'corporate_customer'
    ).order_by('-created_at')[:5]
    recent_statuses = StatusOverview.objects.select_related(
        'transaction', 'user', 'assigned_user'
    ).order_by('-date')[:8]

    # Product type distribution
    product_distribution = Lead.objects.values('product_type').annotate(
        count=Count('id')
    ).order_by('-count')

    context = {
        'total_leads': total_leads,
        'total_deals': total_deals,
        'total_individual': total_individual,
        'total_corporate': total_corporate,
        'total_transactions': total_transactions,
        'total_quotes': total_quotes,
        'total_providers': total_providers,
        'total_users': total_users,
        'revenue': revenue,
        'commission': commission,
        'lead_stages': lead_stages,
        'recent_leads': recent_leads,
        'recent_deals': recent_deals,
        'recent_transactions': recent_transactions,
        'recent_statuses': recent_statuses,
        'product_distribution': product_distribution,
    }
    return render(request, 'ui/admin_dashboard.html', context)


@login_required
def lead_detail(request, lead_id):
    """Detail view for a single Lead — CRM style"""
    if not request.user.is_staff:
        return redirect('home')

    lead = get_object_or_404(Lead.objects.select_related('responsible'), pk=lead_id)
    deals = lead.deals.all().order_by('-created_at')
    all_leads = Lead.objects.order_by('-created_at')[:20]

    # Stage pipeline data
    stages = Lead.STAGE_CHOICES
    current_stage_index = next(
        (i for i, (code, _) in enumerate(stages) if code == lead.stage), 0
    )

    context = {
        'lead': lead,
        'deals': deals,
        'stages': stages,
        'current_stage_index': current_stage_index,
        'all_leads': all_leads,
    }
    return render(request, 'ui/lead_detail.html', context)


@login_required
def deal_detail(request, deal_id):
    """Detail view for a single Deal"""
    if not request.user.is_staff:
        return redirect('home')

    deal = get_object_or_404(
        Deal.objects.select_related('lead'), pk=deal_id
    )
    documents = deal.documents.all().order_by('-uploaded_at')

    context = {
        'deal': deal,
        'documents': documents,
    }
    return render(request, 'ui/deal_detail.html', context)


@login_required
def transaction_detail(request, txn_id):
    """Detail view for a single Transaction"""
    if not request.user.is_staff:
        return redirect('home')

    txn = get_object_or_404(
        Transaction.objects.select_related(
            'individual_customer', 'corporate_customer'
        ), pk=txn_id
    )
    insurer_ref = InsurerReference.objects.filter(transaction=txn).first()
    attachments = txn.attachments.all().order_by('-uploaded_at')
    statuses = txn.status_history.select_related('user', 'assigned_user').order_by('-date')

    context = {
        'txn': txn,
        'insurer_ref': insurer_ref,
        'attachments': attachments,
        'statuses': statuses,
    }
    return render(request, 'ui/transaction_detail.html', context)


@login_required
def leads_list(request):
    """List all leads"""
    if not request.user.is_staff:
        return redirect('home')
    leads = Lead.objects.select_related('responsible').order_by('-created_at')
    context = {'leads': leads, 'stages': Lead.STAGE_CHOICES}
    return render(request, 'ui/leads_list.html', context)


@login_required
def deals_list(request):
    """List all deals"""
    if not request.user.is_staff:
        return redirect('home')
    deals = Deal.objects.select_related('lead').order_by('-created_at')
    context = {'deals': deals}
    return render(request, 'ui/deals_list.html', context)


@login_required
def transactions_list(request):
    """List all transactions"""
    if not request.user.is_staff:
        return redirect('home')
    transactions = Transaction.objects.select_related(
        'individual_customer', 'corporate_customer'
    ).order_by('-created_at')
    context = {'transactions': transactions}
    return render(request, 'ui/transactions_list.html', context)


@login_required
def customers_list(request):
    """List all customers (individual + corporate)"""
    if not request.user.is_staff:
        return redirect('home')
    individuals = IndividualCustomer.objects.order_by('-created_at')
    corporates = CorporateCustomer.objects.order_by('-created_at')
    context = {'individuals': individuals, 'corporates': corporates}
    return render(request, 'ui/customers_list.html', context)
@login_required
def quote_proposal(request, quote_request_id):
    """
    Renders a high-fidelity professional proposal for a specific quote request.
    Includes branding, comparison table, and membership marketing.
    """
    if not request.user.is_staff:
        return redirect('home')
        
    quote_request = get_object_or_404(
        QuoteRequest.objects.select_related('user'), 
        pk=quote_request_id
    )
    # Get all quotes for this request, sorted by best score
    quotes = Quote.objects.filter(quote_request=quote_request).order_by('-comparison_score')
    
    if not quotes.exists():
        messages.warning(request, "No quotes found for this request. Please fetch quotes first.")
        return redirect('admin_dashboard')
        
    best_quote = quotes.filter(is_best=True).first()
    
    context = {
        'quote_request': quote_request,
        'quotes': quotes,
        'best_quote': best_quote,
        'marketing': True # To toggle second page optionally
    }
    
    return render(request, 'ui/proposal.html', context)


@login_required
def quote_new(request):
    """
    Submit motor/quote fields and run the same logic as POST /api/quotes/get-quotes/
    in-process (avoids unreliable loopback HTTP to the same dev server).
    """
    response_json = None
    response_status = None
    error_message = ""

    # Load dropdown lists from SQLite masterdata tables (best-effort; falls back to empty lists).
    nationality_codes = load_dropdown(MASTERDATA_DROPDOWNS["nationality_codes"])
    emirate_codes = load_dropdown(MASTERDATA_DROPDOWNS["emirate_codes"])
    gender_codes = load_dropdown(MASTERDATA_DROPDOWNS["gender_codes"])
    ncd_codes = load_dropdown(MASTERDATA_DROPDOWNS["ncd_codes"])
    plate_code_codes = load_dropdown(MASTERDATA_DROPDOWNS["plate_code_codes"])
    plate_source_codes = load_dropdown(MASTERDATA_DROPDOWNS["plate_source_codes"])
    traffic_trans_codes = load_dropdown(MASTERDATA_DROPDOWNS["traffic_trans_codes"])
    bank_codes = load_dropdown(MASTERDATA_DROPDOWNS["bank_codes"])
    vehicle_usage_codes = load_dropdown(MASTERDATA_DROPDOWNS["vehicle_usage_codes"])

    form = {**DEFAULT_QUOTE_NEW_FORM}

    if request.method == "POST":
        try:
            form, document_lists = parse_quote_new_request(request)
            payload = build_get_quotes_payload(form, document_lists)
            form["city"] = payload["city"]

            # Call GetQuotesView in-process. APIRequestFactory defaults HTTP_HOST to "testserver",
            # which triggers Invalid HTTP_HOST / DisallowedHost when the view uses build_absolute_uri().
            # Mirror the real browser request host (e.g. localhost:8000).
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
            response_status = drf_resp.status_code
            response_json = getattr(drf_resp, "data", None)
            if response_json is None:
                response_json = {"detail": "Unexpected response from GetQuotesView"}

            if response_status >= 400:
                error_message = "API returned an error."
        except Exception as e:
            error_message = str(e)

    context = {
        "form": form,
        "nationality_codes": nationality_codes,
        "emirate_codes": emirate_codes,
        "gender_codes": gender_codes,
        "ncd_codes": ncd_codes,
        "plate_code_codes": plate_code_codes,
        "plate_source_codes": plate_source_codes,
        "traffic_trans_codes": traffic_trans_codes,
        "bank_codes": bank_codes,
        "vehicle_usage_codes": vehicle_usage_codes,
        "response_json": response_json,
        "response_status": response_status,
        "error_message": error_message,
    }
    return render(request, "ui/quote_new.html", context)


@login_required
@require_POST
def bayanaty_vehicle_details(request):
    """
    Calls QIC Bayanaty vehicleDetails API (Vin -> vehicle details/spec).
    Source of truth: Bayanaty API Integration Doc_V3.pdf and Postman collections.
    """
    try:
        payload = json.loads(request.body.decode("utf-8") or "{}")
    except Exception:
        payload = {}

    vin = (payload.get("vin") or payload.get("Vin") or "").strip()
    if not vin:
        return JsonResponse({"error": "vin is required"}, status=400)

    # Log what the browser sent (safe: no credentials here).
    try:
        api_logger.info(
            "Bayanaty vehicleDetails request (from UI) | user=%s payload=%s",
            getattr(request.user, "username", ""),
            json.dumps(payload, ensure_ascii=False)[:5000],
        )
    except Exception:
        api_logger.exception("Failed logging Bayanaty vehicleDetails request payload")

    # From docs: Bayanaty Service URL:
    # https://www.devapi.anoudapps.com/qicservices/aggregator/bayanaty
    # Postman uses: https://devapi.anoudapps.com/qicservices/aggregator/bayanaty/vehicleDetails?company=002
    url = "https://devapi.anoudapps.com/qicservices/aggregator/bayanaty/vehicleDetails"

    # Use the same Basic credentials as Anoudapps motor APIs.
    # Defaults match the Postman collection you shared.
    auth_user = os.environ.get("ANOUDAPPS_USER", "promise_api")
    auth_pass = os.environ.get("ANOUDAPPS_PASS", "promise_api#2026$")
    company = "002"

    upstream_payload = {"Vin": vin}
    try:
        api_logger.info(
            "Bayanaty vehicleDetails upstream call | user=%s url=%s company=%s json=%s",
            getattr(request.user, "username", ""),
            url,
            company,
            json.dumps(upstream_payload, ensure_ascii=False),
        )
    except Exception:
        api_logger.exception("Failed logging Bayanaty vehicleDetails upstream call")

    r = requests.post(
        url,
        params={"company": company},
        headers={"company": company},
        auth=(auth_user, auth_pass),
        json=upstream_payload,
        timeout=30,
    )

    try:
        data = r.json()
    except Exception:
        try:
            api_logger.warning(
                "Bayanaty vehicleDetails non-JSON response | user=%s status=%s body=%s",
                getattr(request.user, "username", ""),
                r.status_code,
                (r.text or "")[:5000],
            )
        except Exception:
            api_logger.exception("Failed logging Bayanaty vehicleDetails non-JSON response")
        return JsonResponse({"error": "non_json_response", "status": r.status_code, "raw": r.text[:2000]}, status=502)

    # Log upstream response (trimmed; keep full JSON in our own return body already).
    try:
        api_logger.info(
            "Bayanaty vehicleDetails response | user=%s status=%s json=%s",
            getattr(request.user, "username", ""),
            r.status_code,
            json.dumps(data, ensure_ascii=False)[:5000],
        )
    except Exception:
        api_logger.exception("Failed logging Bayanaty vehicleDetails upstream response")

    # Best-effort extraction into a frontend-friendly shape.
    # Real response shape (per user): vehicleFeatures.primaryFeatures is an object with lower-case keys.
    vf = (data.get("vehicleFeatures") or data.get("VehicleFeatures") or {}) if isinstance(data, dict) else {}
    primary = (vf.get("primaryFeatures") or vf.get("PrimaryFeatures") or {}) if isinstance(vf, dict) else {}
    if isinstance(primary, list):
        primary = primary[0] if primary else {}
    if not isinstance(primary, dict):
        primary = {}

    def _get_obj(obj: dict, key1: str, key2: str) -> dict:
        v = obj.get(key1) or obj.get(key2) or {}
        return v if isinstance(v, dict) else {}

    make = _get_obj(primary, "make", "Make")
    model = _get_obj(primary, "model", "Model")
    trim = _get_obj(primary, "trim", "Trim")
    body_type = _get_obj(primary, "bodyType", "BodyType")
    engine_capacity = _get_obj(primary, "engineCapacity", "EngineCapacity")

    extracted = {
        "vin": data.get("vin") or vin,
        "statusCode": data.get("statusCode") or data.get("StatusCode"),
        "respCode": data.get("respCode"),
        "errMessage": data.get("errMessage"),
        "modelYear": primary.get("modelYear") or primary.get("ModelYear"),
        "makeId": make.get("id") or make.get("Id"),
        "makeValue": make.get("value") or make.get("Value"),
        "modelId": model.get("id") or model.get("Id"),
        "modelValue": model.get("value") or model.get("Value"),
        "trimId": trim.get("id") or trim.get("Id"),
        "trimValue": trim.get("value") or trim.get("Value"),
        "bodyTypeId": body_type.get("id") or body_type.get("Id"),
        "bodyTypeValue": body_type.get("value") or body_type.get("Value"),
        "engineCapacityId": engine_capacity.get("id") or engine_capacity.get("Id"),
        "engineCapacityValue": engine_capacity.get("value") or engine_capacity.get("Value"),
        "doors": primary.get("doors") or primary.get("Doors"),
        "seats": primary.get("seats") or primary.get("Seats"),
    }

    # Vehicle valuation range (used to pick a valid sumInsured for tariff).
    vv = (data.get("vehicleValues") or data.get("VehicleValues") or {}) if isinstance(data, dict) else {}
    if isinstance(vv, dict):
        extracted["vehicleValueActual"] = vv.get("actual")
        extracted["vehicleValueMin"] = vv.get("minimum")
        extracted["vehicleValueMax"] = vv.get("maximum")

    # Map Bayanaty IDs -> tariff codes using Annoudapps masterdata tables only (no runtime Excel reads).
    try:
        mk = str(extracted.get("makeId") or "").strip()
        md = str(extracted.get("modelId") or "").strip()
        if mk and md:
            mapping_table = _find_annoudapps_make_model_mapping_table()
            if not mapping_table:
                api_logger.warning(
                    "No Annoudapps mapping table available; cannot map Bayanaty IDs to tariff codes | makeId=%s modelId=%s",
                    mk,
                    md,
                )
            else:
                with connection.cursor() as cur:
                    cur.execute(
                        f'SELECT "make_code", "model_code" '
                        f'FROM "{mapping_table}" '
                        f'WHERE "bayanaty_make_code" = %s AND "bayanaty_model_code" = %s '
                        "LIMIT 1",
                        [mk, md],
                    )
                    row = cur.fetchone()
                if row:
                    extracted["tariffMakeCode"] = str(row[0]).strip() if row[0] is not None else ""
                    extracted["tariffModelCode"] = str(row[1]).strip() if row[1] is not None else ""
    except Exception:
        # Non-fatal; UI can still fill Bayanaty IDs.
        api_logger.exception("Failed mapping Bayanaty IDs to Annoudapps tariff codes")

    # Map Bayanaty cylinders -> Anoudapps noOfCylinder code (engineCapacityId field for UI).
    try:
        # Bayanaty response: cylinders usually come from engineDetails.cylinders.
        # In Bayanaty responses, engineDetails is typically nested under vehicleFeatures.
        engine_details = {}
        try:
            if isinstance(vf, dict):
                engine_details = vf.get("engineDetails") or vf.get("EngineDetails") or {}
                if not engine_details and isinstance(vf.get("otherFeatures"), dict):
                    engine_details = (
                        vf["otherFeatures"].get("engineDetails")
                        or vf["otherFeatures"].get("EngineDetails")
                        or {}
                    )
        except Exception:
            engine_details = {}
        if not isinstance(engine_details, dict):
            engine_details = {}
        cylinders_raw = (
            engine_details.get("cylinders")
            or engine_details.get("Cylinders")
            or engine_details.get("noOfCylinders")
            or engine_details.get("NoOfCylinders")
            or engine_details.get("NoOfCylinder")
            or engine_details.get("noOfCylinder")
        )
        if cylinders_raw is not None:
            cylinders_str = str(cylinders_raw).strip()
        else:
            cylinders_str = ""

        # Fallback: if engineDetails doesn't include cylinders, try the earlier engineCapacity object.
        if not cylinders_str:
            cylinders_str = str(engine_capacity.get("value") or engine_capacity.get("Value") or "").strip()

        if cylinders_str:
            mapping_table = _find_annoudapps_no_of_cylinder_mapping_table()
            if not mapping_table:
                api_logger.warning(
                    "No Annoudapps cylinder mapping table available; cannot map cylinders to anoud_noOfCylinder | cylinders=%s",
                    cylinders_str,
                )
            else:
                with connection.cursor() as cur:
                    cur.execute(
                        f'SELECT "cylinder_code" '
                        f'FROM "{mapping_table}" '
                        f'WHERE "cylinder_desc" = %s '
                        "LIMIT 1",
                        [cylinders_str],
                    )
                    row = cur.fetchone()
                if row:
                    extracted["engineCapacityId"] = str(row[0]).strip() if row[0] is not None else extracted.get("engineCapacityId")
                    extracted["engineCapacityValue"] = cylinders_str
                else:
                    api_logger.warning(
                        "Cylinder mapping not found | cylinders=%s table=%s",
                        cylinders_str,
                        mapping_table,
                    )
        else:
            api_logger.warning("No cylinders value found in Bayanaty response for VIN | vin=%s", vin)
    except Exception:
        # Non-fatal; UI can still use whatever engineCapacityId we already extracted.
        api_logger.exception("Failed mapping Bayanaty cylinders to Annoudapps noOfCylinder code")

    # Map Bayanaty bodyTypeId -> Anoudapps vehicleType code.
    try:
        body_type_id = str(extracted.get("bodyTypeId") or "").strip()
        if body_type_id:
            mapping_table = _find_annoudapps_body_type_mapping_table()
            if not mapping_table:
                api_logger.warning(
                    "No Annoudapps bodyType mapping table available; cannot map bodyTypeId to vehicleType | bodyTypeId=%s",
                    body_type_id,
                )
            else:
                needle = f",{body_type_id},"
                with connection.cursor() as cur:
                    cur.execute(
                        f'SELECT "body_type_code" '
                        f'FROM "{mapping_table}" '
                        f'WHERE "bayanaty_body_type_code" LIKE %s '
                        "LIMIT 1",
                        [f"%{needle}%"],
                    )
                    row = cur.fetchone()
                if row and row[0] is not None:
                    extracted["vehicleTypeCode"] = str(row[0]).strip()
        else:
            api_logger.warning("No bodyTypeId found in Bayanaty extracted payload for VIN | vin=%s", vin)
    except Exception:
        api_logger.exception("Failed mapping Bayanaty bodyTypeId to Annoudapps vehicleType code")

    return JsonResponse({"status": r.status_code, "raw": data, "extracted": extracted})
