"""
Shared payload builder for /quotes/new/ (HTML) and POST /api/quotes/get-quote-lists/ (API).
Keeps one code path for GetQuotesView input.
"""
from __future__ import annotations

import base64
import logging
import re
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple

from django.db import connection

from .quote_documents import optimize_quote_document_bytes

api_logger = logging.getLogger("api_providers")

_SAFE_IDENT_RE = re.compile(r"^[a-z0-9_]+$")

# Friendly multipart FILE field names -> DIC broker document code + payload name (quote_new.html).
NAMED_DOCUMENT_SLOT_TO_DIC: Tuple[Tuple[str, str, str], ...] = (
    ("emirate_id_front", "103", "Emirate ID Front"),
    ("emirate_id_back", "105", "Emirate ID Back"),
    ("driving_license_front", "106", "Driving License Front"),
    ("driving_license_back", "121", "Driving License Back"),
    ("mulkiya_id_front", "101", "Mulkiya ID Front"),
    ("mulkiya_id_back", "102", "Mulkiya ID Back"),
    ("bank_lpo", "107", "BankLpo"),
)

# Must match keys used in ui.views.quote_new form dict
DEFAULT_QUOTE_NEW_FORM: Dict[str, str] = {
    "insurance_type": "motor",
    "age": "",
    "sum_insured": "0",
    "city": "",
    "members": "1",
    "insuredName_en": "",
    "insuredName_ar": "",
    "nationality": "",
    "nationalId": "",
    "idExpiryDt": "",
    "dateOfBirth": "",
    "gender": "",
    "emirate": "",
    "emailAddress": "",
    "mobileNumber": "",
    "licenseNo": "",
    "licenseFmDt": "",
    "licenseToDt": "",
    "chassisNumber": "",
    "regNumber": "",
    "regDt": "",
    "plateCode": "",
    "PlateSource": "",
    "tcfNumber": "",
    "ncdYears": "",
    "trafficTranType": "",
    "isVehBrandNew": "N",
    "agencyRepairYn": "N",
    "bankName": "",
    "PolAssrPhone": "",
    "veh_service_type": "No",
    "anoud_makeCode": "",
    "anoud_modelCode": "",
    "anoud_modelYear": "",
    "anoud_vehicleType": "",
    "anoud_vehicleUsage": "",
    "anoud_noOfCylinder": "",
    "anoud_seatingCapacity": "",
    "anoud_firstRegDate": "",
    "anoud_gccSpec": "",
    "anoud_previousInsuranceValid": "",
    "anoud_totalLoss": "",
    "anoud_driverDOB": "",
    "anoud_noClaimYear": "",
    "anoud_selfDeclarationYear": "",
    "anoud_chassisNo": "",
    "anoud_driverExp": "",
    "anoud_admeId": "",
    "anoud_regnLocation": "",
}


def _str(v: Any) -> str:
    if v is None:
        return ""
    return str(v).strip()


def resolve_quote_city(form: Dict[str, Any]) -> str:
    raw = _str(form.get("city"))
    if raw:
        return raw
    em = _str(form.get("emirate"))
    if em:
        tbl = "dic_emirites"
        try:
            if _SAFE_IDENT_RE.fullmatch(tbl):
                with connection.cursor() as cur:
                    cur.execute(
                        f'SELECT "description" FROM "{tbl}" WHERE CAST("code" AS TEXT) = %s LIMIT 1',
                        [em],
                    )
                    row = cur.fetchone()
                if row and row[0] is not None:
                    d = str(row[0]).strip()
                    if d:
                        return d
        except Exception:
            api_logger.exception("resolve_quote_city: emirate lookup failed | emirate=%s", em)
    return "UAE"


def merge_incoming_into_form(
    form: Dict[str, str], incoming: Dict[str, Any]
) -> Dict[str, str]:
    out = dict(form)
    for k in out.keys():
        if k in incoming and incoming[k] is not None:
            out[k] = _str(incoming[k]) if not isinstance(incoming[k], (int, float)) else str(incoming[k])
    return out


def compute_age_from_form(form: Dict[str, str]) -> int:
    dob_raw = _str(form.get("dateOfBirth"))
    age = 0
    if dob_raw:
        dob = None
        for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%Y/%m/%d"):
            try:
                dob = datetime.strptime(dob_raw, fmt).date()
                break
            except Exception:
                continue
        if dob:
            today = date.today()
            age = today.year - dob.year - (1 if (today.month, today.day) < (dob.month, dob.day) else 0)
        else:
            api_logger.warning("Could not parse dateOfBirth for age calc | dateOfBirth=%s", dob_raw)
    if age <= 0:
        try:
            age = int(form.get("age") or "0")
        except (TypeError, ValueError):
            age = 0
    return age


def _infer_doc_type_from_filename(upload_fn: str, explicit: str) -> str:
    ft = _str(explicit).lower().lstrip(".")
    if ft:
        return ft
    fn = (upload_fn or "").lower()
    if fn.endswith(".pdf"):
        return "pdf"
    if fn.endswith(".png"):
        return "png"
    if fn.endswith((".jpg", ".jpeg")):
        return "jpg"
    if fn.endswith(".webp"):
        return "webp"
    return "jpg"


def _infer_doc_type_from_upload(f: Any, explicit: str) -> str:
    ft = _str(explicit).lower().lstrip(".")
    if ft:
        return ft
    ct = (_str(getattr(f, "content_type", "") or "") or "").lower()
    if ct:
        if "pdf" in ct:
            return "pdf"
        if "png" in ct:
            return "png"
        if "jpeg" in ct or "jpg" in ct:
            return "jpg"
        if "webp" in ct:
            return "webp"
    upload_fn = getattr(f, "name", "") or ""
    return _infer_doc_type_from_filename(upload_fn, "")


def _uploaded_file_to_document_entry(code: str, name: str, ftype: str, f: Any) -> Optional[Dict[str, Any]]:
    if not f:
        return None
    code = _str(code)
    name = _str(name)
    if not code:
        return None
    if not name:
        name = "upload"
    try:
        raw = f.read()
        upload_fn = getattr(f, "name", "") or ""
        ft = _infer_doc_type_from_upload(f, ftype)
        optimized, _eff = optimize_quote_document_bytes(raw, upload_fn or "upload.png")
        b64 = base64.b64encode(optimized).decode("utf-8")
        return {"code": code, "name": name, "base64": b64, "type": ft}
    except Exception:
        api_logger.exception("Document upload optimize/base64 failed | code=%s name=%s", code, name)
        return None


def _ensure_request_data_parsed(request) -> None:
    """DRF parses multipart into ``request.data`` / ``_files``; touch ``.data`` so FILES is populated."""
    try:
        _ = getattr(request, "data", None)
    except Exception:
        pass


def _multipart_file_getlist(request, key: str) -> List[Any]:
    """
    Uploaded files for ``key`` from Django FILES and from DRF ``request.data``.
    Postman/curl multipart sometimes surfaces only on ``request.data`` after DRF parse.
    """
    _ensure_request_data_parsed(request)
    out: List[Any] = []
    seen: set[int] = set()

    def add(f: Any) -> None:
        if f is None or isinstance(f, (str, bytes)):
            return
        i = id(f)
        if i in seen:
            return
        seen.add(i)
        out.append(f)

    fo = getattr(request, "FILES", None)
    if fo is not None:
        try:
            for f in fo.getlist(key):
                add(f)
        except Exception:
            try:
                add(fo.get(key))
            except Exception:
                pass

    try:
        d = getattr(request, "data", None)
    except Exception:
        d = None
    if d is not None:
        try:
            if hasattr(d, "getlist"):
                for f in d.getlist(key):
                    add(f)
            elif isinstance(d, dict):
                v = d.get(key)
                if isinstance(v, list):
                    for x in v:
                        add(x)
                else:
                    add(v)
        except Exception:
            pass
    return out


def _multipart_text_getlist(request, key: str) -> List[str]:
    """Parallel text parts (e.g. ``document_code[]``) from POST and DRF ``data``."""
    _ensure_request_data_parsed(request)
    if hasattr(request, "POST") and request.POST:
        xs = request.POST.getlist(key)
        if xs:
            return [_str(x) for x in xs]
    try:
        d = getattr(request, "data", None)
    except Exception:
        d = None
    if d is not None and hasattr(d, "getlist"):
        try:
            xs = d.getlist(key)
            if xs:
                return [_str(x) for x in xs]
        except Exception:
            pass
    return []


def _document_lists_from_named_slot_uploads(request) -> List[Dict[str, Any]]:
    """Map simple FILE keys (e.g. emirate_id_front) to DIC codes + optimize/base64."""
    out: List[Dict[str, Any]] = []
    for field_name, dic_code, dic_name in NAMED_DOCUMENT_SLOT_TO_DIC:
        for f in _multipart_file_getlist(request, field_name):
            entry = _uploaded_file_to_document_entry(dic_code, dic_name, "", f)
            if entry:
                out.append(entry)
    return out


def _legacy_document_lists_from_uploads(request) -> List[Dict[str, Any]]:
    document_lists: List[Dict[str, Any]] = []

    # 1) Legacy bracket fields + document_file[]
    files_br = _multipart_file_getlist(request, "document_file[]")
    if files_br:
        codes = _multipart_text_getlist(request, "document_code[]")
        names = _multipart_text_getlist(request, "document_name[]")
        types = _multipart_text_getlist(request, "document_type[]")
        n = len(files_br)
        codes = (list(codes) + [""] * n)[:n]
        names = (list(names) + [""] * n)[:n]
        types = (list(types) + [""] * n)[:n]
        for i in range(n):
            entry = _uploaded_file_to_document_entry(
                (codes[i] or "").strip(),
                (names[i] or "").strip(),
                (types[i] or "").strip(),
                files_br[i],
            )
            if entry:
                document_lists.append(entry)
        if document_lists:
            return document_lists

    # 2) Postman-friendly: document_file + parallel document_code / document_name / document_type
    files_plain = _multipart_file_getlist(request, "document_file")
    if files_plain:
        codes = _multipart_text_getlist(request, "document_code")
        names = _multipart_text_getlist(request, "document_name")
        types = _multipart_text_getlist(request, "document_type")
        for i, f in enumerate(files_plain):
            code = (codes[i] if i < len(codes) else "") or ""
            name = (names[i] if i < len(names) else "") or ""
            ftype = (types[i] if i < len(types) else "") or ""
            entry = _uploaded_file_to_document_entry(code.strip(), name.strip(), ftype.strip(), f)
            if entry:
                document_lists.append(entry)
        if document_lists:
            return document_lists

    # 3) Indexed: document_0_file, document_0_code, ...
    for i in range(50):
        prefix = f"document_{i}_"
        fk = f"{prefix}file"
        fl = _multipart_file_getlist(request, fk)
        if not fl:
            continue
        f = fl[0]
        _cl = _multipart_text_getlist(request, f"{prefix}code")
        code = _str(_cl[0]) if _cl else _str(request.POST.get(f"{prefix}code"))
        _nl = _multipart_text_getlist(request, f"{prefix}name")
        name = _str(_nl[0]) if _nl else _str(request.POST.get(f"{prefix}name"))
        _tl = _multipart_text_getlist(request, f"{prefix}type")
        ftype = _str(_tl[0]) if _tl else _str(request.POST.get(f"{prefix}type"))
        entry = _uploaded_file_to_document_entry(code, name, ftype, f)
        if entry:
            document_lists.append(entry)

    return document_lists


def build_document_lists_from_uploads(request) -> List[Dict[str, Any]]:
    """
    Build additional_details.documentLists entries for DIC (optimize + base64).

    Named FILE slots (simple Postman keys; code/name chosen server-side), then any legacy uploads:

    - ``emirate_id_front``, ``emirate_id_back``, ``driving_license_front``,
      ``driving_license_back``, ``mulkiya_id_front``, ``mulkiya_id_back``, ``bank_lpo``
      → DIC codes 103, 105, 106, 121, 101, 102, 107 respectively.

    Legacy multipart (first non-empty branch wins among the three below):

    1) Browser — ``document_code[]``, ``document_name[]``, ``document_type[]``, ``document_file[]``
    2) Postman — ``document_code``, ``document_name``, ``document_type`` (optional), ``document_file``
    3) Indexed — ``document_{i}_file`` + optional ``document_{i}_code`` / ``name`` / ``type``

    Type is inferred from explicit field, ``Content-Type``, or filename extension.
    """
    named = _document_lists_from_named_slot_uploads(request)
    legacy = _legacy_document_lists_from_uploads(request)
    return named + legacy


def normalize_json_document_lists(raw: Any) -> List[Dict[str, Any]]:
    if not isinstance(raw, list):
        return []
    out: List[Dict[str, Any]] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        code = _str(item.get("code"))
        name = _str(item.get("name"))
        ftype = _str(item.get("type"))
        b64 = item.get("base64")
        if not code or not name or b64 is None or _str(b64) == "":
            continue
        out.append({"code": code, "name": name, "type": ftype or "png", "base64": str(b64)})
    return out


def build_additional_details(form: Dict[str, str], document_lists: List[Dict[str, Any]]) -> Dict[str, Any]:
    return {
        "insuredName_en": form["insuredName_en"],
        "insuredName_ar": form["insuredName_ar"] or form["insuredName_en"],
        "nationality": form["nationality"],
        "nationalId": form["nationalId"],
        "idExpiryDt": form["idExpiryDt"],
        "dateOfBirth": form["dateOfBirth"],
        "gender": form["gender"],
        "emirate": form["emirate"],
        "emailAddress": form["emailAddress"],
        "mobileNumber": form["mobileNumber"],
        "licenseNo": form["licenseNo"],
        "licenseFmDt": form["licenseFmDt"],
        "licenseToDt": form["licenseToDt"],
        "chassisNumber": form["chassisNumber"],
        "regNumber": form["regNumber"],
        "regDt": form["regDt"],
        "plateCode": form["plateCode"],
        "PlateSource": form["PlateSource"],
        "tcfNumber": form["tcfNumber"],
        "ncdYears": form["ncdYears"],
        "trafficTranType": form["trafficTranType"],
        "isVehBrandNew": form["isVehBrandNew"],
        "agencyRepairYn": form["agencyRepairYn"],
        "bankName": form["bankName"],
        "documentLists": document_lists,
        "PolAssrPhone": form.get("PolAssrPhone") or "",
        "veh_service_type": form.get("veh_service_type") or "No",
        "makeCode": form.get("anoud_makeCode") or "",
        "modelCode": form.get("anoud_modelCode") or "",
        "modelYear": form.get("anoud_modelYear") or "",
        "vehicleType": form.get("anoud_vehicleType") or "",
        "vehicleUsage": form.get("anoud_vehicleUsage") or "",
        "noOfCylinder": form.get("anoud_noOfCylinder") or "",
        "seatingCapacity": form.get("anoud_seatingCapacity") or "",
        "firstRegDate": form["regDt"],
        "gccSpec": form.get("anoud_gccSpec") or "",
        "previousInsuranceValid": form.get("anoud_previousInsuranceValid") or "",
        "totalLoss": form.get("anoud_totalLoss") or "0",
        "driverDOB": form.get("anoud_driverDOB") or "",
        "noClaimYear": form.get("anoud_noClaimYear") or "0",
        "selfDeclarationYear": form.get("anoud_selfDeclarationYear") or "0",
        "chassisNo": form.get("anoud_chassisNo") or (form.get("chassisNumber") or ""),
        "driverExp": form.get("anoud_driverExp") or "",
        "admeId": form.get("anoud_admeId") or "",
        "regnLocation": form["emirate"],
    }


def build_get_quotes_payload(form: Dict[str, str], document_lists: List[Dict[str, Any]]) -> Dict[str, Any]:
    insurance_type = form["insurance_type"]
    age = compute_age_from_form(form)
    sum_insured = float(form.get("sum_insured") or "0")
    city = resolve_quote_city(form)
    form = dict(form)
    form["city"] = city
    members = int(form.get("members") or "1")
    additional_details = build_additional_details(form, document_lists)
    return {
        "insurance_type": insurance_type,
        "age": age,
        "sum_insured": sum_insured,
        "city": city,
        "members": members,
        "additional_details": additional_details,
    }


def _multipart_merge_form_defaults(request, form: Dict[str, str]) -> None:
    """Fill ``form`` text fields from ``request.POST`` or DRF ``request.data`` (skip file parts)."""
    _ensure_request_data_parsed(request)
    try:
        d = getattr(request, "data", None)
    except Exception:
        d = None
    for k in form.keys():
        if hasattr(request, "POST") and k in request.POST:
            form[k] = _str(request.POST.get(k, form[k]))
            continue
        if d is None:
            continue
        try:
            if k not in d:
                continue
        except Exception:
            continue
        val = d.get(k)
        if val is None:
            continue
        if hasattr(val, "read"):
            continue
        if isinstance(val, list):
            form[k] = _str(val[0]) if val else form[k]
            continue
        form[k] = _str(val)


def _looks_like_multipart_upload(request) -> bool:
    ct = (getattr(request, "content_type", None) or "").lower()
    if "multipart/form-data" in ct:
        return True
    try:
        fo = getattr(request, "FILES", None)
        if fo is not None and len(fo) > 0:
            return True
    except Exception:
        pass
    _ensure_request_data_parsed(request)
    try:
        d = getattr(request, "data", None)
    except Exception:
        return False
    if d is None:
        return False
    slot_names = {t[0] for t in NAMED_DOCUMENT_SLOT_TO_DIC}
    try:
        if any(name in d for name in slot_names):
            return True
    except Exception:
        pass
    for legacy in ("document_file", "document_file[]"):
        try:
            if legacy in d:
                return True
        except Exception:
            continue
    return False


def parse_quote_new_request(request) -> Tuple[Dict[str, str], List[Dict[str, Any]]]:
    """
    Parse the same inputs as the HTML form: multipart (browser) or JSON (Postman).
    JSON: either a flat object with form field names, or { "form": { ... }, "documentLists": [...] }.
    """
    form: Dict[str, str] = {**DEFAULT_QUOTE_NEW_FORM}

    if _looks_like_multipart_upload(request):
        _multipart_merge_form_defaults(request, form)
        return form, build_document_lists_from_uploads(request)

    # Typical HTML form: application/x-www-form-urlencoded
    if request.POST:
        for k in form.keys():
            if k in request.POST:
                form[k] = _str(request.POST.get(k, form[k]))
        return form, []

    data = getattr(request, "data", None)
    if not isinstance(data, dict):
        data = {}
    if isinstance(data.get("form"), dict):
        incoming = data["form"]
    else:
        incoming = {k: v for k, v in data.items() if k != "documentLists"}
    form = merge_incoming_into_form(form, incoming)
    return form, normalize_json_document_lists(data.get("documentLists"))
