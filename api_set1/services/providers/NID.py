"""
NIA motor integration.

Your current NIA host supports:
- Api/Auth/Login (username/password/loginMode)
- Api/Motor/CreateQuote
"""
from __future__ import annotations

import logging
import os
import re
import time
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from .base import BaseProvider

logger = logging.getLogger(__name__)
api_logger = logging.getLogger("api_providers")

# Default HTTP paths (override via env)
# New credentials you provided match the legacy login payload: username/password/loginMode.
NIA_LOGIN_PATH = os.environ.get("NIA_LOGIN_PATH", "Api/Auth/Login")
NIA_CREATE_QUOTE_PATH = os.environ.get("NIA_CREATE_QUOTE_PATH", "Api/Motor/CreateQuote")

# UAE emirate codes (DIC-style) → NIA letter codes used in samples (VehRegnEmirate)
_EMIRATE_CODE_TO_REGN: Dict[str, str] = {
    "01": "AUH",
    "02": "AJM",
    "03": "DXB",
    "04": "FUJ",
    "05": "RKT",
    "06": "SHJ",
    "07": "UAQ",
    "AUH": "AUH",
    "AJM": "AJM",
    "DXB": "DXB",
    "FUJ": "FUJ",
    "RKT": "RKT",
    "SHJ": "SHJ",
    "UAQ": "UAQ",
}


def _get_field(data: Dict[str, Any], key: str, default: Any = None) -> Any:
    ad = data.get("additional_details") or {}
    if key in ad:
        return ad.get(key)
    return data.get(key, default)


def _to_str(v: Any) -> str:
    if v is None:
        return ""
    return str(v).strip()


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def _parse_status_code(resp: Optional[Dict]) -> Optional[str]:
    if not resp:
        return None
    st = resp.get("Status")
    if isinstance(st, dict):
        return str(st.get("Code") or "")
    if isinstance(st, list) and st:
        return str(st[0].get("Code") or "")
    return None


def _login_success(resp: Optional[Dict]) -> bool:
    code = _parse_status_code(resp)
    return code == "1005"


def _quotation_success(resp: Optional[Dict]) -> bool:
    code = _parse_status_code(resp)
    return code == "1001"


def _legacy_quote_success(resp: Optional[Dict[str, Any]]) -> bool:
    # legacy shape: {"Status": 1, "Data": {...}}
    return bool(isinstance(resp, dict) and resp.get("Status") == 1 and resp.get("Data"))


def _extract_token(resp: Dict[str, Any]) -> Optional[str]:
    data = resp.get("Data")
    if isinstance(data, list) and data:
        return data[0].get("Token")
    if isinstance(data, dict):
        return data.get("Token")
    # legacy string token
    if isinstance(data, str):
        return data
    return None


def _legacy_login_success(resp: Optional[Dict[str, Any]]) -> bool:
    # legacy shape: {"Status": 1, "Data": "TOKEN"}
    return bool(isinstance(resp, dict) and resp.get("Status") == 1 and resp.get("Data"))


def _normalize_nia_mobile(value: Any) -> str:
    """
    NIA host validation expects a UAE mobile in 9 digits without leading 0,
    e.g. 501234567 (prefix 50 or 52-59).
    """
    if value is None:
        return ""
    s = re.sub(r"\D", "", str(value).strip())
    if not s:
        return ""
    while s.startswith("00"):
        s = s[2:]
    if s.startswith("971") and len(s) >= 12:
        s = s[3:]  # 971 + 9 digits
    if s.startswith("05") and len(s) == 10:
        s = s[1:]  # drop leading 0 -> 9 digits
    return s


def _normalize_nia_datetime(value: Any) -> str:
    """
    NIA CreateQuote validates some fields as DateTime (see 400 validation errors).
    The NIA API in practice accepts US-style `MM/dd/yyyy`. Convert common `dd/MM/yyyy`
    (from DIC payloads) into `MM/dd/yyyy` when unambiguous (day > 12).
    """
    if value is None:
        return ""
    s = str(value).strip()
    if not s:
        return ""

    # Already mm/dd/yyyy (or ambiguous): keep if parseable.
    if re.fullmatch(r"\d{1,2}/\d{1,2}/\d{4}", s):
        a, b, y = s.split("/")
        try:
            mm = int(a)
            dd = int(b)
            yy = int(y)
        except ValueError:
            return s

        # If looks like dd/MM (day>12) swap
        if mm > 12 and dd <= 12:
            mm, dd = dd, mm
        try:
            datetime(yy, mm, dd)
        except ValueError:
            return s
        return f"{mm:02d}/{dd:02d}/{yy}"

    # ISO yyyy-mm-dd
    m = re.fullmatch(r"(\d{4})-(\d{2})-(\d{2})", s)
    if m:
        yy, mm, dd = int(m.group(1)), int(m.group(2)), int(m.group(3))
        try:
            datetime(yy, mm, dd)
        except ValueError:
            return s
        return f"{mm:02d}/{dd:02d}/{yy}"

    return s


class NIAProvider(BaseProvider):
    """
    NIA Insurance — motor quotation API (Assuretech-style JSON per NID folder samples).
    """

    def __init__(self, api_key: str = None, base_url: str = None):
        # Token is sent via HTTP Authorization header for CreateQuote.
        resolved_base_url = base_url or os.environ.get("NIA_BASE_URL")
        super().__init__(api_key=None, base_url=resolved_base_url)
        self.provider_name = os.environ.get("NIA_PROVIDER_DISPLAY_NAME", "NIA Insurance Online")
        self.token: Optional[str] = None
        # Legacy login (Api/Auth/Login)
        # Defaults provided by user (can be overridden by env).
        self.username: str = os.environ.get("NIA_USERNAME", "amir.basha@promiseinsure.com")
        self.password: str = os.environ.get("NIA_PASSWORD", "Mfapidxb!2025")
        self.login_mode: str = os.environ.get("NIA_LOGIN_MODE", "EMAIL")
        self.user_id: str = os.environ.get("NIA_USER_ID", self.username)
        self.agent_code: str = os.environ.get("NIA_AGENT_CODE", "007624")
        self.scheme: str = os.environ.get("NIA_SCHEME", "1005")
        self.product: str = os.environ.get("NIA_PRODUCT", "1005")
        self.login_path = NIA_LOGIN_PATH
        self.create_quote_path = NIA_CREATE_QUOTE_PATH
        self.token_obtained_at: float = 0.0
        self.token_ttl_s: float = float(os.environ.get("NIA_TOKEN_TTL_S", "2700"))  # 45 min default
        self.http_max_retries = int(os.environ.get("NIA_HTTP_RETRIES", "3"))
        self.timeout = int(os.environ.get("NIA_TIMEOUT_S", "60"))
        self.request_timeout = (15.0, float(os.environ.get("NIA_READ_TIMEOUT_S", "120")))

        if not self.base_url:
            api_logger.warning(
                "NIA base_url not configured. Set InsuranceProvider.api_base_url or env NIA_BASE_URL."
            )

    def _request_id(self, data: Dict[str, Any]) -> str:
        return str(
            data.get("request_id")
            or _get_field(data, "request_id")
            or uuid.uuid4()
        )

    def _token_valid(self) -> bool:
        if not self.token:
            return False
        return (time.time() - self.token_obtained_at) < self.token_ttl_s

    def authenticate(self) -> Optional[str]:
        """Auth: Api/Auth/Login (username/password/loginMode)."""
        if not self.base_url:
            return None

        if self.username and self.password:
            payload = {
                "username": self.username,
                "password": self.password,
                "loginMode": self.login_mode,
            }
            api_logger.info(f"NIA Login | username={self.username}")
            response, _rt = self._make_request(
                method="POST",
                endpoint=self.login_path,
                json=payload,
                headers={},
            )
            # Some deployments respond with a doc-style status list even on Api/Auth/Login.
            if response and (_legacy_login_success(response) or _login_success(response)):
                tok = _extract_token(response)
                if tok:
                    self.token = tok
                    self.token_obtained_at = time.time()
                    api_logger.info("NIA Login success")
                    return self.token
            api_logger.warning(f"NIA Login failed | response={response}")
        return None

    def _ensure_token(self) -> bool:
        if self._token_valid():
            return True
        return bool(self.authenticate())

    def _map_emirate(self, raw: str) -> str:
        s = _to_str(raw).upper()
        if s in _EMIRATE_CODE_TO_REGN:
            return _EMIRATE_CODE_TO_REGN[s]
        if re.match(r"^\d{2}$", s) and s in _EMIRATE_CODE_TO_REGN:
            return _EMIRATE_CODE_TO_REGN[s]
        return "DXB"

    def _build_create_quote_payload(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Build the flat JSON body for Api/Motor/CreateQuote as per the working Postman sample you provided
        (Pol*/Veh* fields at top-level; token goes in HTTP Authorization header).
        """
        sum_insured = _safe_float(data.get("sum_insured"), 100000.0)
        insured_first = _to_str(_get_field(data, "first_name")) or _to_str(_get_field(data, "insuredName_en")) or "ABC"
        insured_last = _to_str(_get_field(data, "last_name")) or "2"
        email = _to_str(_get_field(data, "email")) or _to_str(_get_field(data, "emailAddress")) or "garg@email.com"
        mobile_raw = _to_str(_get_field(data, "mobile")) or _to_str(_get_field(data, "mobileNumber")) or "5555555"
        mobile = _normalize_nia_mobile(mobile_raw) or mobile_raw
        dob = _to_str(_get_field(data, "dob")) or _to_str(_get_field(data, "dateOfBirth")) or "12/10/1988"
        eid = _to_str(_get_field(data, "nid")) or _to_str(_get_field(data, "nationalId")) or "784-1995-5555555-5"
        chassis = _to_str(_get_field(data, "chassis_number")) or _to_str(_get_field(data, "chassisNumber")) or "RKLBB0BE4P0048836"

        make = _to_str(_get_field(data, "make_code")) or _to_str(_get_field(data, "VehMake")) or "009"
        model = _to_str(_get_field(data, "model_code")) or _to_str(_get_field(data, "VehModel")) or "9195"
        body = _to_str(_get_field(data, "body_type")) or _to_str(_get_field(data, "VehBodyType")) or "001"
        year = _to_str(_get_field(data, "year")) or _to_str(_get_field(data, "VehMfgYear")) or "2023.0"

        lic_to = _to_str(_get_field(data, "licenseToDt")) or _to_str(_get_field(data, "PolAssrVehLicExpDt")) or "10/05/2026"
        lic_from = _to_str(_get_field(data, "licenseFmDt")) or _to_str(_get_field(data, "PolAssrVehLicIssDt")) or "10/05/2020"
        lic_place = _to_str(_get_field(data, "PolAssrVehLicIssPlace")) or "1000"

        reg_dt = _to_str(_get_field(data, "regDt")) or _to_str(_get_field(data, "VehRegnDt")) or "10/05/2021"
        ncd_years = _to_str(_get_field(data, "ncdYears")) or _to_str(_get_field(data, "VehNcdYears")) or "1"
        brand_new = _to_str(_get_field(data, "isVehBrandNew")) or _to_str(_get_field(data, "VehBrandNewYn")) or "Y"
        agency_type = _to_str(_get_field(data, "agencyRepairYn")) or _to_str(_get_field(data, "VehAgencyType")) or "N"

        # Mandatory fields observed from live NIA validation errors.
        assr_age = _to_str(_get_field(data, "PolAssrAge")) or _to_str(data.get("age")) or ""
        assr_phone = _to_str(_get_field(data, "PolAssrPhone")) or _to_str(_get_field(data, "phoneNumber")) or mobile
        assr_nation = _to_str(_get_field(data, "PolAssrNation")) or _to_str(_get_field(data, "nationality")) or ""
        # Per latest requirement: don't expose VehBodyColor3 in UI; send empty string.
        # Rental/leasing/limousine question should map to VehServiceType (Yes/No).
        veh_body_color3 = ""
        veh_service_type = (
            _to_str(_get_field(data, "VehServiceType"))
            or _to_str(_get_field(data, "veh_service_type"))
            or "No"
        )

        return {
            "PolRefNo": "",
            "PolPartyCode": _to_str(_get_field(data, "party_code")) or "201001",
            "PolDeptCode": "10",
            "PolDivnCode": "813",
            "PolAssrCode": _to_str(_get_field(data, "assr_code")) or "201001",
            "PolAssrType": "100",
            "PolAssrName": insured_first,
            "PolAssrMobile": mobile,
            "PolAssrEmail": email,
            "PolAssrCivilId": eid,
            "PolAssrAge": assr_age,
            "PolAssrNation": assr_nation,
            "PolSchCode": "1000",
            "PolAssrDob": _normalize_nia_datetime(dob),
            "PolPrevInsValidYn": "Y",
            "PolProdCode": _to_str(_get_field(data, "PolProdCode")) or "1002",
            "PolSiCurrCode": "101",
            "PolPremCurrCode": "101",
            "PolSchemeType": "2",
            "PolAssrVehLicExpDt": _normalize_nia_datetime(lic_to),
            "PolAssrVehLicIssDt": _normalize_nia_datetime(lic_from),
            "PolAssrVehLicIssPlace": lic_place,
            "PolPrevPolNo": "0",
            "PolAssrLastName": insured_last,
            "PolAssrPhone": assr_phone,
            "PolAssrTradeLicNo": "",
            "VehMake": make,
            "VehModel": model,
            "VehBodyType": body,
            "VehMfgYear": year,
            "VehNoSeats": _to_str(_get_field(data, "VehNoSeats")) or "5.0",
            "VehBrandNewYn": brand_new,
            "VehRegnDt": _normalize_nia_datetime(reg_dt),
            "VehUsage": _to_str(_get_field(data, "VehUsage")) or "1001",
            "VehNoCylinder": _to_str(_get_field(data, "VehNoCylinder")) or "05",
            "VehFcValue": _to_str(_get_field(data, "VehFcValue")) or str(sum_insured),
            "VehAge": _to_str(_get_field(data, "VehAge")) or "1.0",
            "VehAgencyType": agency_type,
            "VehCc": _to_str(_get_field(data, "VehCc")) or "",
            "VehChassisNo": chassis,
            "VehLoadCapacity": _to_str(_get_field(data, "VehLoadCapacity")) or "0",
            "VehNcdYears": ncd_years,
            "VehOffroadYn": _to_str(_get_field(data, "VehOffroadYn")) or "N",
            "VehPrevInsType": _to_str(_get_field(data, "VehPrevInsType")) or "1",
            "VehRegion": _to_str(_get_field(data, "VehRegion")) or "GCC",
            "VehTrim": _to_str(_get_field(data, "VehTrim")) or "XLI",
            "VehNoDoors": _to_str(_get_field(data, "VehNoDoors")) or "4",
            "VehBackValue": _to_str(_get_field(data, "VehBackValue")) or "111",
            "VehBackValueDesc": _to_str(_get_field(data, "VehBackValueDesc")) or "2",
            "VehAccident": _to_str(_get_field(data, "VehAccident")) or "N",
            "VehDriverExperience": _to_str(_get_field(data, "VehDriverExperience")) or "1",
            "VehRegnCardExp": _to_str(_get_field(data, "VehRegnCardExp")) or "N",
            "PolPrevExpDt": _normalize_nia_datetime(_to_str(_get_field(data, "PolPrevExpDt")) or "05/09/2017"),
            "VehOdometer": _to_str(_get_field(data, "VehOdometer")) or "1",
            "VehBodyColor3": veh_body_color3,
            # Some deployments have odd field-name normalization; send a trailing-space alias defensively.
            "VehBodyColor3 ": veh_body_color3,
            "VehServiceType": veh_service_type,
        }

    def normalize_quotation_response(
        self, raw: Dict[str, Any], response_time_ms: int
    ) -> Dict[str, Any]:
        """Normalize quote response (best-effort)."""
        data = raw.get("Data") or {}
        covers = raw.get("Covers") or []
        premium = 0.0
        benefits: List[str] = []
        for c in covers:
            prem = _safe_float(c.get("Premium"), 0.0)
            premium += prem
            desc = c.get("Description") or {}
            label = ""
            if isinstance(desc, dict):
                label = desc.get("Eng") or desc.get("Ar") or ""
            else:
                label = str(desc)
            sel = (c.get("Selected") or "").upper()
            if label and (sel == "Y" or prem > 0):
                benefits.append(label)

        plan_name = data.get("ProdName") or data.get("ProdNameAr") or "Motor Plan"
        quotation_no = raw.get("QuotationNo") or ""

        return {
            "provider": self.provider_name,
            "provider_name": self.provider_name,
            "logo": "",
            "prod_code": data.get("ProdCode"),
            "plan_name": plan_name,
            "premium": premium,
            "base_price": premium,
            "vat": 0.0,
            "currency": "AED",
            "coverage": _safe_float(data.get("SumInsured"), 0.0),
            "quote_id": quotation_no,
            "reference_no": quotation_no,
            "buy_now_url": "",
            "vehicle_details": {},
            "benefits": benefits,
            "raw_response": raw,
            "response_time_ms": response_time_ms,
            "success": True,
            "error": None,
            "failure_reason": None,
            "provider_id": "nia_online",
        }

    def get_quote(self, data: Dict[str, Any]) -> Optional[List[Dict[str, Any]]]:
        """Get a motor quote (supports both legacy + doc flows)."""
        rid = self._request_id(data)
        if not self.base_url:
            return [
                {
                    "provider": self.provider_name,
                    "provider_id": "nia_online",
                    "success": False,
                    "failure_reason": "config_error:missing_base_url",
                    "response_time_ms": 0,
                    "error": "NIA base_url not configured (set InsuranceProvider.api_base_url or env NIA_BASE_URL).",
                }
            ]
        if not self._ensure_token():
            return [
                {
                    "provider": self.provider_name,
                    "provider_id": "nia_online",
                    "success": False,
                    "failure_reason": "auth_failed",
                    "response_time_ms": 0,
                    "error": "NIA authentication failed",
                }
            ]

        body = self._build_create_quote_payload(data)
        # High-signal log for mandatory field debugging (avoid logging full payload).
        api_logger.info(
            "NIA CreateQuote payload markers | request_id=%s VehBodyColor3=%r PolAssrAge=%r PolAssrNation=%r",
            rid,
            body.get("VehBodyColor3"),
            body.get("PolAssrAge"),
            body.get("PolAssrNation"),
        )

        # Quote request (Api/Motor/CreateQuote) — token in Authorization header.
        api_logger.info(f"NIA CreateQuote | request_id={rid}")
        # Some NIA deployments accept either Bearer or raw token in Authorization header.
        headers = {"Authorization": f"Bearer {self.token}"}
        response, response_time = self._make_request(
            method="POST",
            endpoint=self.create_quote_path,
            json=body,
            headers=headers,
        )
        if isinstance(response, dict) and response.get("_http_status") == 401:
            # Retry once without Bearer format (some APIs expect raw token).
            headers = {"Authorization": str(self.token)}
            response, response_time = self._make_request(
                method="POST",
                endpoint=self.create_quote_path,
                json=body,
                headers=headers,
            )

        if not response:
            err = getattr(self, "_last_transport_error", None)
            return [
                {
                    "provider": self.provider_name,
                    "provider_id": "nia_online",
                    "success": False,
                    "failure_reason": f"transport_error:{err}" if err else "no_response",
                    "response_time_ms": response_time,
                    "error": err or "no_response",
                }
            ]

        # Legacy quote response (PlanDetails list)
        if _legacy_quote_success(response):
            data_block = response.get("Data") or {}
            ref_no = data_block.get("ReferenceNo") or ""
            plans = data_block.get("PlanDetails") or []
            out: List[Dict[str, Any]] = []
            if isinstance(plans, list) and plans:
                for p in plans:
                    if not isinstance(p, dict):
                        continue
                    norm = self.normalize(p)
                    norm.update(
                        {
                            "provider_id": "nia_online",
                            "provider_name": self.provider_name,
                            "logo": "",
                            # Preserve computed base_price (excludes optional covers).
                            "base_price": float(norm.get("base_price") or 0),
                            "vat": 0,
                            "currency": "AED",
                            "quote_id": ref_no,
                            "reference_no": ref_no,
                            "buy_now_url": "",
                            # Preserve computed vehicle details (excess from Deductable).
                            "vehicle_details": norm.get("vehicle_details") or {},
                            "raw_response": response,
                            "response_time_ms": response_time,
                            "success": True,
                            "error": None,
                            "failure_reason": None,
                        }
                    )
                    norm["coverage"] = _safe_float(data.get("sum_insured"), _safe_float(norm.get("coverage"), 0.0))
                    out.append(norm)
            if out:
                return out
            return [
                {
                    "provider": self.provider_name,
                    "provider_id": "nia_online",
                    "success": False,
                    "failure_reason": "empty_plan_list",
                    "raw_response": response,
                    "response_time_ms": response_time,
                    "error": "No plans returned",
                }
            ]

        # Unsupported / unexpected response
        if not _quotation_success(response):
            msg = ""
            st = response.get("Status")
            if isinstance(st, dict):
                msg = st.get("Description") or str(st.get("Code"))
            return [
                {
                    "provider": self.provider_name,
                    "provider_id": "nia_online",
                    "success": False,
                    "failure_reason": msg or "quotation_failed",
                    "raw_response": response,
                    "response_time_ms": response_time,
                    "error": msg,
                }
            ]

        norm = self.normalize_quotation_response(response, response_time)
        # Fix coverage from payload
        norm["coverage"] = _safe_float(data.get("sum_insured"), 0.0)
        norm["mandatory_covers"] = response.get("Covers") or []
        return [norm]

    def normalize(self, plan_data: Dict) -> Dict:
        """Backward compatibility for tests expecting normalize() on a plan fragment."""
        covers = plan_data.get("Covers", []) or []

        def _desc(c: Dict[str, Any]) -> str:
            d = c.get("Description")
            if isinstance(d, str):
                return d.strip()
            if isinstance(d, dict):
                return str(d.get("Eng") or d.get("Ar") or "").strip()
            return ""

        def _is_optional_cover(cover: Dict[str, Any], desc: str) -> bool:
            # Primary signal: CoverFlag == "OC" per requirement.
            try:
                if str(cover.get("CoverFlag") or "").strip().upper() == "OC":
                    return True
            except Exception:
                pass
            # Fallback: description heuristics.
            s = (desc or "").lower()
            return ("rent-a-car" in s) or ("rent a car" in s) or ("personal accident" in s) or ("pab" in s)

        benefit_strings: List[str] = []
        benefits_struct = {
            "loss_or_damage": False,
            "third_party_liability": False,
            "blood_money": False,
            "fire_theft": False,
            "storm_flood": False,
            "natural_perils": False,
            "repairs": None,
            "emergency_medical": False,
            "personal_belongings": False,
            "oman_cover": False,
            "off_road_cover": False,
            "guaranteed_repairs": False,
            "breakdown_recovery": False,
            "ambulance_cover": False,
            "windscreen_damage": False,
        }
        optional_struct = {
            "driver_cover": False,
            "passenger_cover": False,
            "hire_car_benefit": False,
        }

        premium_total = 0.0
        base_total = 0.0
        excess_val: Optional[float] = None

        for c in covers:
            if not isinstance(c, dict):
                continue
            desc = _desc(c)
            if desc:
                benefit_strings.append(desc)

            prem = _safe_float(c.get("CoverPremFc") or c.get("Premium"), 0.0)
            premium_total += prem
            if not _is_optional_cover(c, desc):
                base_total += prem

            s = desc.lower()
            if "loss" in s and "damage" in s:
                benefits_struct["loss_or_damage"] = True
                # Business rule: comprehensive loss & damage implies fire/theft.
                benefits_struct["fire_theft"] = True
            if "third party liability" in s:
                benefits_struct["third_party_liability"] = True
            if "natural calamity" in s:
                benefits_struct["storm_flood"] = True
                benefits_struct["natural_perils"] = True
            if "emergency medical" in s:
                benefits_struct["emergency_medical"] = True
            if "personal belongings" in s:
                benefits_struct["personal_belongings"] = True
            if "roadside assistance" in s or "24 hrs roadside" in s:
                benefits_struct["breakdown_recovery"] = True
            if "oman" in s:
                benefits_struct["oman_cover"] = True
            if "windscreen" in s:
                benefits_struct["windscreen_damage"] = True

            if "personal accident" in s and "driver" in s:
                optional_struct["driver_cover"] = True
            if "personal accident" in s and "passenger" in s:
                optional_struct["passenger_cover"] = True
            if "rent-a-car" in s or "rent a car" in s:
                optional_struct["hire_car_benefit"] = True

            if "deduct" in s:
                excess_val = prem

        provider_display = "NIA ONLINE"
        vehicle_details = {
            "excess": (f"{int(excess_val)} AED" if excess_val is not None else "TBA"),
            "ancillary_excess": "TBA",
            "vehicle_value": "N/A",
        }

        return {
            "provider": provider_display,
            "provider_name": provider_display,
            "prod_code": plan_data.get("Code"),
            "plan_name": plan_data.get("Name"),
            "premium": float(premium_total),
            "base_price": float(base_total),
            "coverage": 100000.0,
            "benefits": benefit_strings,
            "vehicle_details": vehicle_details,
            "benefits_struct": benefits_struct,
            "optional_covers_struct": optional_struct,
        }

    def save_quote_with_plan(self, ref_no: str, prod_code: str, selected_covers: list) -> Optional[str]:
        """SaveQuoteWithPlan — SelectedCoverData shape per SaveQuoteWithPlanReq.txt."""
        if not self._ensure_token():
            return None
        items = []
        cover_rows = []
        for c in selected_covers or []:
            if isinstance(c, dict):
                cover_rows.append(
                    {
                        "Code": c.get("Code"),
                        "CvrType": c.get("CvrType", "BC"),
                        "Premium": c.get("Premium", 0),
                    }
                )
        items.append(
            {
                "QuotNo": ref_no,
                "ProdCode": prod_code,
                "SelectedCovers": cover_rows,
            }
        )
        payload = {
            "Authentication": {"Token": self.token, "UserId": self.user_id},
            "SelectedCoverData": items,
        }
        response, _ = self._make_request(
            method="POST",
            endpoint="Api/Motor/SaveQuoteWithPlan",
            json=payload,
            headers={},
        )
        if response and _parse_status_code(response) == "1003":
            return ref_no
        if response and str(_parse_status_code(response)) in ("1", "1003"):
            return ref_no
        return None

    def approve_policy(self, ref_no: str) -> Optional[str]:
        """ApprovePolicy — ApprovePolicyReq.txt shape."""
        if not self._ensure_token():
            return None
        payload = {
            "Authentication": {"Token": self.token, "UserId": self.user_id},
            "ApprovePolicyData": {"QuotNo": ref_no},
        }
        response, _ = self._make_request(
            method="POST",
            endpoint="Api/Motor/ApprovePolicy",
            json=payload,
            headers={},
        )
        if not response:
            return None
        st = response.get("Status")
        if isinstance(st, dict) and str(st.get("Code")) == "2020":
            return st.get("PolicyNo")
        if isinstance(st, dict) and st.get("PolicyNo"):
            return st.get("PolicyNo")
        return None


class ICICIProvider(NIAProvider):
    """Backward compatibility for DB seeds pointing to ICICIProvider in this module."""

    pass
