from __future__ import annotations

from typing import Optional, Dict, Any, List, Tuple
import base64
import json
import logging
import re
import time
import uuid
from datetime import datetime

from .base import BaseProvider


logger = logging.getLogger(__name__)
api_logger = logging.getLogger("api_providers")


class DICProvider(BaseProvider):
    def __init__(self, api_key: str = None, base_url: str = None):
        super().__init__(
            api_key=api_key or 'dic_uae_test_key_001',
            # Spec (PDF v0.0.3): Test End Point is https://uatbrokerportal.dubins.ae
            # In production this should be overridden via DB (`InsuranceProvider.api_base_url`) or env config.
            base_url=base_url or 'https://uatbrokerportal.dubins.ae/'
        )
        self.provider_name = 'DIC Insurance Broker UAE'
        self.token = None
        self.token_expiry_epoch_s: Optional[float] = None

        # Default UAT credentials (can/should be overridden via DB config in production).
        self.username = "PROMISE_API"
        self.password = "Prom#26@1SE"

        # GenerateQuote can include large base64 document payloads.
        self.timeout = 120
        # Wider read window than a single scalar — avoids premature client-side read timeouts; large uploads need it.
        self.request_timeout = (25.0, 180.0)
        # UAT sometimes resets connections on big POSTs; a few retries usually succeeds (Postman often retries implicitly).
        self.http_max_retries = 5

    def _now_s(self) -> float:
        return time.time()

    def _is_token_valid(self) -> bool:
        if not self.token:
            return False
        if not self.token_expiry_epoch_s:
            # If we can't parse expiry, treat as valid for this process lifetime.
            return True
        # Refresh a little early to avoid boundary failures.
        return self._now_s() < (self.token_expiry_epoch_s - 30)

    def _generate_request_id(self, data: Dict[str, Any]) -> str:
        req_id = (
            data.get("request_id")
            or (data.get("additional_details") or {}).get("request_id")
            or str(uuid.uuid4())
        )
        return str(req_id)

    def _decode_jwt_exp(self, token: str) -> Optional[float]:
        """
        Best-effort JWT `exp` claim parse (seconds since epoch).
        Token format in docs: JWT Bearer token.
        """
        try:
            parts = token.split(".")
            if len(parts) != 3:
                return None
            payload_b64 = parts[1]
            # Add padding for base64url
            payload_b64 += "=" * (-len(payload_b64) % 4)
            payload_raw = base64.urlsafe_b64decode(payload_b64.encode("utf-8")).decode("utf-8")
            payload = json.loads(payload_raw)
            exp = payload.get("exp")
            if exp is None:
                return None
            return float(exp)
        except Exception:
            return None

    def authenticate(self, *, request_id: Optional[str] = None) -> Optional[str]:
        """Standard Authentication API for DIN/DIC"""
        payload = {
            # Docs (PDF + Postman): userName + password
            "userName": self.username,
            "password": self.password,
        }
        headers = {}
        if request_id:
            headers["X-REQUEST-ID"] = request_id
        response, _ = self._make_request(
            method="POST",
            endpoint="api/v1/User/Auth",
            headers=headers,
            json=payload
        )
        if response and response.get("status") == 1 and response.get("data"):
            self.token = response.get("data")
            self.token_expiry_epoch_s = self._decode_jwt_exp(self.token)
            api_logger.info(
                f"DIC auth success | provider={self.provider_name} request_id={request_id} "
                f"token_expiry_epoch_s={self.token_expiry_epoch_s}"
            )
            return self.token

        api_logger.warning(
            f"DIC auth failed | provider={self.provider_name} request_id={request_id} response={response}"
        )
        return None

    def _get_field(self, data: Dict[str, Any], key: str, default: Any = None) -> Any:
        # Prefer `additional_details` as it's where motor-specific fields should live.
        ad = data.get("additional_details") or {}
        if key in ad:
            return ad.get(key)
        return data.get(key, default)

    def _normalize_dic_date(self, value: Any) -> Optional[str]:
        """
        DIC expects `dd/MM/yyyy` with **slashes** (see API error messages).
        Accepts common inputs: ISO, dd/mm/yyyy, mm/dd/yyyy when unambiguous, and
        dashed variants (dd-mm-yyyy / mm-dd-yyyy).
        """
        if value is None:
            return None
        s = str(value).strip()
        if not s:
            return None

        # Already valid API format
        if re.fullmatch(r"\d{2}/\d{2}/\d{4}", s):
            try:
                datetime.strptime(s, "%d/%m/%Y")
                return s
            except ValueError:
                pass

        # ISO YYYY-MM-DD
        m = re.fullmatch(r"(\d{4})-(\d{2})-(\d{2})", s)
        if m:
            y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
            try:
                datetime(y, mo, d)
                return f"{d:02d}/{mo:02d}/{y}"
            except ValueError:
                return None

        # d/m/yyyy or d-m-yyyy with 1–2 digit day/month
        m = re.fullmatch(r"(\d{1,2})[/-](\d{1,2})[/-](\d{4})", s)
        if not m:
            return None

        a, b, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if y < 1900 or y > 2100:
            return None

        # Disambiguate mm/dd vs dd/mm:
        # - second > 12 → first must be month (MM/DD)
        # - first > 12 → first must be day (DD/MM)
        # - else prefer DD/MM (UAE / API spec)
        if b > 12 and a <= 12:
            month, day = a, b
        elif a > 12 and b <= 12:
            day, month = a, b
        else:
            day, month = a, b

        try:
            datetime(y, month, day)
        except ValueError:
            # Last attempt: MM/DD for ambiguous small numbers
            try:
                datetime(y, a, b)
                day, month = b, a
            except ValueError:
                return None

        return f"{day:02d}/{month:02d}/{y}"

    def _normalize_dic_mobile(self, value: Any) -> Optional[str]:
        """
        DIC rejects invalid mobiles (statusId 8012). Accepted shapes per API message:
        - `971` followed by 8 digits (11 digits total)
        - `05` followed by 8 digits (10 digits total, leading zero + UAE mobile)

        Normalizes common inputs: +971…, 00971…, 12-digit intl `9715XXXXXXXX`,
        and 9-digit `5XXXXXXXX` missing the leading 0.
        """
        if value is None:
            return None
        s = re.sub(r"\D", "", str(value).strip())
        if not s:
            return None
        while s.startswith("00"):
            s = s[2:]

        # Already valid per API wording
        if re.fullmatch(r"05\d{8}", s):
            return s
        if re.fullmatch(r"971\d{8}", s) and len(s) == 11:
            return s

        # Full UAE mobile in E.164 without +: 971 + 9 digits (5XXXXXXXX)
        if len(s) == 12 and s.startswith("971") and s[3] == "5":
            # Prefer national form required by many UAE insurer APIs
            out = "0" + s[3:]
            api_logger.debug("DIC mobileNumber normalized | intl_12_to_local_05 | out=%s", out)
            return out

        # Nine digits starting with 5 (missing leading 0)
        if re.fullmatch(r"5\d{8}", s):
            out = "0" + s
            api_logger.debug("DIC mobileNumber normalized | prefix0_added | out=%s", out)
            return out

        return s

    def _build_generate_quote_payload(self, data: Dict[str, Any]) -> Tuple[Dict[str, Any], List[str]]:
        """
        Strictly follows PDF v0.0.3 sample request keys/casing.
        Returns (payload, missing_required_fields).
        """
        # Required fields per PDF. Some keys have inconsistent casing in the PDF/Postman (plateSource vs PlateSource).
        required = [
            "insuredName",
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
            # Use Postman sample key casing:
            "PlateSource",
            "tcfNumber",
            "ncdYears",
            "trafficTranType",
            "isVehBrandNew",
            "agencyRepairYn",
            "documentLists",
        ]

        insured_name_en = self._get_field(data, "insuredName_en") or self._get_field(data, "insured_name_en")
        insured_name_ar = self._get_field(data, "insuredName_ar") or self._get_field(data, "insured_name_ar") or insured_name_en
        id_expiry_raw = self._get_field(data, "idExpiryDt") or self._get_field(data, "id_expiry_dt")
        dob_raw = self._get_field(data, "dateOfBirth") or self._get_field(data, "date_of_birth")
        lic_from_raw = self._get_field(data, "licenseFmDt") or self._get_field(data, "license_from_dt")
        lic_to_raw = self._get_field(data, "licenseToDt") or self._get_field(data, "license_to_dt")
        reg_dt_raw = self._get_field(data, "regDt") or self._get_field(data, "reg_dt")
        mobile_raw = self._get_field(data, "mobileNumber") or self._get_field(data, "mobile")

        payload: Dict[str, Any] = {
            "insuredName": {
                "en": insured_name_en,
                "ar": insured_name_ar,
            },
            "nationality": self._get_field(data, "nationality"),
            "nationalId": self._get_field(data, "nationalId") or self._get_field(data, "national_id"),
            "idExpiryDt": self._normalize_dic_date(id_expiry_raw) or id_expiry_raw,
            "dateOfBirth": self._normalize_dic_date(dob_raw) or dob_raw,
            "gender": self._get_field(data, "gender"),
            "emirate": self._get_field(data, "emirate"),
            "emailAddress": self._get_field(data, "emailAddress") or self._get_field(data, "email"),
            "mobileNumber": self._normalize_dic_mobile(mobile_raw) or mobile_raw,
            "licenseNo": self._get_field(data, "licenseNo") or self._get_field(data, "license_no"),
            "licenseFmDt": self._normalize_dic_date(lic_from_raw) or lic_from_raw,
            "licenseToDt": self._normalize_dic_date(lic_to_raw) or lic_to_raw,
            "chassisNumber": self._get_field(data, "chassisNumber") or self._get_field(data, "chassis_number"),
            "regNumber": self._get_field(data, "regNumber") or self._get_field(data, "reg_number"),
            "regDt": self._normalize_dic_date(reg_dt_raw) or reg_dt_raw,
            "plateCode": self._get_field(data, "plateCode") or self._get_field(data, "plate_code"),
            # NOTE: Postman sample uses "PlateSource" (capital P). Use that for strict match.
            "PlateSource": self._get_field(data, "PlateSource") or self._get_field(data, "plateSource") or self._get_field(data, "plate_source"),
            "tcfNumber": self._get_field(data, "tcfNumber") or self._get_field(data, "tcf_number"),
            "ncdYears": self._get_field(data, "ncdYears") or self._get_field(data, "ncd_years"),
            "trafficTranType": self._get_field(data, "trafficTranType") or self._get_field(data, "traffic_tran_type"),
            "isVehBrandNew": self._get_field(data, "isVehBrandNew") or ("Y" if self._get_field(data, "is_vehicle_brand_new") else "N"),
            "agencyRepairYn": self._get_field(data, "agencyRepairYn") or ("Y" if self._get_field(data, "agency_repair") else "N"),
            # Optional per PDF
            "bankName": self._get_field(data, "bankName") or self._get_field(data, "bank_name") or "",
            "documentLists": self._get_field(data, "documentLists") or self._get_field(data, "document_lists") or [],
        }

        # Basic required validation
        missing = []
        for key in required:
            if key == "insuredName":
                v = payload.get("insuredName") or {}
                if not v.get("en"):
                    missing.append("insuredName.en")
            elif key == "documentLists":
                # Must be present as an array; empty list is valid (no uploads / tests).
                v = payload.get("documentLists")
                if v is None or not isinstance(v, list):
                    missing.append(key)
            else:
                v = payload.get(key)
                if v is None or v == "" or v == []:
                    missing.append(key)
        return payload, missing

    def _ensure_token(self, request_id: str) -> bool:
        if self._is_token_valid():
            return True
        return bool(self.authenticate(request_id=request_id))

    def get_quote(self, data: Dict[str, Any]) -> Optional[List[Dict[str, Any]]]:
        """Generate Quote API (PDF v0.0.3: /api/v1/Insurance/GenerateQuote)"""
        request_id = self._generate_request_id(data)

        if not self._ensure_token(request_id):
            return [
                {
                    "provider": self.provider_name,
                    "provider_id": "dic_broker_uae",
                    "success": False,
                    "failure_reason": "auth_failed",
                    "response_time_ms": 0,
                }
            ]

        payload, missing = self._build_generate_quote_payload(data)
        if missing:
            api_logger.warning(
                f"DIC GenerateQuote missing required fields | request_id={request_id} missing={missing}"
            )
            return [
                {
                    "provider": self.provider_name,
                    "provider_id": "dic_broker_uae",
                    "success": False,
                    "failure_reason": f"missing_required_fields:{','.join(missing)}",
                    "raw_response": None,
                    "response_time_ms": 0,
                }
            ]

        def _sanitize_payload_for_logs(p: Dict[str, Any]) -> Dict[str, Any]:
            """
            Avoid logging huge base64 blobs. Keep enough shape to diff with Postman.
            """
            out = dict(p)
            docs = out.get("documentLists")
            if isinstance(docs, list):
                sanitized_docs = []
                for d in docs:
                    if not isinstance(d, dict):
                        continue
                    dd = dict(d)
                    b64 = dd.get("base64")
                    if isinstance(b64, str):
                        dd["base64"] = f"<base64_len={len(b64)}>"
                    sanitized_docs.append(dd)
                out["documentLists"] = sanitized_docs
            return out

        # High-signal audit log for 8021 debugging (values, casing, and date formats).
        # This is INFO (not DEBUG) because UAT troubleshooting often needs the exact payload.
        api_logger.info(
            "DIC GenerateQuote send | request_id=%s payload=%s",
            request_id,
            _sanitize_payload_for_logs(payload),
        )

        headers = {"Authorization": f"Bearer {self.token}", "X-REQUEST-ID": request_id}

        response, response_time = self._make_request(
            method="POST",
            endpoint="api/v1/Insurance/GenerateQuote",
            headers=headers,
            json=payload,
        )

        # Token might be expired/invalid: retry once.
        if response is None:
            why = "no_response"
            err = getattr(self, "_last_transport_error", None)
            if err:
                why = f"transport_error:{err}"
            return [
                {
                    "provider": self.provider_name,
                    "provider_id": "dic_broker_uae",
                    "success": False,
                    "failure_reason": why,
                    "raw_response": response,
                    "response_time_ms": response_time,
                }
            ]

        if isinstance(response, dict) and response.get("status") in (0, 2) and response.get("statusId") == "8107":
            # Unauthorized access per error codes list; re-auth and retry once.
            self.token = None
            self.token_expiry_epoch_s = None
            if self.authenticate(request_id=request_id):
                headers["Authorization"] = f"Bearer {self.token}"
                response, response_time = self._make_request(
                    method="POST",
                    endpoint="api/v1/Insurance/GenerateQuote",
                    headers=headers,
                    json=payload,
                )

        # Spec success response wraps list in `data`.
        if isinstance(response, dict) and response.get("status") == 1:
            items = response.get("data") or []
            if not isinstance(items, list):
                items = []

            normalized_quotes: List[Dict[str, Any]] = []
            for item in items:
                norm = self.normalize(item)
                norm["provider_id"] = "dic_broker_uae"
                norm["response_time_ms"] = response_time
                norm["raw_response"] = item
                norm["success"] = True
                norm.setdefault("failure_reason", None)
                normalized_quotes.append(norm)

            if not normalized_quotes:
                return [
                    {
                        "provider": self.provider_name,
                        "provider_id": "dic_broker_uae",
                        "success": False,
                        "failure_reason": "empty_quote_list",
                        "raw_response": response,
                        "response_time_ms": response_time,
                    }
                ]

            # Return ALL plans (aggregator will fan-in).
            return normalized_quotes

        # Non-success: return a single failure object (so the caller can persist/display why).
        failure_reason = None
        if isinstance(response, dict):
            failure_reason = (
                (response.get("message") or {}).get("en")
                or response.get("statusId")
                or response.get("statusCategory")
            )

        return [
            {
                "provider": self.provider_name,
                "provider_id": "dic_broker_uae",
                "success": False,
                "failure_reason": failure_reason or "quote_failed",
                "raw_response": response,
                # Include sanitized payload so UI/API consumers can diff against Postman requests.
                "sent_payload": _sanitize_payload_for_logs(payload),
                "response_time_ms": response_time,
            }
        ]

    def choose_scheme(self, prod_code: str, covers: Dict = None) -> Optional[Dict]:
        """Select Scheme API"""
        request_id = str(uuid.uuid4())
        if not self._ensure_token(request_id):
            return None

        payload = {
            "prodCode": prod_code,
            "covers": covers or {"mandatory": "", "optional": ""}
        }
        
        response, _ = self._make_request(
            method="POST",
            endpoint="api/v1/Insurance/ChooseScheme",
            headers={
                "Authorization": f"Bearer {self.token}",
                "X-REQUEST-ID": request_id,
            },
            json=payload
        )
        return response

    def get_policy(self, quotation_no: str) -> Optional[Dict]:
        """Get Policy / Payment Info API"""
        request_id = str(uuid.uuid4())
        if not self._ensure_token(request_id):
            return None

        # PDF shows endpoint `/api/v1/Insurance/GetPaymentInfo`. Parameter name is inconsistent in doc
        # (failure data says TranId, sample earlier used quotationNo in your code). Use both defensively.
        response, _ = self._make_request(
            method="GET",
            endpoint="api/v1/Insurance/GetPaymentInfo",
            headers={
                "Authorization": f"Bearer {self.token}",
                "X-REQUEST-ID": request_id,
            },
            params={"quotationNo": quotation_no, "tranId": quotation_no},
        )
        return response

    def normalize(self, response_data: Dict[str, Any]) -> Dict[str, Any]:
        """Normalized response for UI comparison"""
        prod_name = response_data.get("prodName")
        plan_name = None
        if isinstance(prod_name, dict):
            plan_name = prod_name.get("en") or prod_name.get("ar")
        else:
            plan_name = prod_name

        covers = response_data.get("covers") or {}
        mandatory = covers.get("mandatory") or []
        optional = covers.get("optional") or []
        if mandatory is None:
            mandatory = []
        if optional is None:
            optional = []

        def _cover_label(c: Dict[str, Any]) -> str:
            name = c.get("coverName")
            if isinstance(name, dict):
                return name.get("en") or name.get("ar") or str(name)
            return str(name) if name is not None else str(c.get("coverCode") or "")

        # GenerateQuote response doesn't provide a single "premium" number; it's embedded per cover.
        premium = 0.0
        for c in mandatory:
            try:
                premium += float(c.get("premium") or 0)
            except (TypeError, ValueError):
                continue

        benefits = [_cover_label(c) for c in (mandatory + optional) if _cover_label(c)]

        return {
            'provider': self.provider_name,
            'prod_code': response_data.get('prodCode'),
            'plan_name': plan_name,
            'premium': premium,
            'coverage': float(response_data.get('sumInsured', 0)),
            'quote_id': None,  # DIC spec provides quotationNo after ChooseScheme; GenerateQuote doesn't return one.
            'benefits': benefits,
            'mandatory_covers': mandatory,
            'optional_covers': optional,
        }
