from __future__ import annotations

import logging
import os
import json
from typing import Any, Dict, List, Optional, Tuple

from .base import BaseProvider

logger = logging.getLogger(__name__)
api_logger = logging.getLogger("api_providers")

try:
    # Prefer DB mapping when available (migration adds this model).
    from api_set1.models import AnoudMakeModelMapping  # type: ignore
except Exception:  # pragma: no cover
    AnoudMakeModelMapping = None  # type: ignore


def _to_str(v: Any) -> str:
    if v is None:
        return ""
    return str(v).strip()


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


class AnoudappsProvider(BaseProvider):
    """
    QIC Anoudapps aggregator provider (UAE motor).

    Docs: Anoud-API-Motor-Document-UAE-Updated.pdf
    Base URL: https://www.devapi.anoudapps.com/qicservices/aggregator
    """

    def __init__(self, api_key: str = None, base_url: str = None):
        # api_key here is expected to be a Basic auth token (either "Basic xxx" or just "xxx").
        resolved_base_url = base_url or os.environ.get(
            "ANOUDAPPS_BASE_URL", "https://www.devapi.anoudapps.com/qicservices/aggregator/"
        )
        super().__init__(api_key=None, base_url=resolved_base_url)
        self.provider_name = "Anoudapps (QIC)"

        self.company = os.environ.get("ANOUDAPPS_COMPANY", "002")
        # Prefer DB api_key, fallback to env. User shared a base64 value in the request.
        self.basic_auth = api_key or os.environ.get("ANOUDAPPS_BASIC_AUTH", "")

        # Reliability
        self.timeout = int(os.environ.get("ANOUDAPPS_TIMEOUT_S", "60"))
        self.request_timeout = (10.0, float(os.environ.get("ANOUDAPPS_READ_TIMEOUT_S", "90")))
        self.http_max_retries = int(os.environ.get("ANOUDAPPS_HTTP_RETRIES", "3"))

    def _get_field(self, data: Dict[str, Any], key: str, default: Any = None) -> Any:
        ad = data.get("additional_details") or {}
        if key in ad:
            return ad.get(key)
        return data.get(key, default)

    def _auth_header(self) -> str:
        tok = _to_str(self.basic_auth)
        if not tok:
            return ""
        if tok.lower().startswith("basic "):
            return tok
        return f"Basic {tok}"

    def _build_tariff_payload(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Build request payload for POST motor/tariff.
        Prefer fields from additional_details if present (so UI can supply exact codes).
        """
        return {
            "insuredName": _to_str(self._get_field(data, "insuredName")) or _to_str(self._get_field(data, "insuredName_en")) or "Insured",
            "policyFromDate": _to_str(self._get_field(data, "policyFromDate")),
            "makeCode": _to_str(self._get_field(data, "makeCode")),
            "modelCode": _to_str(self._get_field(data, "modelCode")),
            "modelYear": _to_str(self._get_field(data, "modelYear")) or _to_str(self._get_field(data, "year")),
            "sumInsured": _safe_float(self._get_field(data, "sumInsured", data.get("sum_insured")), 0.0),
            "vehicleType": _to_str(self._get_field(data, "vehicleType")) or "",
            "vehicleUsage": _to_str(self._get_field(data, "vehicleUsage")) or "",
            "noOfCylinder": _to_str(self._get_field(data, "noOfCylinder")),
            "nationality": _to_str(self._get_field(data, "nationality")),
            "seatingCapacity": _to_str(self._get_field(data, "seatingCapacity")) or "",
            "firstRegDate": _to_str(self._get_field(data, "firstRegDate")) or _to_str(self._get_field(data, "regDt")),
            "gccSpec": int(self._get_field(data, "gccSpec", 1) or 0),
            "previousInsuranceValid": int(self._get_field(data, "previousInsuranceValid", 1) or 0),
            "totalLoss": int(self._get_field(data, "totalLoss", 0) or 0),
            "driverDOB": _to_str(self._get_field(data, "driverDOB")) or _to_str(self._get_field(data, "dateOfBirth")),
            "noClaimYear": int(self._get_field(data, "noClaimYear", 0) or 0),
            "selfDeclarationYear": int(self._get_field(data, "selfDeclarationYear", 0) or 0),
            "chassisNo": _to_str(self._get_field(data, "chassisNo")) or _to_str(self._get_field(data, "chassisNumber")),
            "driverExp": _to_str(self._get_field(data, "driverExp")),
            "admeId": int(self._get_field(data, "admeId", 0) or 0),
            "regnLocation": _to_str(self._get_field(data, "regnLocation")) or "",
        }

    def _fetch_net_premium(self, quote_no: str, schemes: List[Dict[str, Any]]) -> Tuple[Optional[Dict[str, Any]], int]:
        """
        POST motor/netPremium.
        The Postman collection sends: {quoteNo, schemes:[{schemeCode, productCode}]}
        """
        headers = {
            "company": self.company,
            "Authorization": self._auth_header(),
            "Content-Type": "application/json",
        }
        body = {
            "quoteNo": quote_no,
            "schemes": [{"schemeCode": s.get("schemeCode"), "productCode": s.get("productCode")} for s in schemes if isinstance(s, dict)],
        }
        return self._make_request(
            method="POST",
            endpoint="motor/netPremium",
            headers=headers,
            params={"company": self.company},
            json=body,
        )

    def _fetch_vehicle_details(self, vin: str) -> Tuple[Optional[Dict[str, Any]], int]:
        """
        POST bayanaty/vehicleDetails?company=002
        Used to derive makeCode/modelCode/admeId when only chassis/VIN is available.
        """
        headers = {
            "company": self.company,
            "Authorization": self._auth_header(),
            "Content-Type": "application/json",
        }
        return self._make_request(
            method="POST",
            endpoint="bayanaty/vehicleDetails",
            headers=headers,
            params={"company": self.company},
            json={"Vin": vin},
        )

    def get_quote(self, data: Dict[str, Any]) -> Optional[List[Dict[str, Any]]]:
        headers = {
            "company": self.company,
            "Authorization": self._auth_header(),
            "Content-Type": "application/json",
        }
        if not headers.get("Authorization"):
            return [
                {
                    "provider": "Anoudapps (QIC)",
                    "provider_id": "anoudapps_uae",
                    "success": False,
                    "failure_reason": "config_error:missing_basic_auth",
                    "error": "Missing Anoudapps Basic Auth (set InsuranceProvider.api_key or env ANOUDAPPS_BASIC_AUTH).",
                    "response_time_ms": 0,
                }
            ]

        payload = self._build_tariff_payload(data)

        def _sanitize_payload_for_logs(p: Dict[str, Any]) -> Dict[str, Any]:
            return dict(p)
        
        def _trim_for_log(obj: Any, limit: int = 5000) -> str:
            try:
                return json.dumps(obj, ensure_ascii=False)[:limit]
            except Exception:
                return str(obj)[:limit]

        # If codes missing but VIN/chassis present, try Bayanaty lookup first.
        if not payload.get("makeCode") or not payload.get("modelCode") or not payload.get("admeId"):
            vin = _to_str(payload.get("chassisNo"))
            if vin:
                vd, _vdt = self._fetch_vehicle_details(vin)
                if isinstance(vd, dict):
                    primary = None
                    try:
                        vf = (vd.get("vehicleFeatures") or vd.get("VehicleFeatures") or {})
                        primary = (vf.get("primaryFeatures") or vf.get("PrimaryFeatures") or {})
                        if isinstance(primary, list):
                            primary = primary[0] if primary else {}
                    except Exception:
                        primary = None
                    if isinstance(primary, dict):
                        make_obj = (primary.get("make") or primary.get("Make") or {}) if isinstance(primary, dict) else {}
                        model_obj = (primary.get("model") or primary.get("Model") or {}) if isinstance(primary, dict) else {}
                        trim_obj = (primary.get("trim") or primary.get("Trim") or {}) if isinstance(primary, dict) else {}
                        make_id = (make_obj.get("id") or make_obj.get("Id")) if isinstance(make_obj, dict) else None
                        model_id = (model_obj.get("id") or model_obj.get("Id")) if isinstance(model_obj, dict) else None
                        trim_id = (trim_obj.get("id") or trim_obj.get("Id")) if isinstance(trim_obj, dict) else None
                        year = primary.get("modelYear") or primary.get("ModelYear")
                        if make_id and not payload.get("makeCode"):
                            payload["makeCode"] = str(make_id)
                        if model_id and not payload.get("modelCode"):
                            payload["modelCode"] = str(model_id)
                        if trim_id and not payload.get("admeId"):
                            try:
                                payload["admeId"] = int(trim_id)
                            except Exception:
                                payload["admeId"] = trim_id
                        if year and not payload.get("modelYear"):
                            payload["modelYear"] = str(year)

                    # If Bayanaty provides valuation range, clamp sumInsured to a valid value.
                    vv = (vd.get("vehicleValues") or vd.get("VehicleValues") or {}) if isinstance(vd, dict) else {}
                    if isinstance(vv, dict):
                        try:
                            vmin = float(vv.get("minimum")) if vv.get("minimum") is not None else None
                            vmax = float(vv.get("maximum")) if vv.get("maximum") is not None else None
                            vact = float(vv.get("actual")) if vv.get("actual") is not None else None
                            cur = float(payload.get("sumInsured") or 0)
                        except Exception:
                            vmin = vmax = vact = None
                            cur = None
                        if cur is not None and (vmin is not None or vmax is not None or vact is not None):
                            if vmin is not None and cur < vmin:
                                payload["sumInsured"] = vact if vact is not None else vmin
                            elif vmax is not None and cur > vmax:
                                payload["sumInsured"] = vact if vact is not None else vmax

        # If we received Bayanaty IDs (large numeric) for make/model, map to tariff codes using Excel masterdata.
        # This is needed because tariff expects different codes than vehicleDetails IDs.
        try:
            if AnoudMakeModelMapping is not None:
                mk = _to_str(payload.get("makeCode"))
                md = _to_str(payload.get("modelCode"))
                if mk and md and mk.isdigit() and md.isdigit() and int(mk) >= 100000 and int(md) >= 100000:
                    m = (
                        AnoudMakeModelMapping.objects.filter(bayanaty_make_id=mk, bayanaty_model_id=md)
                        .only("tariff_make_code", "tariff_model_code")
                        .first()
                    )
                    if m:
                        payload["makeCode"] = m.tariff_make_code
                        payload["modelCode"] = m.tariff_model_code
        except Exception:
            api_logger.exception("Anoudapps mapping failed (AnoudMakeModelMapping lookup)")
        missing: List[str] = []
        required_keys = (
            "makeCode",
            "modelCode",
            "modelYear",
            "sumInsured",
            "vehicleType",
            "vehicleUsage",
            "noOfCylinder",
            "nationality",
            "seatingCapacity",
            "firstRegDate",
            "gccSpec",
            "previousInsuranceValid",
            "totalLoss",
            "driverDOB",
            "chassisNo",
            "admeId",
            "regnLocation",
        )
        # These are required but 0 is a valid value (0/1 flags).
        allow_zero = {"gccSpec", "previousInsuranceValid", "totalLoss"}
        for k in required_keys:
            v = payload.get(k)
            if v is None or v == "":
                missing.append(k)
                continue
            if k == "sumInsured":
                # Can be 0 for TP flows.
                continue
            if k in allow_zero:
                continue
            if v == 0 or v == "0":
                missing.append(k)
        if missing:
            api_logger.warning(
                "Anoudapps tariff missing required fields | company=%s missing=%s payload=%s",
                self.company,
                ",".join(missing),
                _trim_for_log(_sanitize_payload_for_logs(payload)),
            )
            return [
                {
                    "provider": "Anoudapps (QIC)",
                    "provider_id": "anoudapps_uae",
                    "success": False,
                    "failure_reason": f"missing_required_fields:{','.join(missing)}",
                    "raw_response": None,
                    "sent_payload": _sanitize_payload_for_logs(payload),
                    "response_time_ms": 0,
                    "error": "Missing required tariff fields",
                }
            ]
        api_logger.info(
            "Anoudapps tariff | company=%s payload=%s",
            self.company,
            _trim_for_log(_sanitize_payload_for_logs(payload)),
        )
        resp, rt = self._make_request(
            method="POST",
            endpoint="motor/tariff",
            headers=headers,
            params={"company": self.company},
            json=payload,
        )
        api_logger.info(
            "Anoudapps tariff response | company=%s response_time_ms=%s status=%s body=%s",
            self.company,
            rt,
            getattr(self, "_last_status_code", None),
            _trim_for_log(resp),
        )
        if resp is None:
            err = getattr(self, "_last_transport_error", None)
            return [
                {
                    "provider": "Anoudapps (QIC)",
                    "provider_id": "anoudapps_uae",
                    "success": False,
                    "failure_reason": f"transport_error:{err}" if err else "no_response",
                    "raw_response": None,
                    "response_time_ms": rt,
                    "error": err or "no_response",
                }
            ]

        # Doc response: respCode, errMessage, quoteNo, schemes[]
        resp_code = _to_str(resp.get("respCode"))
        # Observed success: respCode = "2000" (your sample). Allow other common success encodings.
        ok_codes = {"0", "2000", "success", "s", "ok"}
        if resp_code and resp_code.strip().lower() not in ok_codes:
            return [
                {
                    "provider": "Anoudapps (QIC)",
                    "provider_id": "anoudapps_uae",
                    "success": False,
                    "failure_reason": resp.get("errMessage") or f"tariff_failed:{resp_code}",
                    "raw_response": resp,
                    "sent_payload": _sanitize_payload_for_logs(payload),
                    "response_time_ms": rt,
                    "error": resp.get("errMessage") or "",
                }
            ]

        quote_no = _to_str(resp.get("quoteNo"))
        schemes = resp.get("schemes") or []
        if not isinstance(schemes, list) or not schemes:
            return [
                {
                    "provider": "Anoudapps (QIC)",
                    "provider_id": "anoudapps_uae",
                    "success": False,
                    "failure_reason": "empty_schemes",
                    "raw_response": resp,
                    "response_time_ms": rt,
                    "error": resp.get("errMessage") or "",
                }
            ]

        # Optional: netPremium endpoint provides taxAmount. Use it when possible.
        net_resp, net_rt = (None, 0)
        try:
            api_logger.info(
                "Anoudapps netPremium request | company=%s quoteNo=%s schemes=%s",
                self.company,
                quote_no,
                _trim_for_log([{"schemeCode": s.get("schemeCode"), "productCode": s.get("productCode")} for s in schemes if isinstance(s, dict)]),
            )
            net_resp, net_rt = self._fetch_net_premium(
                quote_no,
                [{"schemeCode": s.get("schemeCode"), "productCode": s.get("productCode")} for s in schemes if isinstance(s, dict)],
            )
        except Exception:
            net_resp, net_rt = (None, 0)
            api_logger.exception("Anoudapps netPremium failed | company=%s quoteNo=%s", self.company, quote_no)
        else:
            api_logger.info(
                "Anoudapps netPremium response | company=%s response_time_ms=%s status=%s body=%s",
                self.company,
                net_rt,
                getattr(self, "_last_status_code", None),
                _trim_for_log(net_resp),
            )

        out: List[Dict[str, Any]] = []
        for s in schemes:
            if not isinstance(s, dict):
                continue
            norm = self.normalize({"quoteNo": quote_no, "scheme": s, "netPremium": net_resp, "tariff": resp})
            norm.update(
                {
                    "provider": "Anoudapps (QIC)",
                    "provider_name": "Anoudapps (QIC)",
                    "provider_id": "anoudapps_uae",
                    "response_time_ms": int(rt + (net_rt or 0)),
                    "success": True,
                    "error": None,
                    "failure_reason": None,
                }
            )
            out.append(norm)
        return out

    def normalize(self, response_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Normalize (tariff scheme + optional netPremium summary) to app quote structure.
        """
        scheme = response_data.get("scheme") or {}
        quote_no = _to_str(response_data.get("quoteNo") or "")
        scheme_name = _to_str(scheme.get("schemeName") or scheme.get("schemeCode") or "")
        product_code = _to_str(scheme.get("productCode") or "")
        scheme_code = _to_str(scheme.get("schemeCode") or "")

        # Preferred pricing from netPremium response if present.
        net_resp = response_data.get("netPremium") or {}
        net_premium = _safe_float(
            net_resp.get("netPremium"),
            _safe_float(scheme.get("totalNetPremium"), _safe_float(scheme.get("netPremium"), 0.0)),
        )
        tax_amount = _safe_float(net_resp.get("taxAmount"), 0.0)
        base_price = max(net_premium - tax_amount, 0.0) if net_premium else _safe_float(scheme.get("netPremium"), 0.0)

        benefits: List[str] = []
        for k in ("basicCovers", "inclusiveCovers", "optionalCovers", "excessCovers", "discountCovers"):
            arr = scheme.get(k) or []
            if isinstance(arr, list):
                for c in arr:
                    if isinstance(c, dict):
                        nm = _to_str(c.get("name"))
                        if nm:
                            benefits.append(nm)

        return {
            "logo": "",
            "plan_name": scheme_name,
            "premium": float(net_premium),
            "base_price": float(base_price),
            "vat": float(tax_amount),
            "currency": "AED",
            "quote_id": quote_no,
            "reference_no": quote_no,
            "prod_code": f"{product_code}:{scheme_code}" if product_code or scheme_code else "",
            "buy_now_url": "",
            "vehicle_details": {
                "excess": "TBA",
                "ancillary_excess": "TBA",
                "vehicle_value": "N/A",
            },
            "benefits": benefits,
            "optional_covers": {},
            "raw_response": {
                "tariff": response_data.get("tariff"),
                "netPremium": net_resp,
                "scheme": scheme,
            },
        }

