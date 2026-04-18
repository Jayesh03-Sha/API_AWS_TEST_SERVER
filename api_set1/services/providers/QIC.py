from typing import Optional, Dict
from .base import BaseProvider


class QICProvider(BaseProvider):
    """QIC Insurance Broker UAE Provider"""

    def __init__(self, api_key: str = None, base_url: str = None):
        super().__init__(
            api_key=api_key or 'qic_uae_test_key_002',
            base_url=base_url or 'http://localhost:8000/mock-api/qic-uae/'
        )
        self.provider_name = 'QIC Insurance UAE'

    def get_quote(self, data: Dict) -> Optional[Dict]:
        """
        Fetch quote using Emirates ID (NID)
        """
        try:
            emirates_id = data.get('nid') or data.get('emirates_id')

            if not emirates_id:
                # Default for testing
                emirates_id = "784-1990-1234567-1"

            payload = {
                "emirates_id": emirates_id,
                "age": data.get("age"),
                "sum_insured": data.get("sum_insured"),
                "gender": data.get("gender"),
                "plan_type": data.get("plan_type", "basic"),
                "insurance_type": data.get("insurance_type", "health"),
                "additional_details": data.get("additional_details", {})
            }

            response, response_time = self._make_request(
                method="POST",
                endpoint="quotes/",
                json=payload
            )

            if response:
                normalized_quote = self.normalize(response)
                normalized_quote['provider_id'] = 'qic_uae'
                normalized_quote['response_time_ms'] = response_time
                return normalized_quote

            return None

        except Exception as e:
            raise Exception(f"QIC UAE Provider Error: {str(e)}")

    def normalize(self, response_data: Dict) -> Dict:
        """
        Normalize QIC API response into standard format
        """
        return {
            'provider': response_data.get('provider', self.provider_name),
            'premium': float(response_data.get('premium', 0)),
            'coverage': float(response_data.get('coverage', 0)),
            'benefits': response_data.get('benefits', []),
            'policy_term': response_data.get('policy_term', 1),
            'waiting_period': response_data.get('waiting_period', 0),
        }