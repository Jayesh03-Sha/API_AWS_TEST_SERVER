from __future__ import annotations

from django.core.management.base import BaseCommand


class Command(BaseCommand):
    help = "Seed/update InsuranceProvider rows (idempotent)."

    def handle(self, *args, **options):
        from api_set1.models import InsuranceProvider

        providers = [
            {
                "name": "DIC Insurance Broker UAE",
                "code": "dic-broker-uae",
                "api_base_url": "https://uatbrokerportal.dubins.ae/",
                "api_key": "dic_uae_test_key_001",
                "is_active": True,
                "provider_class_path": "api_set1.services.providers.DIC.DICProvider",
                "icon_name": "fas fa-handshake",
            },
            {
                "name": "NIA ONLINE",
                "code": "nia-online",
                "api_base_url": "http://194.170.131.42:78/",
                "api_key": "",
                "is_active": True,
                "provider_class_path": "api_set1.services.providers.NIA.NIAProvider",
                "icon_name": "fas fa-building",
            },
            {
                "name": "QIC Insurance UAE",
                "code": "qic-uae",
                "api_base_url": "https://api.qic.com.qa/v1",
                "api_key": "qic_test_key_789",
                "is_active": False,
                "provider_class_path": "api_set1.services.providers.QIC.QICProvider",
                "icon_name": "fas fa-shield-alt",
            },
            {
                "name": "Anoudapps (QIC) UAE",
                "code": "anoudapps-uae",
                "api_base_url": "https://www.devapi.anoudapps.com/qicservices/aggregator/",
                # Store the Basic token base64 (without "Basic ") or the full "Basic ..." value.
                "api_key": "cHJvbWlzZV9hcGk6cHJvbWlzZV9hcGkjMjAyNiQ=",
                "is_active": True,
                "provider_class_path": "api_set1.services.providers.Anoudapps.AnoudappsProvider",
                "icon_name": "fas fa-car",
            },
        ]

        for p in providers:
            code = p["code"]
            defaults = {k: v for k, v in p.items() if k != "code"}
            obj, created = InsuranceProvider.objects.update_or_create(code=code, defaults=defaults)
            if created:
                self.stdout.write(self.style.SUCCESS(f"Created provider: {obj.code}"))
            else:
                self.stdout.write(self.style.SUCCESS(f"Updated provider: {obj.code}"))

