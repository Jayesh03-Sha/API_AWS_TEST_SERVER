import os
import django

# Setup Django environment
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'api_test_server.settings')
django.setup()

from api_set1.models import InsuranceProvider

def seed_providers():
    providers = [
        {
            "name": "DIC Insurance Broker UAE",
            "code": "dic-broker-uae",
            "api_base_url": "https://uatbrokerportal.dubins.ae/",
            "api_key": "dic_uae_test_key_001",
            "is_active": True,
            "provider_class_path": "api_set1.services.providers.DIC.DICProvider",
            "icon_name": "fas fa-handshake"
        },
        {
            "name": "NIA ONLINE",
            "code": "nia-online",
            # Set this to the real NIA/NID base URL in your environment.
            # Leave blank here if you prefer to fill it from Django admin.
            "api_base_url": "http://194.170.131.42:78/",
            "api_key": "",
            "is_active": True,
            "provider_class_path": "api_set1.services.providers.NIA.NIAProvider",
            "icon_name": "fas fa-building"
        },
        {
            "name": "QIC Insurance UAE",
            "code": "qic-uae",
            "api_base_url": "https://api.qic.com.qa/v1",
            "api_key": "qic_test_key_789",
            "is_active": False,
            "provider_class_path": "api_set1.services.providers.QIC.QICProvider",
            "icon_name": "fas fa-shield-alt"
        },
        {
            "name": "Anoudapps (QIC) UAE",
            "code": "anoudapps-uae",
            "api_base_url": "https://www.devapi.anoudapps.com/qicservices/aggregator/",
            # Store the Basic token base64 (without "Basic ") or the full "Basic ..." value.
            # Example (shared by user): cHJvbWlzZV9hcGk6cHJvbWlzZV9hcGkjMjAyNiQ=
            "api_key": "cHJvbWlzZV9hcGk6cHJvbWlzZV9hcGkjMjAyNiQ=",
            "is_active": True,
            "provider_class_path": "api_set1.services.providers.Anoudapps.AnoudappsProvider",
            "icon_name": "fas fa-car"
        }
    ]

    for p_data in providers:
        code = p_data["code"]
        defaults = {k: v for k, v in p_data.items() if k != "code"}
        obj, created = InsuranceProvider.objects.update_or_create(code=code, defaults=defaults)
        if created:
            print(f"Created provider: {p_data['name']}")
        else:
            print(f"Provider already exists: {p_data['name']}")

if __name__ == "__main__":
    seed_providers()
