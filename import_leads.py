import os
import django
import datetime

# Setup Django environment
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'api_test_server.settings')
django.setup()

from api_set1.models import Lead, Deal
from django.contrib.auth.models import User

def parse_date(date_str):
    if not date_str:
        return None
    try:
        # Expected formats like M/D/YYYY or MM/DD/YYYY
        return datetime.datetime.strptime(date_str, '%m/%d/%Y').date()
    except ValueError:
        try:
            return datetime.datetime.strptime(date_str, '%d/%m/%Y').date()
        except ValueError:
            return None

def seed_leads():
    # Fetch responsible user
    admin_user = User.objects.first()
    if not admin_user:
        print("No user found in database. Create a superuser first.")
        return

    # Clear existing leads to avoid duplicates during testing
    Lead.objects.all().delete()
    print("Cleared existing leads.")

    leads_data = [
        {
            "name": "JAISON SAM JOHN JOHN KUNJUMMEN",
            "name_arabic": "جايسون سام جوهن جوهن كونجومين سام",
            "eid": "784-1985-1579056-0",
            "eid_expiry": "7/17/2026",
            "email": "motor@promiseinsure.com",
            "phone": "501788753",
            "nationality": "INDIAN",
            "dob": "1/31/1985",
            "license_no": "4830078",
            "license_from": "11/23/2024",
            "license_to": "11/23/2026",
            "tcf_number": "16553853",
            "emirate": "DUBAI",
            "reg_number": "63211",
            "plate_code": "DD",
            "reg_date": "4/16/2025",
            "insurance_expiry": "5/15/2026",
            "model_year": "2024",
            "chassis": "5N1DR3MR0RC317219",
            "engine": "VQ35 921964W",
            "make": "NISSAN",
            "model": "PATHFINDER",
            "colour": "RED BLACK",
            "value": "68000"
        },
        {
            "name": "KUNHI ABDULLA KUNHI PARAMBATH MOOSA HAJEE",
            "name_arabic": "كونهى عبدالله كونهى بار امبات موسى حاجى",
            "eid": "784-1958-9498547-2",
            "eid_expiry": "10/12/2034",
            "email": "anwarwarnsmart@gmail.com",
            "phone": "501788753",
            "nationality": "INDIAN",
            "dob": "4/5/1958",
            "license_no": "4590759",
            "license_from": "10/11/2023",
            "license_to": "10/11/2030",
            "tcf_number": "16063012",
            "emirate": "DUBAI",
            "reg_number": "3639",
            "plate_code": "K",
            "reg_date": "4/26/2023",
            "insurance_expiry": "5/21/2026",
            "model_year": "2023",
            "chassis": "JTMABBBJ7P4064250",
            "engine": "V35A0119250",
            "make": "TOYOTA",
            "model": "LAND CRUISER",
            "colour": "WHITE PEARL",
            "value": "7500"
        },
        {
            "name": "FADI SAFWAN AL MAWAS",
            "name_arabic": "فادي صفوان المواس",
            "eid": "784-1985-4372640-4",
            "eid_expiry": "9/10/2026",
            "email": "ameer.edrees@promiseinsure.com",
            "phone": "585561978",
            "nationality": "SYRIAN",
            "dob": "3/3/1985",
            "license_no": "858356",
            "license_from": "8/4/2009",
            "license_to": "5/3/2029",
            "tcf_number": "1080115236",
            "emirate": "SHARJAH",
            "reg_number": "51882",
            "plate_code": "1",
            "reg_date": "9/3/2023",
            "insurance_expiry": "3/5/2026",
            "model_year": "2023",
            "chassis": "MA3NC2B13PA505837",
            "engine": "K15BN1306559",
            "make": "SUZUKI",
            "model": "ERTIGA",
            "colour": "BLUE",
            "value": "68700"
        },
        {
            "name": "VIJAY RAVI VARMA KRISHNA KUMAR",
            "name_arabic": "فجای رافی فارما كريشنا كومار",
            "eid": "784-1982-2696406-6",
            "eid_expiry": "5/19/2032",
            "email": "motor@promiseinsure.com",
            "phone": "501788749",
            "nationality": "INDIAN",
            "dob": "12/23/1982",
            "license_no": "3748347",
            "license_from": "11/10/2017",
            "license_to": "11/10/2029",
            "tcf_number": "13327229",
            "emirate": "DUBAI",
            "reg_number": "23855",
            "plate_code": "V",
            "reg_date": "4/30/2018",
            "insurance_expiry": "4/20/2026",
            "model_year": "2018",
            "chassis": "5J6RW5817JL700303",
            "engine": "K24W94008772",
            "make": "HONDA",
            "model": "CRV",
            "colour": "RED",
            "value": "55000"
        },
        {
            "name": "SHIJU JOSEPH JOSEPH",
            "name_arabic": "SHIJU JOSEPH JOSEPH",
            "eid": "784-1983-5060409-0",
            "eid_expiry": "7/17/2026",
            "email": "shiju.joseph@example.com", # Generated placeholder
            "phone": "54152226",
            "nationality": "INDIAN",
            "dob": "9/16/1983",
            "license_no": "548772",
            "license_from": None,
            "license_to": None,
            "tcf_number": "54152226",
            "emirate": "ABU DHABI",
            "reg_number": "18691",
            "plate_code": "8",
            "reg_date": "6/17/2023",
            "insurance_expiry": "4/5/2026",
            "model_year": "2018",
            "chassis": "1C4HJXEG8JW156414",
            "engine": "NIL",
            "make": "JEEP",
            "model": "WRANGLER",
            "colour": "RED",
            "value": "37441"
        }
    ]

    for data in leads_data:
        # Create Lead
        lead = Lead.objects.create(
            name=data["name"],
            name_arabic=data["name_arabic"],
            email=data["email"],
            mobile_number=data["phone"],
            product_type="motor",
            responsible=admin_user,
            stage="qualified"
        )
        
        # Create Deal
        Deal.objects.create(
            lead=lead,
            nationality=data["nationality"],
            emirates_id=data["eid"],
            id_expiry_date=parse_date(data["eid_expiry"]),
            date_of_birth=parse_date(data["dob"]),
            license_no=data["license_no"],
            license_from_date=parse_date(data["license_from"]),
            license_to_date=parse_date(data["license_to"]),
            chassis_number=data["chassis"],
            reg_number=data["reg_number"],
            reg_date=parse_date(data["reg_date"]),
            plate_code=data["plate_code"],
            emirate=data["emirate"],
            tcf_number=data["tcf_number"],
            model_year=int(data["model_year"]) if data["model_year"] else None,
            make_id=data["make"],
            model_id=data["model"],
            colour=data["colour"],
            vehicle_value=float(data["value"]) if data["value"] else 0
        )
        print(f"Created lead and deal for {data['name']}")

if __name__ == "__main__":
    seed_leads()
