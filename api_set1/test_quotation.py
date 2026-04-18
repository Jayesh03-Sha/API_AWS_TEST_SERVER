"""
Comprehensive tests for the Insurance Quotation Comparison System

Tests cover:
- Provider API integrations
- Quote aggregation
- Quote comparison and scoring
- API endpoints
- Authentication and permissions
"""

import json
from decimal import Decimal
from django.test import TestCase, Client
from django.contrib.auth.models import User
from rest_framework.test import APITestCase, APIClient
from unittest.mock import patch, MagicMock
from rest_framework import status
from rest_framework_simplejwt.tokens import RefreshToken

from .models import QuoteRequest, Quote, UserProfile, InsuranceProvider
from .services.providers.DIC import DICProvider
from .services.providers.NIA import NIAProvider
from .services.providers.QIC import QICProvider
from .services.aggregator import QuoteAggregator
from .services.comparator import QuoteComparator


# ============================================================================
# Provider Tests
# ============================================================================

class DICProviderTestCase(TestCase):
    """Test DIC Provider Service"""
    
    def setUp(self):
        self.provider = DICProvider()
        self.test_data = {
            'age': 30,
            'sum_insured': 500000,
            'city': 'Dubai',
            'members': 2,
            'nid': '784-1990-1234567-1',
            # DIC motor API fields should be passed via additional_details
            'additional_details': {
                "insuredName_en": "Test User",
                "insuredName_ar": "Test User",
                "nationality": "101",
                "nationalId": "35363735322",
                "idExpiryDt": "21/10/2038",
                "dateOfBirth": "14/10/1997",
                "gender": "F",
                "emirate": "03",
                "emailAddress": "test@example.com",
                "mobileNumber": "971500000000",
                "licenseNo": "35372179989",
                "licenseFmDt": "21/05/2017",
                "licenseToDt": "28/11/2038",
                "chassisNumber": "JTJHY00W0J4282052",
                "regNumber": "6562",
                "regDt": "17/07/2020",
                "plateCode": "E",
                "PlateSource": "0001",
                "tcfNumber": "343",
                "ncdYears": "2",
                "trafficTranType": "101",
                "isVehBrandNew": "N",
                "agencyRepairYn": "N",
                "documentLists": [],
            },
        }
    
    def test_provider_initialization(self):
        """Test provider is initialized correctly"""
        self.assertEqual(self.provider.provider_name, 'DIC Insurance Broker UAE')
        self.assertIsNotNone(self.provider.api_key)
    
    # Note: get_quote calls the mock API which might not be running during tests
    # unless we mock the request or start the server. 
    # For unit tests, we at least test the normalization and initialization.

class NIAProviderTestCase(TestCase):
    """Test NIA Provider Service"""
    
    def setUp(self):
        self.provider = NIAProvider()
        self.test_data = {
            'age': 35,
            'sum_insured': 750000,
            'city': 'Abu Dhabi',
            'members': 3,
            'nid': '784-1990-1234567-1'
        }
    
    def test_provider_initialization(self):
        """Test provider is initialized correctly"""
        self.assertEqual(self.provider.provider_name, 'NIA Insurance Online')

class QICProviderTestCase(TestCase):
    """Test QIC Provider Service"""
    
    def setUp(self):
        self.provider = QICProvider()
        self.test_data = {
            'age': 28,
            'sum_insured': 600000,
            'city': 'Sharjah',
            'members': 2,
            'nid': '784-1990-1234567-1'
        }
    
    def test_provider_initialization(self):
        """Test provider is initialized correctly"""
        self.assertEqual(self.provider.provider_name, 'QIC Insurance UAE')


# ============================================================================
# Aggregator Tests
# ============================================================================

class QuoteAggregatorTestCase(TestCase):
    """Test Quote Aggregator Service"""
    
    def setUp(self):
        # Seed some providers for the aggregator to find
        InsuranceProvider.objects.create(
            name="DIC UAE",
            code="dic-broker-uae",
            is_active=True,
            provider_class_path="api_set1.services.providers.DIC.DICProvider"
        )
        InsuranceProvider.objects.create(
            name="NIA ONLINE",
            code="nia-online",
            is_active=True,
            provider_class_path="api_set1.services.providers.NIA.NIAProvider"
        )
        
        self.aggregator = QuoteAggregator()
        self.test_data = {
            'age': 30,
            'sum_insured': 500000,
            'city': 'Dubai',
            'members': 2,
            'insurance_type': 'health',
            'nid': '784-1990-1234567-1'
        }
    
    def test_aggregator_initialization(self):
        """Test aggregator has all providers"""
        self.assertEqual(len(self.aggregator.providers), 2)


# ============================================================================
# Comparator Tests
# ============================================================================

class QuoteComparatorTestCase(TestCase):
    """Test Quote Comparator Service"""
    
    def setUp(self):
        self.comparator = QuoteComparator()
        self.sample_quotes = [
            {
                'provider': 'DIC Insurance Broker UAE',
                'premium': 8500,
                'coverage': 500000,
                'benefits': ['Cashless Hospitals', 'No Claim Bonus'],
                'claim_settlement_ratio': 95
            },
            {
                'provider': 'ICICI UAE',
                'premium': 9100,
                'coverage': 500000,
                'benefits': ['Cashless Network', '24/7 Claim Support', 'Room Upgrade'],
                'claim_settlement_ratio': 92
            },
            {
                'provider': 'QIC Insurance UAE',
                'premium': 8700,
                'coverage': 500000,
                'benefits': ['Cashless', 'Ambulance', 'Health Checkup'],
                'network_hospitals': 11000
            }
        ]
    
    def test_score_calculation(self):
        """Test score calculation for a quote"""
        quote = self.sample_quotes[0]
        score = self.comparator._calculate_score(quote)
        
        self.assertGreaterEqual(score, 0)
        self.assertLessEqual(score, 100)
    
    def test_compare_quotes(self):
        """Test comparing multiple quotes — ranked by lowest premium, then score."""
        best_quote, sorted_quotes = self.comparator.compare_quotes(self.sample_quotes)
        
        self.assertIsNotNone(best_quote)
        self.assertEqual(len(sorted_quotes), 3)
        self.assertTrue(sorted_quotes[0]['is_best'])
        premiums = [q['premium'] for q in sorted_quotes]
        self.assertEqual(premiums, sorted(premiums))
        self.assertEqual(best_quote['premium'], min(q['premium'] for q in self.sample_quotes))


# ============================================================================
# API Endpoint Tests
# ============================================================================

class QuoteAPITestCase(APITestCase):
    """Test Quote API Endpoints"""
    
    def setUp(self):
        """Set up test user and client"""
        # Create user
        self.user = User.objects.create_user(
            username='testuser',
            email='test@example.com',
            password='TestPassword123!'
        )
        UserProfile.objects.create(user=self.user)
        
        # Get tokens
        refresh = RefreshToken.for_user(self.user)
        self.access_token = str(refresh.access_token)
        
        # Create client and set authentication
        self.client = APIClient()
        self.client.credentials(HTTP_AUTHORIZATION=f'Bearer {self.access_token}')
        
        # Seed providers
        InsuranceProvider.objects.create(
            name="DIC UAE",
            code="dic-broker-uae",
            is_active=True,
            provider_class_path="api_set1.services.providers.DIC.DICProvider"
        )
        
        self.base_url = '/api'
    
    def test_user_not_authenticated_cannot_get_quotes(self):
        """Test unauthenticated user cannot get quotes"""
        client = APIClient()
        data = {
            'insurance_type': 'health',
            'age': 30,
            'sum_insured': 500000,
            'city': 'Dubai',
            'members': 2,
            'nid': '784-1990-1234567-1'
        }
        response = client.post(f'{self.base_url}/quotes/get-quotes/', data, format='json')
        self.assertEqual(response.status_code, status.HTTP_401_UNAUTHORIZED)
    
    def test_get_quotes_invalid_age(self):
        """Test quote request with invalid age"""
        data = {
            'insurance_type': 'health',
            'age': 15,  # Too young
            'sum_insured': 500000,
            'city': 'Dubai',
            'members': 2
        }
        response = self.client.post(f'{self.base_url}/quotes/get-quotes/', data, format='json')
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn('age', response.data['errors'])

class DICMultiStepFlowTestCase(TestCase):
    """Test the complete 4-step flow for DIC Provider"""
    
    def setUp(self):
        self.provider = DICProvider(base_url='http://localhost:8000/mock-api/')
        self.test_data = {
            'request_id': 'TEST-REQ-001',
            'additional_details': {
                "insuredName_en": "shreyaa",
                "insuredName_ar": "shreyaa",
                "nationality": "101",
                "nationalId": "35363735322",
                "idExpiryDt": "21/10/2038",
                "dateOfBirth": "14/10/1997",
                "gender": "F",
                "emirate": "03",
                "emailAddress": "shreyash@gmail.com",
                "mobileNumber": "97135687632",
                "licenseNo": "35372179989",
                "licenseFmDt": "21/05/2017",
                "licenseToDt": "28/11/2038",
                "chassisNumber": "JTJHY00W0J4282052",
                "regNumber": "6562",
                "regDt": "17/07/2020",
                "plateCode": "E",
                "PlateSource": "0001",
                "tcfNumber": "343",
                "ncdYears": "2",
                "trafficTranType": "101",
                "isVehBrandNew": "N",
                "agencyRepairYn": "N",
                "documentLists": [],
            },
        }

    @patch('requests.post')
    @patch('requests.get')
    def test_complete_flow(self, mock_get, mock_post):
        """Test Auth -> Quote -> Choose -> Policy flow"""
        # Mock Auth Response
        mock_post.side_effect = [
            # Auth
            MagicMock(status_code=200, json=lambda: {"status": 1, "data": "MOCK_JWT_TOKEN_123456789"}),
            # Generate Quote
            MagicMock(status_code=200, json=lambda: {
                "status": 1,
                "statusId": "8006",
                "data": [{
                    "prodCode": "1001",
                    "prodName": {"en": "1001 - Comprehensive Gold", "ar": "1001 - Comprehensive Gold"},
                    "sumInsured": 244871,
                    "covers": {
                        "mandatory": [{"coverCode": "15001", "coverName": {"en": "Third Party Bodily Injury"}, "premium": 1100}],
                        "optional": [{"coverCode": "10002", "coverName": {"en": "PAB to Driver"}, "premium": 120}],
                    }
                }]
            }),
            # Choose Scheme
            MagicMock(status_code=200, json=lambda: {
                "status": 1,
                "statusId": "8106",
                "data": {
                    "quotationNo": "QUO-1001-999",
                    "paymentUrl": "http://mock-payment",
                    "grossPremium": 1220,
                    "netToCustomer": 1281,
                }
            })
        ]
        mock_get.return_value = MagicMock(status_code=200, json=lambda: {
            "status": 1,
            "statusId": "8201",
            "data": {
                "polNo": "P123",
                "polStatus": "APPROVED",
                "paymentStatus": "S"
            }
        })

        # 1. Auth
        token = self.provider.authenticate()
        self.assertIsNotNone(token)
        self.assertEqual(token, "MOCK_JWT_TOKEN_123456789")

        # 2. Generate Quote
        quotes = self.provider.get_quote(self.test_data)
        self.assertIsNotNone(quotes)
        self.assertIsInstance(quotes, list)
        self.assertGreaterEqual(len(quotes), 1)
        self.assertEqual(quotes[0]['prod_code'], '1001')
        self.assertIn('Comprehensive Gold', quotes[0]['plan_name'])
        self.assertGreater(len(quotes[0]['benefits']), 0)

        # 3. Choose Scheme
        scheme = self.provider.choose_scheme('1001')
        self.assertIsNotNone(scheme)
        self.assertEqual(scheme.get("status"), 1)
        self.assertIn('paymentUrl', scheme.get("data", {}))
        self.assertEqual(scheme["data"]['quotationNo'], 'QUO-1001-999')

        # 4. Get Policy
        policy = self.provider.get_policy('QUO-1001-999')
        self.assertIsNotNone(policy)
        self.assertEqual(policy.get("status"), 1)
class NIAMotorFlowTestCase(TestCase):
    """Test the complete 7-step flow for NIA Insurance (Assuretech)"""
    
    def setUp(self):
        self.provider = NIAProvider(base_url='http://localhost:8000/mock-api/')
        self.test_data = {
            'first_name': 'ADITHI',
            'last_name': 'B',
            'nid': '784199432474021',
            'email': 'adithi@mev1.ae',
            'mobile': '528649081',
            'chassis_number': '1N4SL3A92EC173668',
            'make_code': 'E1034',
            'model_code': 'E1034009',
            'year': 2019
        }
        InsuranceProvider.objects.create(
            name="NIA ONLINE",
            code="nia-online",
            is_active=True,
            provider_class_path="api_set1.services.providers.NIA.NIAProvider"
        )
        
        self.aggregator = QuoteAggregator()

    @patch('requests.post')
    def test_complete_nia_flow(self, mock_post):
        """Test Step 1 to 7 flow for NIA"""
        # Mock responses for all steps
        mock_post.side_effect = [
            # 1. ValidateLogin (NID sample shape)
            MagicMock(
                status_code=200,
                json=lambda: {
                    "Status": [{"Code": "1005", "Description": "Successfully Logged In"}],
                    "Data": [{"Token": "JWT_TOKEN_ABC"}],
                },
            ),
            # 2. CreateQuotation
            MagicMock(
                status_code=200,
                json=lambda: {
                    "Status": {"Code": "1001", "Description": "Quotation Created Successfully"},
                    "QuotationNo": "Q0/01/10/2015/02007",
                    "Data": {"ProdCode": "1005", "ProdName": "NIA Comfort"},
                    "Covers": [
                        {"Description": {"Eng": "Cover A"}, "Premium": "100", "Selected": "Y"},
                    ],
                },
            ),
            # 3. SaveQuoteWithPlan
            MagicMock(
                status_code=200,
                json=lambda: {
                    "Status": {"Code": "1003", "Description": "Quotation Information Saved Successfully"}
                },
            ),
            # 4. ApprovePolicy
            MagicMock(
                status_code=200,
                json=lambda: {
                    "Status": {
                        "Code": "2020",
                        "Description": "Policy Approved",
                        "PolicyNo": "P-NIA-2022-17082",
                    }
                },
            ),
        ]

        token = self.provider.authenticate()
        self.assertEqual(token, "JWT_TOKEN_ABC")

        quotes = self.provider.get_quote(self.test_data)
        self.assertIsInstance(quotes, list)
        self.assertEqual(quotes[0]["reference_no"], "Q0/01/10/2015/02007")
        self.assertTrue(quotes[0].get("success"))

        quot_no = self.provider.save_quote_with_plan(
            "Q0/01/10/2015/02007",
            "1005",
            [{"Code": "1001", "Premium": 100, "CvrType": "BC"}],
        )
        self.assertEqual(quot_no, "Q0/01/10/2015/02007")

        policy_no = self.provider.approve_policy("Q0/01/10/2015/02007")
        self.assertEqual(policy_no, "P-NIA-2022-17082")
