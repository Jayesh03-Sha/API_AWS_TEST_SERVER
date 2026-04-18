from django.db import models
from django.contrib.auth.models import User
import json

class UserProfile(models.Model):
    """Extended user profile model for additional user information"""
    user = models.OneToOneField(User, on_delete=models.CASCADE, related_name='profile')
    phone_number = models.CharField(max_length=20, blank=True, null=True)
    organization = models.CharField(max_length=255, blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"{self.user.username} Profile"

    class Meta:
        ordering = ['-created_at']


class QuoteRequest(models.Model):
    """Model to store quote requests from users"""
    INSURANCE_TYPE_CHOICES = [
        ('health', 'Health Insurance'),
        ('travel', 'Travel Insurance'),
        ('motor', 'Motor Insurance'),
        ('home', 'Home Insurance'),
    ]
    
    user = models.ForeignKey(User, on_delete=models.CASCADE, related_name='quote_requests')
    insurance_type = models.CharField(max_length=50, choices=INSURANCE_TYPE_CHOICES)
    age = models.IntegerField()
    sum_insured = models.DecimalField(max_digits=12, decimal_places=2)
    city = models.CharField(max_length=100)
    members = models.IntegerField(default=1)
    additional_details = models.JSONField(default=dict, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"Quote Request - {self.user.username} - {self.insurance_type}"

    class Meta:
        ordering = ['-created_at']


class Quote(models.Model):
    """Model to store individual quotes from providers"""
    PROVIDER_CHOICES = [
        ('hdfc', 'HDFC Ergo'),
        ('icici', 'ICICI Lombard'),
        ('star', 'Star Health'),
    ]
    
    quote_request = models.ForeignKey(QuoteRequest, on_delete=models.CASCADE, related_name='quotes')
    provider = models.CharField(max_length=50, choices=PROVIDER_CHOICES)
    premium = models.DecimalField(max_digits=12, decimal_places=2)
    coverage = models.DecimalField(max_digits=12, decimal_places=2)
    benefits = models.JSONField(default=list, blank=True)
    comparison_score = models.FloatField(default=0)
    scoring_breakdown = models.JSONField(default=dict, blank=True)
    competitive_advantages = models.JSONField(default=list, blank=True)
    verdict = models.TextField(blank=True, null=True)
    is_best = models.BooleanField(default=False)
    response_time_ms = models.IntegerField(default=0)
    provider_metadata = models.JSONField(default=dict, blank=True, help_text="Stored provider-specific data (e.g. Reference No)")
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"{self.provider} - {self.premium}"

    class Meta:
        ordering = ['-comparison_score']
        # Multiple plans per provider on one request (e.g. DIC) — do not enforce unique (quote_request, provider).

class InsuranceProvider(models.Model):
    """Model to manage comparison companies (providers) dynamically from admin"""
    name = models.CharField(max_length=100)
    code = models.SlugField(max_length=50, unique=True, help_text="e.g. icici-uae")
    api_base_url = models.URLField(max_length=255, blank=True, null=True)
    api_key = models.CharField(max_length=255, blank=True, null=True)
    is_active = models.BooleanField(default=True)
    provider_class_path = models.CharField(
        max_length=255, 
        help_text="Full path to provider class, e.g. api_set1.services.providers.NID.NIAProvider"
    )
    icon_name = models.CharField(max_length=50, default="fas fa-building", help_text="FontAwesome icon name")
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return self.name

    class Meta:
        ordering = ['name']
        verbose_name = 'Comparison Company'
        verbose_name_plural = 'Comparison Companies'


class AnoudMakeModelMapping(models.Model):
    """
    Maps Bayanaty vehicleDetails IDs (make.id + model.id) to Anoudapps tariff makeCode/modelCode.

    Source: Make_Model_list_with_Bayanaty_MasterData.xlsx
    """

    bayanaty_make_id = models.CharField(max_length=32, db_index=True)
    bayanaty_model_id = models.CharField(max_length=32, db_index=True)
    tariff_make_code = models.CharField(max_length=32)
    tariff_model_code = models.CharField(max_length=32)

    make_name = models.CharField(max_length=120, blank=True, null=True)
    model_name = models.CharField(max_length=120, blank=True, null=True)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self) -> str:
        return f"{self.bayanaty_make_id}/{self.bayanaty_model_id} -> {self.tariff_make_code}/{self.tariff_model_code}"

    class Meta:
        unique_together = (("bayanaty_make_id", "bayanaty_model_id"),)
        indexes = [
            models.Index(fields=["bayanaty_make_id", "bayanaty_model_id"]),
        ]


# =============================================================================
# LEADS MODULE
# =============================================================================

class Lead(models.Model):
    """Lead management model for tracking prospective customers"""
    PRODUCT_TYPE_CHOICES = [
        ('health', 'Health Insurance'),
        ('travel', 'Travel Insurance'),
        ('motor', 'Motor Insurance'),
        ('home', 'Home Insurance'),
        ('life', 'Life Insurance'),
        ('marine', 'Marine Insurance'),
        ('property', 'Property Insurance'),
    ]
    STAGE_CHOICES = [
        ('new', 'New'),
        ('contacted', 'Contacted'),
        ('qualified', 'Qualified'),
        ('proposal', 'Proposal'),
        ('negotiation', 'Negotiation'),
        ('won', 'Won'),
        ('lost', 'Lost'),
    ]

    name = models.CharField(max_length=200)
    name_arabic = models.CharField(max_length=200, blank=True, null=True, verbose_name="Name (Arabic)")
    address = models.TextField(blank=True, null=True)
    occupation = models.CharField(max_length=200, blank=True, null=True)
    mobile_number = models.CharField(max_length=20)
    phone_number = models.CharField(max_length=20, blank=True, null=True)
    email = models.EmailField()
    product_type = models.CharField(max_length=50, choices=PRODUCT_TYPE_CHOICES)
    delivery_channel = models.CharField(max_length=100, blank=True, null=True, help_text="Lead Source")
    pep = models.BooleanField(default=False, verbose_name="PEP", help_text="Politically Exposed Person")
    responsible = models.ForeignKey(
        User, on_delete=models.SET_NULL, null=True, blank=True,
        related_name='assigned_leads', help_text="Responsible user"
    )
    stage = models.CharField(max_length=20, choices=STAGE_CHOICES, default='new')
    created_at = models.DateTimeField(auto_now_add=True)
    modified_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"{self.name} — {self.get_product_type_display()}"

    class Meta:
        ordering = ['-created_at']
        verbose_name = 'Lead'
        verbose_name_plural = 'Leads'


# =============================================================================
# DEALS MODULE
# =============================================================================

class Deal(models.Model):
    """Deal / vehicle information linked to a lead"""
    GENDER_CHOICES = [
        ('male', 'Male'),
        ('female', 'Female'),
    ]

    lead = models.ForeignKey(Lead, on_delete=models.CASCADE, related_name='deals')

    # Personal Details
    nationality = models.CharField(max_length=100, blank=True, null=True)
    emirates_id = models.CharField(max_length=20, blank=True, null=True, verbose_name="Emirates ID")
    id_expiry_date = models.DateField(blank=True, null=True, verbose_name="ID Expiry Date")
    date_of_birth = models.DateField(blank=True, null=True)
    gender = models.CharField(max_length=10, choices=GENDER_CHOICES, blank=True, null=True)
    emirate = models.CharField(max_length=100, blank=True, null=True)

    # License Details
    license_no = models.CharField(max_length=50, blank=True, null=True, verbose_name="License No")
    license_from_date = models.DateField(blank=True, null=True, verbose_name="License From Date")
    license_to_date = models.DateField(blank=True, null=True, verbose_name="License To Date")

    # Vehicle Details
    chassis_number = models.CharField(max_length=50, blank=True, null=True, verbose_name="Chassis Number / VIN No")
    reg_number = models.CharField(max_length=50, blank=True, null=True, verbose_name="Registration Number")
    reg_date = models.DateField(blank=True, null=True, verbose_name="Registration Date")
    plate_code = models.CharField(max_length=20, blank=True, null=True)
    plate_source = models.CharField(max_length=50, blank=True, null=True)
    tcf_number = models.CharField(max_length=50, blank=True, null=True, verbose_name="TCF Number")
    ncd_years = models.IntegerField(default=0, verbose_name="NCD Years", help_text="No-Claims Discount years")
    traffic_tran_type = models.CharField(max_length=50, blank=True, null=True, verbose_name="Traffic Transaction Type")
    is_vehicle_brand_new = models.BooleanField(default=False, verbose_name="Is Vehicle Brand New")
    agency_repair = models.BooleanField(default=False, verbose_name="Agency Repair")

    # Vehicle Specification (OR section)
    model_year = models.IntegerField(blank=True, null=True)
    make_id = models.CharField(max_length=50, blank=True, null=True, verbose_name="Make ID")
    model_id = models.CharField(max_length=50, blank=True, null=True, verbose_name="Model ID")
    trim_id = models.CharField(max_length=50, blank=True, null=True, verbose_name="Trim ID")
    body_type_id = models.CharField(max_length=50, blank=True, null=True, verbose_name="Body Type ID")
    engine_capacity_id = models.CharField(max_length=50, blank=True, null=True, verbose_name="Engine Capacity ID")
    transmission_id = models.CharField(max_length=50, blank=True, null=True, verbose_name="Transmission ID")
    is_gcc_spec = models.BooleanField(default=False, verbose_name="Is GCC Spec")
    colour = models.CharField(max_length=50, blank=True, null=True)
    vehicle_value = models.DecimalField(max_digits=12, decimal_places=2, blank=True, null=True)
    mileage = models.IntegerField(blank=True, null=True)
    valuation_date = models.DateField(blank=True, null=True)

    created_at = models.DateTimeField(auto_now_add=True)
    modified_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"Deal #{self.pk} — {self.lead.name}"

    class Meta:
        ordering = ['-created_at']
        verbose_name = 'Deal'
        verbose_name_plural = 'Deals'


class DealDocument(models.Model):
    """Documents attached to a deal"""
    deal = models.ForeignKey(Deal, on_delete=models.CASCADE, related_name='documents')
    document_type = models.CharField(max_length=100, help_text="e.g. EID, Passport, Visa, Registration Card")
    file = models.FileField(upload_to='deal_documents/')
    uploaded_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"{self.document_type} — Deal #{self.deal.pk}"

    class Meta:
        ordering = ['-uploaded_at']
        verbose_name = 'Deal Document'
        verbose_name_plural = 'Deal Documents'


# =============================================================================
# INVOICE MODULE — Individual Customer
# =============================================================================

class IndividualCustomer(models.Model):
    """Individual customer for invoicing"""
    TITLE_CHOICES = [
        ('mr', 'Mr'),
        ('mrs', 'Mrs'),
        ('ms', 'Ms'),
        ('dr', 'Dr'),
    ]
    GENDER_CHOICES = [
        ('male', 'Male'),
        ('female', 'Female'),
    ]

    title = models.CharField(max_length=20, choices=TITLE_CHOICES, blank=True, null=True)
    name = models.CharField(max_length=200)
    address = models.TextField(blank=True, null=True)
    occupation = models.CharField(max_length=200, blank=True, null=True)
    email = models.EmailField()
    mobile_number = models.CharField(max_length=20)
    phone_number = models.CharField(max_length=20, blank=True, null=True)
    gender = models.CharField(max_length=10, choices=GENDER_CHOICES, blank=True, null=True)
    date_of_birth = models.DateField(blank=True, null=True)
    emirates_id = models.CharField(max_length=20, blank=True, null=True, verbose_name="Emirates ID")
    id_expiry_date = models.DateField(blank=True, null=True, verbose_name="ID Expiry Date")
    nationality = models.CharField(max_length=100, blank=True, null=True)
    producer = models.CharField(max_length=200, blank=True, null=True)
    is_main_customer = models.BooleanField(default=False, verbose_name="Main Customer")
    is_resident = models.BooleanField(default=True, verbose_name="Resident")
    delivery_channel = models.CharField(max_length=100, blank=True, null=True, help_text="Lead Source")
    pep = models.BooleanField(default=False, verbose_name="PEP", help_text="Politically Exposed Person")
    aml_remarks = models.TextField(blank=True, null=True, verbose_name="AML Remarks", help_text="Remarks from AML Trace")
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"{self.get_title_display() or ''} {self.name}".strip()

    class Meta:
        ordering = ['-created_at']
        verbose_name = 'Individual Customer'
        verbose_name_plural = 'Individual Customers'


# =============================================================================
# INVOICE MODULE — Corporate Customer
# =============================================================================

class CorporateCustomer(models.Model):
    """Corporate customer for invoicing"""
    licence_type = models.CharField(max_length=100, blank=True, null=True, verbose_name="Licence Type")
    company_name = models.CharField(max_length=255)
    trade_licence_id = models.CharField(max_length=100, blank=True, null=True, verbose_name="Trade Licence ID")
    email = models.EmailField()
    phone_number = models.CharField(max_length=20)
    address = models.TextField(blank=True, null=True)
    po_box = models.CharField(max_length=50, blank=True, null=True, verbose_name="PO Box")
    contact_person = models.CharField(max_length=200, blank=True, null=True)
    producer = models.CharField(max_length=200, blank=True, null=True)
    is_main_customer = models.BooleanField(default=False, verbose_name="Main Customer")
    insured_name = models.CharField(max_length=200, blank=True, null=True)
    is_resident = models.BooleanField(default=True, verbose_name="Resident")
    id_expiry_date = models.DateField(blank=True, null=True, verbose_name="ID Expiry Date")
    nationality = models.CharField(max_length=100, blank=True, null=True)
    date_of_incorporation = models.DateField(blank=True, null=True)
    company_activity = models.CharField(max_length=200, blank=True, null=True)
    delivery_channel = models.CharField(max_length=100, blank=True, null=True, help_text="Lead Source")
    pep = models.BooleanField(default=False, verbose_name="PEP", help_text="Politically Exposed Person")
    remarks = models.TextField(blank=True, null=True)
    first_business_date = models.DateField(blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return self.company_name

    class Meta:
        ordering = ['-created_at']
        verbose_name = 'Corporate Customer'
        verbose_name_plural = 'Corporate Customers'


class UBODetail(models.Model):
    """Ultimate Beneficial Owner details for a corporate customer"""
    corporate_customer = models.ForeignKey(
        CorporateCustomer, on_delete=models.CASCADE, related_name='ubo_details'
    )
    name = models.CharField(max_length=200)
    eid_passport = models.CharField(max_length=50, verbose_name="EID / Passport")
    date_of_birth = models.DateField(blank=True, null=True)
    nationality = models.CharField(max_length=100, blank=True, null=True)
    contact_number = models.CharField(max_length=20, blank=True, null=True)
    designation = models.CharField(max_length=100, blank=True, null=True)

    def __str__(self):
        return f"{self.name} — {self.corporate_customer.company_name}"

    class Meta:
        ordering = ['name']
        verbose_name = 'UBO Detail'
        verbose_name_plural = 'UBO Details'


# =============================================================================
# TRANSACTION MODULE
# =============================================================================

class Transaction(models.Model):
    """Transaction record linking customer to policy and premium details"""
    CURRENCY_CHOICES = [
        ('AED', 'AED'),
        ('USD', 'USD'),
        ('EUR', 'EUR'),
        ('GBP', 'GBP'),
    ]

    # Customer link — one or the other
    individual_customer = models.ForeignKey(
        IndividualCustomer, on_delete=models.SET_NULL, null=True, blank=True,
        related_name='transactions', verbose_name="Individual Customer"
    )
    corporate_customer = models.ForeignKey(
        CorporateCustomer, on_delete=models.SET_NULL, null=True, blank=True,
        related_name='transactions', verbose_name="Corporate Customer"
    )

    # Transaction Details
    is_direct_payment = models.BooleanField(default=False, verbose_name="Direct Payment")
    insurer_name = models.CharField(max_length=200)
    invoice_date = models.DateField()
    policy_start_date = models.DateField()
    policy_end_date = models.DateField()
    premium_currency = models.CharField(max_length=10, choices=CURRENCY_CHOICES, default='AED')
    amount_currency = models.CharField(max_length=10, choices=CURRENCY_CHOICES, default='AED')
    branch = models.CharField(max_length=100, blank=True, null=True)
    center = models.CharField(max_length=100, blank=True, null=True)
    policy_type = models.CharField(max_length=100, blank=True, null=True)
    policy_cover = models.CharField(max_length=100, blank=True, null=True)
    reference_number = models.CharField(
        max_length=50, unique=True, blank=True, null=True,
        verbose_name="Reference Number", help_text="System generated"
    )

    # Customer Premium
    customer_net_premium = models.DecimalField(max_digits=14, decimal_places=2, default=0)
    customer_charges = models.DecimalField(max_digits=14, decimal_places=2, default=0)
    customer_total_premium = models.DecimalField(max_digits=14, decimal_places=2, default=0)
    customer_vat_amount = models.DecimalField(max_digits=14, decimal_places=2, default=0)
    customer_net_due = models.DecimalField(max_digits=14, decimal_places=2, default=0)

    # Insurance Company
    insurer_net_premium = models.DecimalField(max_digits=14, decimal_places=2, default=0)
    insurer_charges = models.DecimalField(max_digits=14, decimal_places=2, default=0)
    insurer_total_premium = models.DecimalField(max_digits=14, decimal_places=2, default=0)
    commission_percentage = models.DecimalField(
        max_digits=5, decimal_places=2, default=0,
        verbose_name="Commission %", help_text="System generated"
    )
    commission_amount = models.DecimalField(
        max_digits=14, decimal_places=2, default=0,
        help_text="System generated"
    )
    insurer_vat_amount = models.DecimalField(max_digits=14, decimal_places=2, default=0)
    insurer_net_due = models.DecimalField(max_digits=14, decimal_places=2, default=0)

    created_at = models.DateTimeField(auto_now_add=True)
    modified_at = models.DateTimeField(auto_now=True)

    def save(self, *args, **kwargs):
        # Auto-generate reference number
        if not self.reference_number:
            import uuid
            self.reference_number = f"TXN-{uuid.uuid4().hex[:8].upper()}"
        # Auto-calculate customer totals
        self.customer_total_premium = self.customer_net_premium + self.customer_charges
        self.customer_net_due = self.customer_total_premium + self.customer_vat_amount
        # Auto-calculate insurer totals
        self.insurer_total_premium = self.insurer_net_premium + self.insurer_charges
        if self.insurer_net_premium and self.commission_percentage:
            self.commission_amount = (self.insurer_net_premium * self.commission_percentage) / 100
        self.insurer_net_due = self.insurer_total_premium + self.insurer_vat_amount - self.commission_amount
        super().save(*args, **kwargs)

    def get_customer_name(self):
        if self.individual_customer:
            return str(self.individual_customer)
        elif self.corporate_customer:
            return str(self.corporate_customer)
        return "—"

    def __str__(self):
        return f"TXN {self.reference_number or self.pk} — {self.get_customer_name()}"

    class Meta:
        ordering = ['-created_at']
        verbose_name = 'Transaction'
        verbose_name_plural = 'Transactions'


# =============================================================================
# INSURER REFERENCE
# =============================================================================

class InsurerReference(models.Model):
    """Insurer reference numbers linked to a transaction"""
    transaction = models.OneToOneField(
        Transaction, on_delete=models.CASCADE, related_name='insurer_reference'
    )
    policy_number = models.CharField(max_length=100, blank=True, null=True)
    tax_invoice_number = models.CharField(max_length=100, blank=True, null=True)
    commission_invoice_number = models.CharField(max_length=100, blank=True, null=True)
    due_date = models.DateField(blank=True, null=True)

    def __str__(self):
        return f"Ref: {self.policy_number or '—'} — TXN {self.transaction.reference_number}"

    class Meta:
        verbose_name = 'Insurer Reference'
        verbose_name_plural = 'Insurer References'


# =============================================================================
# ATTACHMENTS
# =============================================================================

class Attachment(models.Model):
    """Document attachments linked to a transaction"""
    ATTACHMENT_TYPE_CHOICES = [
        ('eid', 'EID'),
        ('passport', 'Passport'),
        ('visa', 'Visa'),
        ('policy_documents', 'Policy Documents'),
        ('policy_schedule', 'Policy Schedule'),
        ('credit_note', 'Credit Note'),
        ('debit_note_invoice', 'Debit Note / Invoice'),
        ('other', 'Other Documents'),
    ]

    transaction = models.ForeignKey(
        Transaction, on_delete=models.CASCADE, related_name='attachments'
    )
    attachment_type = models.CharField(max_length=30, choices=ATTACHMENT_TYPE_CHOICES)
    file = models.FileField(upload_to='transaction_attachments/')
    description = models.TextField(blank=True, null=True)
    uploaded_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"{self.get_attachment_type_display()} — TXN {self.transaction.reference_number}"

    class Meta:
        ordering = ['-uploaded_at']
        verbose_name = 'Attachment'
        verbose_name_plural = 'Attachments'


# =============================================================================
# STATUS OVERVIEW
# =============================================================================

class StatusOverview(models.Model):
    """Workflow status tracking for transactions"""
    transaction = models.ForeignKey(
        Transaction, on_delete=models.CASCADE, related_name='status_history'
    )
    user = models.ForeignKey(
        User, on_delete=models.SET_NULL, null=True, blank=True,
        related_name='status_actions'
    )
    date = models.DateTimeField(auto_now_add=True)
    pending_at = models.CharField(max_length=200, blank=True, null=True, help_text="Stage/Department where it is pending")
    assigned_user = models.ForeignKey(
        User, on_delete=models.SET_NULL, null=True, blank=True,
        related_name='status_assignments'
    )

    def __str__(self):
        return f"Status @ {self.date:%Y-%m-%d} — TXN {self.transaction.reference_number}"

    class Meta:
        ordering = ['-date']
        verbose_name = 'Status Overview'
# =============================================================================
# ADDITIONAL MODELS FOR QUOTATION COMPARISON FLOW
# =============================================================================

class InsuranceCover(models.Model):
    """Specific covers (mandatory/optional) for a product"""
    COVER_TYPE_CHOICES = [
        ('mandatory', 'Mandatory'),
        ('optional', 'Optional'),
    ]
    
    quote = models.ForeignKey(Quote, on_delete=models.CASCADE, related_name='covers')
    cover_code = models.CharField(max_length=50)
    cover_name = models.CharField(max_length=255)
    cover_type = models.CharField(max_length=20, choices=COVER_TYPE_CHOICES)
    premium = models.DecimalField(max_digits=12, decimal_places=2, default=0)

    def __str__(self):
        return f"{self.cover_name} ({self.get_cover_type_display()})"


class SelectedScheme(models.Model):
    """The scheme selected by the user for payment and policy generation"""
    quote = models.OneToOneField(Quote, on_delete=models.CASCADE, related_name='selected_scheme')
    quotation_number = models.CharField(max_length=100, blank=True, null=True)
    net_premium = models.DecimalField(max_digits=12, decimal_places=2)
    vat_amount = models.DecimalField(max_digits=12, decimal_places=2)
    total_amount = models.DecimalField(max_digits=12, decimal_places=2)
    payment_url = models.URLField(max_length=500, blank=True, null=True)
    status = models.CharField(max_length=50, default='PENDING') # PENDING, PAID, FAILED
    
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"Selected: {self.quote.prod_code if hasattr(self.quote, 'prod_code') else self.quote.provider} - {self.status}"

