from django.contrib import admin
from django.contrib.auth.models import User
from django.contrib.auth.admin import UserAdmin as DefaultUserAdmin
from .models import (
    UserProfile, QuoteRequest, Quote, InsuranceProvider,
    Lead, Deal, DealDocument,
    IndividualCustomer, CorporateCustomer, UBODetail,
    Transaction, InsurerReference, Attachment, StatusOverview,
)


# =============================================================================
# AUTH CUSTOMIZATION FOR AUTOCOMPLETE
# =============================================================================
admin.site.unregister(User)

@admin.register(User)
class UserAdmin(DefaultUserAdmin):
    search_fields = ('username', 'email', 'first_name', 'last_name')


# =============================================================================
# EXISTING MODELS
# =============================================================================

@admin.register(InsuranceProvider)
class InsuranceProviderAdmin(admin.ModelAdmin):
    list_display = ('name', 'code', 'is_active', 'api_base_url', 'updated_at')
    list_editable = ('is_active',)
    search_fields = ('name', 'code')
    list_filter = ('is_active',)
    fieldsets = (
        ('General Information', {
            'fields': ('name', 'code', 'is_active', 'icon_name')
        }),
        ('API Configuration', {
            'fields': ('api_base_url', 'api_key', 'provider_class_path')
        }),
    )

@admin.register(UserProfile)
class UserProfileAdmin(admin.ModelAdmin):
    list_display = ('user', 'phone_number', 'organization', 'created_at')
    search_fields = ('user__username', 'phone_number', 'organization')
    list_filter = ('created_at',)

@admin.register(QuoteRequest)
class QuoteRequestAdmin(admin.ModelAdmin):
    list_display = ('id', 'user', 'insurance_type', 'sum_insured', 'city', 'created_at')
    list_filter = ('insurance_type', 'city', 'created_at')
    search_fields = ('user__username', 'city')
    date_hierarchy = 'created_at'

@admin.register(Quote)
class QuoteAdmin(admin.ModelAdmin):
    list_display = ('provider', 'premium', 'comparison_score', 'is_best', 'quote_request')
    list_filter = ('provider', 'is_best', 'created_at')
    search_fields = ('provider', 'quote_request__user__username')
    readonly_fields = ('created_at',)


# =============================================================================
# LEADS MODULE
# =============================================================================

@admin.register(Lead)
class LeadAdmin(admin.ModelAdmin):
    list_display = (
        'id', 'name', 'mobile_number', 'email',
        'product_type', 'stage', 'responsible', 'pep', 'created_at',
    )
    list_filter = ('product_type', 'stage', 'pep', 'delivery_channel', 'created_at')
    search_fields = ('name', 'email', 'mobile_number', 'phone_number')
    list_editable = ('stage',)
    date_hierarchy = 'created_at'
    readonly_fields = ('created_at', 'modified_at')
    autocomplete_fields = ('responsible',)

    fieldsets = (
        ('Contact Information', {
            'fields': ('name', 'email', 'mobile_number', 'phone_number', 'address')
        }),
        ('Professional', {
            'fields': ('occupation',)
        }),
        ('Lead Details', {
            'fields': ('product_type', 'delivery_channel', 'stage', 'responsible')
        }),
        ('Compliance', {
            'fields': ('pep',),
            'classes': ('collapse',),
        }),
        ('Timestamps', {
            'fields': ('created_at', 'modified_at'),
            'classes': ('collapse',),
        }),
    )


# =============================================================================
# DEALS MODULE
# =============================================================================

class DealDocumentInline(admin.TabularInline):
    model = DealDocument
    extra = 1
    readonly_fields = ('uploaded_at',)


@admin.register(Deal)
class DealAdmin(admin.ModelAdmin):
    list_display = (
        'id', 'lead', 'nationality', 'emirates_id', 'gender',
        'is_vehicle_brand_new', 'agency_repair', 'created_at',
    )
    list_filter = (
        'gender', 'emirate', 'is_vehicle_brand_new', 'agency_repair',
        'is_gcc_spec', 'created_at',
    )
    search_fields = ('lead__name', 'emirates_id', 'chassis_number', 'reg_number', 'license_no')
    date_hierarchy = 'created_at'
    readonly_fields = ('created_at', 'modified_at')
    autocomplete_fields = ('lead',)
    list_per_page = 20
    inlines = [DealDocumentInline]

    fieldsets = (
        ('Lead', {
            'fields': ('lead',)
        }),
        ('Personal Details', {
            'fields': (
                'nationality', 'emirates_id', 'id_expiry_date',
                'date_of_birth', 'gender', 'emirate',
            )
        }),
        ('License Details', {
            'fields': ('license_no', 'license_from_date', 'license_to_date'),
        }),
        ('Vehicle Details', {
            'fields': (
                'chassis_number', 'reg_number', 'reg_date',
                'plate_code', 'plate_source', 'tcf_number',
                'ncd_years', 'traffic_tran_type',
                'is_vehicle_brand_new', 'agency_repair',
            )
        }),
        ('Vehicle Specification', {
            'fields': (
                'model_year', 'make_id', 'model_id', 'trim_id',
                'body_type_id', 'engine_capacity_id', 'transmission_id',
                'is_gcc_spec', 'mileage', 'valuation_date',
            ),
            'classes': ('collapse',),
        }),
        ('Timestamps', {
            'fields': ('created_at', 'modified_at'),
            'classes': ('collapse',),
        }),
    )


# =============================================================================
# INVOICE — Individual Customer
# =============================================================================

@admin.register(IndividualCustomer)
class IndividualCustomerAdmin(admin.ModelAdmin):
    list_display = (
        'id', 'title', 'name', 'email', 'mobile_number',
        'nationality', 'is_main_customer', 'pep', 'created_at',
    )
    list_filter = ('gender', 'nationality', 'is_main_customer', 'is_resident', 'pep', 'created_at')
    search_fields = ('name', 'email', 'mobile_number', 'emirates_id')
    date_hierarchy = 'created_at'
    readonly_fields = ('created_at',)

    fieldsets = (
        ('Personal Information', {
            'fields': (
                'title', 'name', 'gender', 'date_of_birth',
                'nationality', 'occupation',
            )
        }),
        ('Contact', {
            'fields': ('email', 'mobile_number', 'phone_number', 'address')
        }),
        ('Identification', {
            'fields': ('emirates_id', 'id_expiry_date')
        }),
        ('Business Details', {
            'fields': (
                'producer', 'is_main_customer', 'is_resident',
                'delivery_channel',
            )
        }),
        ('Compliance', {
            'fields': ('pep', 'aml_remarks'),
            'classes': ('collapse',),
        }),
        ('Timestamps', {
            'fields': ('created_at',),
            'classes': ('collapse',),
        }),
    )


# =============================================================================
# INVOICE — Corporate Customer + UBO
# =============================================================================

class UBODetailInline(admin.TabularInline):
    model = UBODetail
    extra = 1


@admin.register(CorporateCustomer)
class CorporateCustomerAdmin(admin.ModelAdmin):
    list_display = (
        'id', 'company_name', 'trade_licence_id', 'email',
        'phone_number', 'contact_person', 'is_main_customer', 'pep', 'created_at',
    )
    list_filter = ('licence_type', 'nationality', 'is_main_customer', 'is_resident', 'pep', 'created_at')
    search_fields = ('company_name', 'trade_licence_id', 'email', 'contact_person', 'insured_name')
    date_hierarchy = 'created_at'
    readonly_fields = ('created_at',)
    inlines = [UBODetailInline]

    fieldsets = (
        ('Company Information', {
            'fields': (
                'licence_type', 'company_name', 'trade_licence_id',
                'company_activity', 'date_of_incorporation',
                'insured_name',
            )
        }),
        ('Contact', {
            'fields': ('email', 'phone_number', 'address', 'po_box', 'contact_person')
        }),
        ('Business Details', {
            'fields': (
                'producer', 'is_main_customer', 'is_resident',
                'nationality', 'id_expiry_date',
                'first_business_date', 'delivery_channel',
            )
        }),
        ('Compliance', {
            'fields': ('pep', 'remarks'),
            'classes': ('collapse',),
        }),
        ('Timestamps', {
            'fields': ('created_at',),
            'classes': ('collapse',),
        }),
    )


@admin.register(UBODetail)
class UBODetailAdmin(admin.ModelAdmin):
    list_display = ('name', 'corporate_customer', 'eid_passport', 'nationality', 'designation')
    search_fields = ('name', 'eid_passport', 'corporate_customer__company_name')
    list_filter = ('nationality',)


# =============================================================================
# TRANSACTION MODULE
# =============================================================================

class InsurerReferenceInline(admin.StackedInline):
    model = InsurerReference
    extra = 0
    max_num = 1


class AttachmentInline(admin.TabularInline):
    model = Attachment
    extra = 1
    readonly_fields = ('uploaded_at',)


class StatusOverviewInline(admin.TabularInline):
    model = StatusOverview
    extra = 1
    readonly_fields = ('date',)
    autocomplete_fields = ('user', 'assigned_user')


@admin.register(Transaction)
class TransactionAdmin(admin.ModelAdmin):
    list_display = (
        'reference_number', 'get_customer_name', 'insurer_name',
        'policy_type', 'invoice_date', 'policy_start_date', 'policy_end_date',
        'customer_net_due', 'insurer_net_due', 'created_at',
    )
    list_filter = (
        'is_direct_payment', 'premium_currency', 'policy_type',
        'branch', 'invoice_date', 'created_at',
    )
    search_fields = (
        'reference_number', 'insurer_name',
        'individual_customer__name', 'corporate_customer__company_name',
    )
    date_hierarchy = 'invoice_date'
    readonly_fields = (
        'reference_number', 'customer_total_premium', 'customer_net_due',
        'insurer_total_premium', 'commission_amount', 'insurer_net_due',
        'created_at', 'modified_at',
    )
    autocomplete_fields = ('individual_customer', 'corporate_customer')
    inlines = [InsurerReferenceInline, AttachmentInline, StatusOverviewInline]

    fieldsets = (
        ('Customer', {
            'fields': ('individual_customer', 'corporate_customer'),
            'description': 'Link this transaction to either an Individual or Corporate customer.',
        }),
        ('Transaction Details', {
            'fields': (
                'reference_number', 'is_direct_payment', 'insurer_name',
                'invoice_date', 'policy_start_date', 'policy_end_date',
                'premium_currency', 'amount_currency',
                'branch', 'center', 'policy_type', 'policy_cover',
            )
        }),
        ('Customer Premium', {
            'fields': (
                'customer_net_premium', 'customer_charges',
                'customer_total_premium', 'customer_vat_amount',
                'customer_net_due',
            ),
        }),
        ('Insurance Company', {
            'fields': (
                'insurer_net_premium', 'insurer_charges',
                'insurer_total_premium', 'commission_percentage',
                'commission_amount', 'insurer_vat_amount',
                'insurer_net_due',
            ),
        }),
        ('Timestamps', {
            'fields': ('created_at', 'modified_at'),
            'classes': ('collapse',),
        }),
    )

    @admin.display(description='Customer')
    def get_customer_name(self, obj):
        return obj.get_customer_name()


# =============================================================================
# INSURER REFERENCE (standalone access)
# =============================================================================

@admin.register(InsurerReference)
class InsurerReferenceAdmin(admin.ModelAdmin):
    list_display = ('transaction', 'policy_number', 'tax_invoice_number', 'commission_invoice_number', 'due_date')
    search_fields = ('policy_number', 'tax_invoice_number', 'commission_invoice_number', 'transaction__reference_number')
    list_filter = ('due_date',)


# =============================================================================
# ATTACHMENTS (standalone access)
# =============================================================================

@admin.register(Attachment)
class AttachmentAdmin(admin.ModelAdmin):
    list_display = ('id', 'transaction', 'attachment_type', 'file', 'uploaded_at')
    list_filter = ('attachment_type', 'uploaded_at')
    search_fields = ('transaction__reference_number', 'description')
    readonly_fields = ('uploaded_at',)


# =============================================================================
# STATUS OVERVIEW (standalone access)
# =============================================================================

@admin.register(StatusOverview)
class StatusOverviewAdmin(admin.ModelAdmin):
    list_display = ('transaction', 'user', 'date', 'pending_at', 'assigned_user')
    list_filter = ('date', 'pending_at')
    search_fields = ('transaction__reference_number', 'user__username', 'assigned_user__username', 'pending_at')
    readonly_fields = ('date',)
    autocomplete_fields = ('user', 'assigned_user', 'transaction')
