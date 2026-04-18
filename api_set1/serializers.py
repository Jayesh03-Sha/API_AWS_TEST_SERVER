from rest_framework import serializers
from django.contrib.auth.models import User
from django.contrib.auth.password_validation import validate_password
from rest_framework_simplejwt.serializers import TokenObtainPairSerializer
from .models import UserProfile, QuoteRequest, Quote


class RegisterSerializer(serializers.ModelSerializer):
    """Serializer for user registration"""
    password = serializers.CharField(write_only=True, required=True, validators=[validate_password])
    password2 = serializers.CharField(write_only=True, required=True)

    class Meta:
        model = User
        fields = ('username', 'email', 'first_name', 'last_name', 'password', 'password2')
        extra_kwargs = {
            'email': {'required': True},
            'first_name': {'required': False},
            'last_name': {'required': False},
        }

    def validate(self, data):
        if data['password'] != data['password2']:
            raise serializers.ValidationError({"password": "Password fields didn't match."})
        
        if User.objects.filter(username=data['username']).exists():
            raise serializers.ValidationError({"username": "Username already exists."})
        
        if User.objects.filter(email=data['email']).exists():
            raise serializers.ValidationError({"email": "Email already exists."})
        
        return data

    def create(self, validated_data):
        validated_data.pop('password2')
        user = User.objects.create_user(**validated_data)
        UserProfile.objects.create(user=user)
        return user


class UserSerializer(serializers.ModelSerializer):
    """Serializer for user details"""
    profile = serializers.SerializerMethodField()

    class Meta:
        model = User
        fields = ('id', 'username', 'email', 'first_name', 'last_name', 'profile')
        read_only_fields = ('id',)

    def get_profile(self, obj):
        try:
            profile = obj.profile
            return {
                'phone_number': profile.phone_number,
                'organization': profile.organization,
                'created_at': profile.created_at,
            }
        except UserProfile.DoesNotExist:
            return None


class CustomTokenObtainPairSerializer(TokenObtainPairSerializer):
    """Custom serializer for JWT token with user details"""
    
    def validate(self, attrs):
        data = super().validate(attrs)
        user = self.user
        data['user'] = UserSerializer(user).data
        return data


class ChangePasswordSerializer(serializers.Serializer):
    """Serializer for changing password"""
    old_password = serializers.CharField(write_only=True, required=True)
    new_password = serializers.CharField(write_only=True, required=True, validators=[validate_password])
    new_password2 = serializers.CharField(write_only=True, required=True)

    def validate(self, data):
        if data['new_password'] != data['new_password2']:
            raise serializers.ValidationError({"new_password": "Password fields didn't match."})
        return data


class QuoteRequestSerializer(serializers.ModelSerializer):
    """Serializer for creating quote requests"""
    
    class Meta:
        model = QuoteRequest
        fields = ('id', 'insurance_type', 'age', 'sum_insured', 'city', 'members', 'additional_details', 'created_at')
        read_only_fields = ('id', 'created_at')
    
    def validate_age(self, value):
        if value < 18 or value > 100:
            raise serializers.ValidationError("Age must be between 18 and 100")
        return value
    
    def validate_sum_insured(self, value):
        if value <= 0:
            raise serializers.ValidationError("Sum insured must be greater than 0")
        if value > 10000000:
            raise serializers.ValidationError("Sum insured cannot exceed 10,000,000")
        return value
    
    def validate_members(self, value):
        if value < 1 or value > 10:
            raise serializers.ValidationError("Members count must be between 1 and 10")
        return value


class QuoteSerializer(serializers.ModelSerializer):
    """Serializer for individual quotes"""
    
    class Meta:
        model = Quote
        fields = (
            'id', 'provider', 'premium', 'coverage', 'benefits', 
            'comparison_score', 'scoring_breakdown', 'competitive_advantages', 
            'verdict', 'is_best', 'response_time_ms', 'provider_metadata', 'created_at'
        )
        read_only_fields = ('id', 'created_at', 'comparison_score', 'is_best', 'response_time_ms', 'provider_metadata')


class QuoteResponseSerializer(serializers.Serializer):
    """Serializer for quote response with best quote and all options"""
    
    best_quote = QuoteSerializer()
    quotes = QuoteSerializer(many=True)
    comparison_summary = serializers.SerializerMethodField()
    
    def get_comparison_summary(self, data):
        """Calculate and return comparison summary"""
        quotes = data.get('quotes', [])
        if not quotes:
            return None
        
        premiums = [float(q.premium) for q in quotes]
        scores = [float(q.comparison_score) for q in quotes]
        
        return {
            'count': len(quotes),
            'avg_premium': round(sum(premiums) / len(premiums), 2),
            'min_premium': round(min(premiums), 2),
            'max_premium': round(max(premiums), 2),
            'premium_range': round(max(premiums) - min(premiums), 2),
            'avg_score': round(sum(scores) / len(scores), 2) if scores else 0,
            'highest_score': round(max(scores), 2) if scores else 0,
            'savings_potential': round(max(premiums) - min(premiums), 2),
        }
        return data
