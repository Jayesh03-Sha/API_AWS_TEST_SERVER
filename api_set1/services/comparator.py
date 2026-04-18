from typing import List, Dict, Tuple, Optional
import logging

logger = logging.getLogger(__name__)
api_logger = logging.getLogger('api_providers')


class QuoteComparator:
    """
    Compares and scores insurance quotes using intelligent algorithms.
    Provides best quote selection, ranking, and detailed competitive analysis.
    """
    
    # Scoring weights (must sum to 1.0)
    WEIGHT_PREMIUM = 0.40      # 40% - Lower premium = better
    WEIGHT_BENEFITS = 0.30     # 30% - More benefits = better
    WEIGHT_COVERAGE = 0.15     # 15% - Higher coverage = better
    WEIGHT_CLAIM_SETTLEMENT = 0.10  # 10% - Higher ratio = better
    WEIGHT_NETWORK = 0.05      # 5% - Larger network = better
    
    def __init__(self):
        """Initialize quote comparator"""
        self._validate_weights()
    
    def _validate_weights(self):
        """Validate that weights sum to 1.0"""
        total = (
            self.WEIGHT_PREMIUM +
            self.WEIGHT_BENEFITS +
            self.WEIGHT_COVERAGE +
            self.WEIGHT_CLAIM_SETTLEMENT +
            self.WEIGHT_NETWORK
        )
        assert abs(total - 1.0) < 0.01, "Scoring weights must sum to 1.0"
    
    def _calculate_score(self, quote: Dict) -> float:
        """Compatibility wrapper for calculating score only"""
        score, _ = self._calculate_score_with_breakdown(quote)
        return score
    
    def compare_quotes(self, quotes: List[Dict]) -> Tuple[Optional[Dict], List[Dict]]:
        """
        Compare and score quotes, returning best quote and ranked list.
        """
        if not quotes:
            return None, []
        
        # Calculate scores and breakdowns for all quotes.
        # Score failures should not take down the whole request; they should be visible in logs.
        for quote in quotes:
            try:
                score, breakdown = self._calculate_score_with_breakdown(quote)
                quote['score'] = score
                quote['scoring_breakdown'] = breakdown
                quote['competitive_advantages'] = self._determine_advantages(quote, quotes)
            except Exception as e:
                api_logger.error(
                    f"Scoring failed | provider={quote.get('provider')} quote={quote} | error={e}",
                    exc_info=True,
                )
                quote['score'] = float('-inf')
                quote['scoring_breakdown'] = {'error': str(e)}
                quote['competitive_advantages'] = []

        def _prem(v) -> float:
            try:
                return float(v)
            except (TypeError, ValueError):
                return float("inf")

        def _sort_key(x: Dict) -> Tuple[float, float]:
            p = _prem(x.get("premium"))
            try:
                s = float(x.get("score", float("-inf")))
            except (TypeError, ValueError):
                s = float("-inf")
            return (p, -s)

        # Motor comparison: lowest premium first; tie-break by higher score.
        sorted_quotes = sorted(quotes, key=_sort_key)
        
        # Mark best quote and add verdict
        if sorted_quotes:
            best = sorted_quotes[0]
            best['is_best'] = True
            best['verdict'] = self._generate_verdict(best)
            
            # Label others
            for i, q in enumerate(sorted_quotes[1:], 1):
                q['is_best'] = False
                q['rank'] = i + 1
        
        best_quote = sorted_quotes[0] if sorted_quotes else None
        
        return best_quote, sorted_quotes
    
    def _calculate_score_with_breakdown(self, quote: Dict) -> Tuple[float, Dict]:
        """Calculates score and returns individual component points."""
        breakdown = {}
        total_score = 0.0

        def _to_float(value, default: float) -> float:
            if value is None:
                return default
            try:
                return float(value)
            except (TypeError, ValueError):
                return default
        
        # 1. Premium (40%)
        premium = _to_float(quote.get('premium', 10000), 10000)
        premium_points = max(0, min(100, (10000 - premium) / 100))
        breakdown['price_score'] = round(premium_points, 1)
        total_score += premium_points * self.WEIGHT_PREMIUM
        
        # 2. Benefits (30%)
        benefits = quote.get('benefits', [])
        benefits_points = min(100, len(benefits) * 20)
        breakdown['benefit_score'] = round(benefits_points, 1)
        total_score += benefits_points * self.WEIGHT_BENEFITS
        
        # 3. Coverage (15%)
        coverage = _to_float(quote.get('coverage', 500000), 500000)
        coverage_points = min(100, (coverage / 100000))
        breakdown['coverage_score'] = round(coverage_points, 1)
        total_score += coverage_points * self.WEIGHT_COVERAGE
        
        # 4. Claim Settlement (10%)
        claim_ratio = _to_float(quote.get('claim_settlement_ratio', 90), 90)
        breakdown['settlement_score'] = round(claim_ratio, 1)
        total_score += claim_ratio * self.WEIGHT_CLAIM_SETTLEMENT
        
        # 5. Network (5%)
        network = _to_float(quote.get('network_hospitals', 5000), 5000)
        network_points = min(100, (network / 1000) * 10)
        breakdown['network_score'] = round(network_points, 1)
        total_score += network_points * self.WEIGHT_NETWORK
        
        return round(total_score, 2), breakdown

    def _determine_advantages(self, quote: Dict, all_quotes: List[Dict]) -> List[str]:
        """Identify what makes this quote stand out."""
        advantages = []
        
        premiums = [float(q.get('premium', 999999)) for q in all_quotes]
        if float(quote.get('premium', 999999)) == min(premiums):
            advantages.append("Most Affordable Premium")
            
        coverages = [float(q.get('coverage', 0)) for q in all_quotes]
        if float(quote.get('coverage', 0)) == max(coverages):
            advantages.append("Highest Coverage Limit")
            
        benefits_count = [len(q.get('benefits', [])) for q in all_quotes]
        if len(quote.get('benefits', [])) == max(benefits_count):
            advantages.append("Most Comprehensive Benefits")
            
        return advantages

    def _generate_verdict(self, best_quote: Dict) -> str:
        """Generate a professional rationale for the top ranking."""
        reasons = []
        bd = best_quote['scoring_breakdown']
        
        if bd['price_score'] > 80:
            reasons.append("exceptional pricing")
        if bd['benefit_score'] > 80:
            reasons.append("extensive benefit package")
        if bd['coverage_score'] > 80:
            reasons.append("high coverage limits")
            
        reason_str = " and ".join(reasons) if reasons else "well-balanced features"
        return f"Ranked as the #1 choice due to its {reason_str}, offering the best overall value for your profile."

    def get_comparison_summary(self, quotes: List[Dict]) -> Dict:
        """Get summary statistics about the quotes."""
        if not quotes:
            return {}
        
        premiums = [float(q.get('premium', 0)) for q in quotes]
        scores = [q.get('score', 0) for q in quotes]
        
        return {
            'count': len(quotes),
            'avg_premium': round(sum(premiums) / len(premiums), 2),
            'min_premium': round(min(premiums), 2),
            'max_premium': round(max(premiums), 2),
            'market_avg_score': round(sum(scores) / len(scores), 2),
            'best_available_score': round(max(scores), 2),
        }
