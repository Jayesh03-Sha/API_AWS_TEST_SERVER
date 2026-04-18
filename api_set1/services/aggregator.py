from typing import List, Dict, Tuple, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
import importlib
from ..models import InsuranceProvider
from django.db import close_old_connections

logger = logging.getLogger(__name__)
api_logger = logging.getLogger('api_providers')


class QuoteAggregator:
    """
    Aggregates quotes from multiple insurance providers dynamically loaded from the database.
    Handles parallel API calls and error management.
    """
    
    def __init__(self, providers: List = None):
        """
        Initialize aggregator. If no providers are passed, loads active ones from DB.
        """
        if providers is None:
            self.providers = self._load_active_providers()
        else:
            self.providers = providers
        
        self.max_workers = max(len(self.providers), 1)
    
    def _load_active_providers(self) -> List:
        """
        Loads active providers from the database and instantiates their classes.
        """
        active_providers = InsuranceProvider.objects.filter(is_active=True)
        instances = []
        
        for provider_data in active_providers:
            try:
                # Dynamic import using the class path stored in DB
                module_path, class_name = provider_data.provider_class_path.rsplit('.', 1)
                module = importlib.import_module(module_path)
                provider_class = getattr(module, class_name)
                
                # Instantiate with DB-configured URL and Key
                base_url = provider_data.api_base_url
                if provider_data.code == "dic-broker-uae" and base_url:
                    b = str(base_url).strip().lower()
                    if "localhost" in b or "127.0.0.1" in b:
                        api_logger.warning(
                            "Ignoring InsuranceProvider.api_base_url for DIC (localhost) — "
                            "using provider default https://uatbrokerportal.dubins.ae/"
                        )
                        base_url = None
                instance = provider_class(
                    api_key=provider_data.api_key,
                    base_url=base_url,
                )
                instance.provider_name = provider_data.name
                instances.append(instance)
                api_logger.info(
                    f"Loaded provider | name={provider_data.name} code={provider_data.code} "
                    f"class={provider_data.provider_class_path} base_url={provider_data.api_base_url}"
                )
                
            except Exception as e:
                api_logger.error(
                    f"Failed to load provider | name={provider_data.name} code={provider_data.code} "
                    f"class={provider_data.provider_class_path} | error={e}",
                    exc_info=True,
                )
                
        return instances

    def get_all_quotes(self, data: Dict, parallel: bool = True) -> List[Dict]:
        """
        Get quotes from all providers.
        """
        if not self.providers:
            api_logger.warning("No active providers loaded (0 providers).")
            return []

        if parallel:
            return self._get_quotes_parallel(data)
        else:
            return self._get_quotes_sequential(data)
    
    def _get_quotes_sequential(self, data: Dict) -> List[Dict]:
        quotes = []
        for provider in self.providers:
            try:
                quote = provider.get_quote(data)
                if quote:
                    # Providers may return a single quote dict or a list of quotes (multi-plan).
                    if isinstance(quote, list):
                        quotes.extend([q for q in quote if q])
                        api_logger.info(
                            f"Quote received (multi) | provider={provider.provider_name} count={len(quote)}"
                        )
                    else:
                        quotes.append(quote)
                        api_logger.info(
                            f"Quote received | provider={provider.provider_name} "
                            f"premium={quote.get('premium')} coverage={quote.get('coverage')} "
                            f"response_time_ms={quote.get('response_time_ms')}"
                        )
                else:
                    api_logger.warning(f"No quote | provider={provider.provider_name}")
            except Exception as e:
                api_logger.error(f"Provider quote failed | provider={provider.provider_name} | error={e}", exc_info=True)
        return quotes
    
    def _get_quotes_parallel(self, data: Dict) -> List[Dict]:
        quotes = []
        api_logger.info(f"Fanout start | providers={len(self.providers)} parallel=True max_workers={self.max_workers}")

        def _call_provider(provider, payload):
            # Ensure this thread doesn't reuse a DB connection from another thread/process.
            close_old_connections()
            return provider.get_quote(payload)

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_provider = {
                executor.submit(_call_provider, provider, data): provider
                for provider in self.providers
            }
            for future in as_completed(future_to_provider):
                provider = future_to_provider[future]
                try:
                    quote = future.result()
                    if quote:
                        # Providers may return a single quote dict or a list of quotes (multi-plan).
                        if isinstance(quote, list):
                            quotes.extend([q for q in quote if q])
                            api_logger.info(
                                f"Quote received (multi) | provider={provider.provider_name} count={len(quote)}"
                            )
                        else:
                            quotes.append(quote)
                            api_logger.info(
                                f"Quote received | provider={provider.provider_name} "
                                f"premium={quote.get('premium')} coverage={quote.get('coverage')} "
                                f"response_time_ms={quote.get('response_time_ms')}"
                            )
                    else:
                        api_logger.warning(f"No quote | provider={provider.provider_name}")
                except Exception as e:
                    api_logger.error(f"Provider quote failed | provider={provider.provider_name} | error={e}", exc_info=True)
        api_logger.info(f"Fanout complete | quotes_received={len(quotes)}")
        return quotes
