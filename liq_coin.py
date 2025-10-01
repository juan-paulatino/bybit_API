import requests
import json
from typing import List, Dict, Any, Optional

# Base URL for the Coinalyze v1 API
BASE_URL = "https://api.coinalyze.net/v1"

def get_liquidation_history(
    api_key: str,
    symbol: Optional[str] = None,
    exchange: Optional[str] = None,
    start_time: Optional[int] = None,
    end_time: Optional[int] = None,
    auth_method: str = 'query'
) -> List[Dict[str, Any]]:
    """
    Fetches liquidation history data from the Coinalyze API.

    Authentication can be done via 'query' parameter (default) or 'header'.

    Args:
        api_key: Your Coinalyze API Key.
        symbol: Filter by a specific symbol (e.g., 'BTCUSDT').
        exchange: Filter by a specific exchange (e.g., 'binance').
        start_time: Start timestamp in seconds (integer).
        end_time: End timestamp in seconds (integer).
        auth_method: 'query' (API key in URL) or 'header' (API key in HTTP header).

    Returns:
        A list of dictionaries containing liquidation data, or an empty list on failure.
    """
    endpoint = f"{BASE_URL}/liquidation-history"
    
    # 1. Build the query parameters
    params = {
        'symbol': symbol,
        'exchange': exchange,
        'from': start_time,
        'to': end_time,
    }
    # Remove parameters that were not provided (None values)
    params = {k: v for k, v in params.items() if v is not None}
    
    headers = {}

    # 2. Handle Authentication
    if auth_method == 'query':
        # Option 1: API Key in URL query parameter (as requested)
        params['api_key'] = api_key
    elif auth_method == 'header':
        # Option 2: API Key in HTTP Header (as requested)
        # Note: If the API supports both, choose the one you prefer.
        headers['api_key'] = api_key
    else:
        print("Error: Invalid auth_method specified. Use 'query' or 'header'.")
        return []

    print(f"Making request to: {endpoint} with parameters: {params}...")

    try:
        # 3. Make the GET request
        response = requests.get(endpoint, headers=headers, params=params)

        # 4. Check for successful response
        response.raise_for_status() # Raises an HTTPError for bad responses (4xx or 5xx)

        # 5. Parse and return the JSON data
        data = response.json()
        print("Successfully retrieved data.")
        return data

    except requests.exceptions.HTTPError as e:
        print(f"HTTP Error occurred: {e}")
        # Attempt to print response content if available for more detail
        try:
            print(f"Server Response Content: {e.response.text}")
        except:
            pass
        return []
    except requests.exceptions.RequestException as e:
        print(f"An error occurred during the request: {e}")
        return []

# --- Example Usage ---
if __name__ == '__main__':
    # REPLACE THIS WITH YOUR ACTUAL COINALYZE API KEY
    # WARNING: Do not commit actual keys to public repositories.
    MY_API_KEY = "YOUR_SECRET_API_KEY_HERE"

    if MY_API_KEY == "YOUR_SECRET_API_KEY_HERE":
        print("Please replace 'YOUR_SECRET_API_KEY_HERE' with your actual API key to run this example.")
    else:
        # Example 1: Fetching recent history for BTCUSDT (using query parameter auth)
        print("\n--- Example 1: BTCUSDT History (Query Auth) ---")
        btc_liquidations = get_liquidation_history(
            api_key=MY_API_KEY,
            symbol='BTCUSDT',
            exchange='binance'
        )
        
        if btc_liquidations:
            print(f"Total symbols returned: {len(btc_liquidations)}")
            # Pretty print the first 5 entries of the first symbol's history
            first_symbol_data = btc_liquidations[0]
            print(f"Symbol: {first_symbol_data['symbol']}")
            print(f"History (First 5 records):\n{json.dumps(first_symbol_data['history'][:5], indent=2)}")
        else:
            print("Failed to retrieve BTCUSDT liquidation data.")

        # Example 2: Fetching all symbols (using header auth) - might be a large request!
        print("\n--- Example 2: All Symbols History (Header Auth) ---")
        all_liquidations = get_liquidation_history(
            api_key=MY_API_KEY,
            auth_method='header'
        )

        if all_liquidations:
            print(f"Successfully retrieved history for {len(all_liquidations)} symbols.")
        else:
            print("Failed to retrieve all liquidation data.")
