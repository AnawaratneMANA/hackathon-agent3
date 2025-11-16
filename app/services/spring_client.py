import os
import json
import requests
from typing import Dict, Any, Tuple

# --- 1. API Configuration ---
# Set these environment variables in your deployment environment:
# INVENTORY_API_URL: The base URL of your inventory service (e.g., "https://api.mycompany.com")
# INVENTORY_API_KEY: Optional key for authorization
API_BASE_URL = os.getenv("INVENTORY_API_URL", "http://localhost:8080")
API_ENDPOINT = "/api/inventory/status"
# API_KEY = os.getenv("INVENTORY_API_KEY")

def fetch_stock_level_from_api(item_id: str) -> Tuple[int, str]:
    """
    Fetches the current stock level for an item via the Inventory Data API.
    
    Args:
        item_id: The unique identifier for the inventory item.
            
    Returns:
        A tuple (stock_quantity, error_message). stock_quantity is 0 on failure.
    """
    url = f"{API_BASE_URL}{API_ENDPOINT}"
    headers = {}
    
    # Note: In a real environment, you must install requests: pip install requests
    try:
        print(f"Attempting to call API: {url} with item_id={item_id}")
        response = requests.get(url, headers=headers, params=params, timeout=5)
        
        # Raise an exception for bad status codes (4xx or 5xx)
        response.raise_for_status()
        
        data = response.json()
        
        # ASSUMPTION: The API response returns a JSON object with a 'quantity' field.
        stock_level = data.get('quantity')
        
        if stock_level is not None and isinstance(stock_level, (int, float)) and stock_level >= 0:
            return int(stock_level), ""
        else:
            return 0, f"API returned successfully, but 'quantity' field was missing, null, or invalid in response: {data}"

    except requests.exceptions.RequestException as e:
        error_msg = f"API Request Error (GET {url}): {e}. HTTP Status Code: {getattr(e.response, 'status_code', 'N/A')}"
        print(error_msg)
        return 0, error_msg
    except json.JSONDecodeError:
        error_msg = f"API Response Error: Could not parse JSON from API."
        print(error_msg)
        return 0, error_msg
    except Exception as e:
        error_msg = f"An unexpected error occurred during API fetch: {e}"
        print(error_msg)
        return 0, error_msg

# --- 3. Risk Assessment Logic ---

def analyze_inventory_risk(current_stock: int, thresholds: Dict[str, int]) -> Tuple[int, str]:
    """
    Calculates a risk score (1-5) and associated status based on stock level.
    """
    low_risk_min = thresholds.get('low_risk_min', 100)
    high_risk_max = thresholds.get('high_risk_max', 20)
    
    # 5 (Critical) - Below or at the highest risk threshold
    if current_stock <= high_risk_max:
        return 5, "Critical: Stock is severely low, immediate reorder required."
    
    # 4 (High) - Between critical and low-medium
    if current_stock <= high_risk_max * 1.5:
        return 4, "High Risk: Stock is low, needs urgent attention and review."
        
    # 3 (Medium) - Standard reorder zone
    if current_stock <= low_risk_min / 2:
        return 3, "Medium Risk: Stock is approaching reorder level, monitor closely."
        
    # 2 (Low-Medium) - Still healthy, but below the ideal safety stock
    if current_stock < low_risk_min:
        return 2, "Low Risk: Stock is healthy but below ideal safety buffer."
        
    # 1 (Safe) - At or above the desired safety stock level
    return 1, "Safe: Stock is at optimal level, no action required."


# --- 4. Main Agent Function (Exposed to Orchestrator) ---

def get_inventory_and_risk_score(item_id: str) -> Dict[str, Any]:
    """
    The main agent function to be called by the orchestrator.
    Fetches the stock level via API call and calculates the risk score.
    """
    print(f"\n[POSTGRE ANALYZER AGENT] Analyzing inventory for Item ID: {item_id}")

    # Define item-specific risk thresholds (can be customized per item or globally)
    RISK_THRESHOLDS = {
        'low_risk_min': 100,  # Optimal safety stock (Score 1)
        'high_risk_max': 20, # Critical threshold (Score 5)
    }

    stock_level = 0
    error_message = ""
    data_source = "API"

    # --- Fetch Stock Level via API ---
    try:
        stock_level, error_message = fetch_stock_level_from_api(item_id)
    except NameError:
        # Fallback if 'requests' library is not installed/available
        error_message = "Python 'requests' library is not available. Falling back to mock data."
        data_source = "Mock"
        stock_level = fetch_stock_level_from_api(item_id)
    except Exception as e:
        # Catch any other import or initialization error related to requests
        error_message = f"API connection issue: {e}. Falling back to mock data."
        data_source = "Mock"
        stock_level = fetch_stock_level_from_api(item_id)
        
    
    # If the real API call failed (stock_level is 0 and error_message exists), 
    # and we are running locally (like in this environment), use mock data for demo output.
    if stock_level == 0 and error_message and API_BASE_URL.startswith("http://localhost:8080"):
         stock_level = fetch_stock_level_from_api(item_id)
         if stock_level > 0:
              error_message = f"API failed. Used mock data (Stock: {stock_level}) for demonstration."
              data_source = "Mock"
         elif error_message != "":
              data_source = "Failed"


    # --- Analyze Risk ---
    if stock_level > 0:
        risk_score, status = analyze_inventory_risk(stock_level, RISK_THRESHOLDS)
    else:
        # If stock is 0 (even from mock), or the API failed entirely
        risk_score = "N/A"
        status = f"Data Retrieval Failure: {error_message}"
        data_source = "Failed"

    # --- Prepare Final Output ---
    result = {
        "item_id": item_id,
        "current_stock_level": stock_level,
        "risk_score": risk_score,
        "risk_status": status,
        "data_source": data_source,
        "thresholds_used": RISK_THRESHOLDS
    }
    
    print(f"Analysis Complete: Stock={stock_level}, Risk Score={risk_score}, Status={status}")
    return result