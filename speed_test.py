import time
import json
from pybit.unified_trading import HTTP
from decimal import Decimal, getcontext

# --- Configuration ---
# IMPORTANT: Use Read-Write API keys created in Demo Mode.
API_KEY = "api_key"
API_SECRET = "api_secret"

# Order Details
CATEGORY = "spot"
SYMBOL = "BTCUSDT"
# Minimum order value on Bybit for BTC/USDT is ~1 USDT.
# At a low price of 000, we need at least 0.001 BTC.
QUANTITY = "0.005"

# Speed Test Parameters
NUM_ORDERS = 100
START_PRICE = Decimal("1000.0")
PRICE_INCREMENT = Decimal("0.1")
DELAY_SECONDS = 0.150  # 55 milliseconds (Original comment was 150 milliseconds, adjusted to match value)

def run_speed_test():
    """
    Connects to Bybit and rapidly places a sequence of orders.
    """
    if "api_key" in API_KEY or "api_secret" in API_SECRET: # Changed "PASTE_YOUR" to "api_key" and "api_secret" to match initial config
        print("FATAL ERROR: Please open the script and paste your actual API Key and Secret.")
        return

    # Set precision for Decimal calculations
    getcontext().prec = 10

    print("--- Bybit Order Speed Test ---")
    print(f"This script will attempt to place {NUM_ORDERS} orders.")
    print("!!! WARNING: ENSURE YOU ARE USING DEMO ACCOUNT KEYS !!!")

    # Countdown to give the user a chance to cancel
    for i in range(5, 0, -1):
        print(f"Starting in {i}...")
        time.sleep(1)

    print("\nConnecting to Bybit Demo Account...")
    session = HTTP(
        testnet=False, # Set to False for the actual Bybit demo environment, True for testnet.
        demo=True, # This is CRITICAL for using the demo account
        api_key=API_KEY,
        api_secret=API_SECRET,
    )

    successful_orders = 0
    failed_orders = 0

    try:
        for i in range(NUM_ORDERS):
            current_price = START_PRICE + (i * PRICE_INCREMENT)

            print(f"Placing order {i+1}/{NUM_ORDERS} | Price: {current_price:.1f} USDT...")

            try:
                response = session.place_order(
                    category=CATEGORY,
                    symbol=SYMBOL,
                    side="Buy",
                    orderType="Limit",
                    qty=str(QUANTITY),
                    price=str(current_price),
                )

                if response.get("retCode") == 0:
                    successful_orders += 1
                else:
                    failed_orders += 1
                    print(f"  -> FAILED: {response.get('retMsg')}")

            except Exception as e:
                failed_orders += 1
                print(f"  -> FAILED with exception: {e}")

            # Wait for the specified interval
            time.sleep(DELAY_SECONDS)

    except KeyboardInterrupt:
        print("\n--> Test stopped by user.")
    except Exception as e:
        print(f"\nA critical exception occurred: {e}")

    finally:
        print("\n--- Speed Test Complete ---")
        print(f"Successful orders: {successful_orders}")
        print(f"Failed orders: {failed_orders}")


if __name__ == "__main__":
    run_speed_test()