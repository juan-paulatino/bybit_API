from pybit.unified_trading import WebSocket, HTTP
from time import sleep
import json
import threading
import uuid # For generating unique orderLinkIds
import math # For precision calculations
from decimal import Decimal, ROUND_DOWN # For precise decimal arithmetic

# --- PASTE YOUR DEMO ACCOUNT API KEYS HERE ---
# These keys must be generated from the API page while in Demo Trading mode.
# For the trader part, these keys MUST have "Read-Write" permissions for Orders/Trade.
API_KEY = "gkyik81IrYIbYhI1pT"  # <<< REPLACE WITH YOUR ACTUAL API KEY
API_SECRET = "kWmk58Z57AOIm1mSXrbsngkL2nqdVxVYjEnw"  # <<< REPLACE WITH YOUR ACTUAL API SECRET

# --- MARKET BUY ORDER PARAMETERS ---
BUY_CATEGORY = "spot"
BUY_SYMBOL = "BTCUSDT"
BUY_SIDE = "Buy"
BUY_ORDER_TYPE = "Market"
# For a Market Buy order on the Spot market, 'QUANTITY' typically refers to the
# amount of the QUOTE CURRENCY (USDT in BTCUSDT) you want to spend.
BUY_QUANTITY = "100" # Spend 100 USDT to buy BTC at market price

# --- LIMIT SELL ORDER PARAMETERS ---
SELL_CATEGORY = "spot"
SELL_SYMBOL = "BTCUSDT"
SELL_SIDE = "Sell"
SELL_ORDER_TYPE = "Limit"
SELL_LIMIT_PRICE = "125000.99" # The target price to sell at (will be rounded)

# Global WebSocket instance
ws = None
# Flag to indicate if the WebSocket connection is ready
websocket_ready = threading.Event()

# Global dictionary to track the market buy order's status and filled quantity
# Now includes order_link_id for robust tracking
market_buy_order_info = {
    "order_id": None,
    "order_link_id": None, # Our unique ID for the buy order
    "is_filled": False,
    "filled_qty": "0" # Will store the cumExecQty once the buy order is filled
}

def handle_demo_message(message):
    """
    This function will be called for each message received from the WebSocket.
    It filters and prints specific fields from the order update.
    It also checks for the market buy order's 'Filled' status to trigger a limit sell order.
    """
    print("\n--- Received Filtered WebSocket Message ---")
    if 'data' in message and isinstance(message['data'], list):
        for order_data in message['data']:
            filtered_data = {
                "symbol": order_data.get("symbol"),
                "orderId": order_data.get("orderId"),
                "orderLinkId": order_data.get("orderLinkId"), # Include orderLinkId in filtered data
                "side": order_data.get("side"),
                "orderStatus": order_data.get("orderStatus"),
                "qty": order_data.get("qty"),
                "avgPrice": order_data.get("avgPrice"),
                "cumExecQty": order_data.get("cumExecQty"),
                "cumExecValue": order_data.get("cumExecValue"),
                "cumExecFee": order_data.get("cumExecFee"),
                "orderType": order_data.get("orderType"),
                "createdTime": order_data.get("createdTime")
            }
            print(json.dumps(filtered_data, indent=2))

            # --- Logic to trigger Limit Sell after Market Buy is Filled ---
            # Check if this update is for our market buy order using orderLinkId
            if market_buy_order_info["order_link_id"] and \
               order_data.get("orderLinkId") == market_buy_order_info["order_link_id"]:

                current_status = order_data.get("orderStatus")
                current_cum_exec_qty = order_data.get("cumExecQty")

                # If the market buy order is now 'Filled' and we haven't placed the sell order yet
                if current_status == "Filled" and not market_buy_order_info["is_filled"]:
                    market_buy_order_info["is_filled"] = True
                    market_buy_order_info["filled_qty"] = current_cum_exec_qty
                    # Optionally, store Bybit's orderId once it's available in the stream
                    market_buy_order_info["order_id"] = order_data.get("orderId")

                    print(f"\nMarket Buy Order (OrderLinkID: {market_buy_order_info['order_link_id']}) is FILLED!")
                    print(f"Filled Quantity: {market_buy_order_info['filled_qty']}")

                    # Place the limit sell order in a new thread to avoid blocking WebSocket listener
                    sell_thread = threading.Thread(
                        target=place_bybit_limit_sell_order,
                        args=(market_buy_order_info["filled_qty"], SELL_LIMIT_PRICE)
                    )
                    sell_thread.daemon = True # Allow main program to exit
                    sell_thread.start()
                    print(f"Attempting to place Limit Sell Order for {market_buy_order_info['filled_qty']} at {SELL_LIMIT_PRICE}...")
                elif current_status == "Rejected":
                    print(f"Market Buy Order (OrderLinkID: {market_buy_order_info['order_link_id']}) was REJECTED. Not placing sell order.")
                    market_buy_order_info["is_filled"] = True # Prevent further processing for this order
                elif current_status == "Cancelled":
                    print(f"Market Buy Order (OrderLinkID: {market_buy_order_info['order_link_id']}) was CANCELLED. Not placing sell order.")
                    market_buy_order_info["is_filled"] = True # Prevent further processing for this order

    else:
        print("No 'data' array found or 'data' is not a list in the message.")
        print(json.dumps(message, indent=2)) # Print full message if data structure is unexpected

    print("-------------------------------------------\n")
    # Signal that the WebSocket is ready after receiving the first message (e.g., subscription confirmation)
    if not websocket_ready.is_set():
        websocket_ready.set()

def start_websocket_listener():
    """
    Initializes and starts the Bybit WebSocket listener in a separate thread.
    """
    global ws
    print("Attempting to connect to DEMO TRADING private stream...")
    try:
        ws = WebSocket(
            testnet=False,
            demo=True,
            channel_type="private",
            api_key=API_KEY,
            api_secret=API_SECRET,
        )
        # Subscribe to the order topic to receive updates for your demo orders.
        ws.order_stream(callback=handle_demo_message)
        print("WebSocket listener started. Waiting for connection confirmation...")

        # Wait for a brief moment to allow the WebSocket to connect and subscribe
        # The handle_demo_message will set the event once a message is received
        # but a small initial sleep ensures the connection attempt has begun.
        sleep(2) # Give it a moment to connect
        if not websocket_ready.is_set():
            print("No initial WebSocket message received, proceeding anyway. Monitor console for updates.")
            websocket_ready.set() # Force set if no message after initial wait

    except Exception as e:
        print(f"Error starting WebSocket listener: {e}")
        websocket_ready.set() # Set the event to unblock main thread even on error

def place_bybit_market_buy_order():
    """
    Connects to the Bybit API and places a single market buy order.
    Stores the orderId in global state for tracking.
    """
    # Check if the user has replaced the placeholder keys
    if "api_key" in API_KEY or "api_secret" in API_SECRET:
        print("ERROR: Please open the script and replace the placeholder API_KEY and API_SECRET with your actual demo keys.")
        return

    print("\n--- Placing Market Buy Order ---")
    print("Connecting to Bybit Demo Account HTTP API...")

    session = HTTP(
        testnet=False,
        demo=True,
        api_key=API_KEY,
        api_secret=API_SECRET,
    )

    # Generate a unique orderLinkId for this buy order
    client_order_link_id = f"buy-{uuid.uuid4()}"
    market_buy_order_info["order_link_id"] = client_order_link_id # Store it before placing

    print(f"Attempting to place a {BUY_SIDE} {BUY_ORDER_TYPE} order for {BUY_QUANTITY} {BUY_SYMBOL} at market price (OrderLinkID: {client_order_link_id})...")

    try:
        order_params = {
            "category": BUY_CATEGORY,
            "symbol": BUY_SYMBOL,
            "side": BUY_SIDE,
            "orderType": BUY_ORDER_TYPE,
            "qty": BUY_QUANTITY,
            "orderLinkId": client_order_link_id, # Pass our generated ID
        }

        # For Spot Market Buy orders, Bybit's API expects 'qty' to be in the quote currency
        if BUY_CATEGORY == "spot" and BUY_SIDE == "Buy" and BUY_ORDER_TYPE == "Market":
            order_params["marketUnit"] = "quoteCoin"

        response = session.place_order(**order_params)

        print("\n--- Response from Bybit HTTP API (Market Buy) ---")
        print(json.dumps(response, indent=2))
        print("--------------------------------------------------\n")

        if response.get("retCode") == 0:
            order_id = response.get("result", {}).get("orderId")
            print(f"SUCCESS: Market Buy Order placed successfully! Order ID: {order_id}")
            # We already stored order_link_id. We can also store Bybit's order_id here if needed.
            # market_buy_order_info["order_id"] = order_id # This is now less critical for tracking
        else:
            print(f"ERROR: Market Buy Order placement failed. Reason: {response.get('retMsg')}")

    except Exception as e:
        print(f"An exception occurred during market buy order placement: {e}")

def place_bybit_limit_sell_order(quantity_to_sell, sell_price):
    """
    Connects to the Bybit API and places a single limit sell order.
    This function is called once the market buy order is confirmed as 'Filled'.
    It fetches instrument info to apply correct price and quantity precision.
    """
    # Check if the user has replaced the placeholder keys
    if "api_key" in API_KEY or "api_secret" in API_SECRET:
        print("ERROR: Please open the script and replace the placeholder API_KEY and API_SECRET with your actual demo keys.")
        return

    print(f"\n--- Preparing Limit Sell Order for {quantity_to_sell} {SELL_SYMBOL} at {sell_price} ---")
    print("Connecting to Bybit Demo Account HTTP API for Limit Sell...")

    session = HTTP(
        testnet=False,
        demo=True,
        api_key=API_KEY,
        api_secret=API_SECRET,
    )

    try:
        # --- Fetch instrument info for precision ---
        instrument_info_response = session.get_instruments_info(
            category=SELL_CATEGORY,
            symbol=SELL_SYMBOL
        )

        print("\n--- Instrument Info Response ---")
        print(json.dumps(instrument_info_response, indent=2))
        print("----------------------------------\n")

        tick_size = None
        min_qty = None
        base_precision = None

        if instrument_info_response and instrument_info_response.get("retCode") == 0:
            list_data = instrument_info_response.get("result", {}).get("list", [])
            if list_data:
                instrument = list_data[0] # Assuming the first item in the list is the correct instrument
                if 'priceFilter' in instrument:
                    tick_size = float(instrument['priceFilter'].get('tickSize'))
                if 'lotSizeFilter' in instrument:
                    min_qty = float(instrument['lotSizeFilter'].get('minOrderQty'))
                    base_precision = float(instrument['lotSizeFilter'].get('basePrecision'))

        # --- Apply Price Precision ---
        rounded_price_str = str(sell_price) # Default to original string
        if tick_size is not None:
            # Calculate number of decimal places from tick_size
            num_decimal_places_price = int(max(0, -math.log10(tick_size + 1e-9)))
            rounded_price = round(float(sell_price), num_decimal_places_price)
            rounded_price_str = f"{rounded_price:.{num_decimal_places_price}f}"
            print(f"Applying price precision: {num_decimal_places_price} decimals (tickSize: {tick_size}) -> {rounded_price_str}")
        else:
            print("WARNING: Could not fetch tickSize for price. Using default rounding (2 decimal places).")
            rounded_price_str = f"{float(sell_price):.2f}"


        # --- Apply Quantity Precision using Decimal ---
        rounded_qty_str = str(quantity_to_sell) # Default to original string
        if base_precision is not None:
            # Convert to Decimal for precise arithmetic
            qty_decimal = Decimal(str(quantity_to_sell))
            precision_decimal = Decimal(str(base_precision))

            # Round quantity to the nearest multiple of base_precision
            # Using quantize with ROUND_DOWN to ensure we don't over-sell
            # The 'exp' argument for quantize should be a Decimal representing the precision
            # e.g., for 0.000001, it's Decimal('0.000001')
            rounded_qty_decimal = qty_decimal.quantize(precision_decimal, rounding=ROUND_DOWN)
            rounded_qty_str = str(rounded_qty_decimal)

            # Ensure the string format has the correct number of decimal places for display/API
            num_decimal_places_qty = int(max(0, -math.log10(base_precision + 1e-9)))
            rounded_qty_str = f"{float(rounded_qty_str):.{num_decimal_places_qty}f}"

            print(f"Applying quantity precision: {num_decimal_places_qty} decimals (basePrecision: {base_precision}) -> {rounded_qty_str}")
        else:
            print("WARNING: Could not fetch quantity precision (basePrecision). Using default rounding (8 decimal places).")
            rounded_qty_str = f"{float(quantity_to_sell):.8f}"


        # --- Validation before placing order ---
        final_qty = float(rounded_qty_str)
        if final_qty <= 0:
            print("WARNING: Cannot place sell order with zero or negative quantity after rounding. Skipping.")
            return

        if min_qty is not None and final_qty < min_qty:
            print(f"WARNING: Rounded quantity {final_qty} is less than minimum order quantity {min_qty}. Skipping sell order.")
            return

        print(f"Attempting to place a {SELL_SIDE} {SELL_ORDER_TYPE} order for {rounded_qty_str} {SELL_SYMBOL} at a price of {rounded_price_str}...")

        response = session.place_order(
            category=SELL_CATEGORY,
            symbol=SELL_SYMBOL,
            side=SELL_SIDE,
            orderType=SELL_ORDER_TYPE,
            qty=rounded_qty_str, # Use rounded quantity
            price=rounded_price_str, # Use rounded price
        )

        print("\n--- Response from Bybit HTTP API (Limit Sell) ---")
        print(json.dumps(response, indent=2))
        print("--------------------------------------------------\n")

        if response.get("retCode") == 0:
            order_id = response.get("result", {}).get("orderId")
            print(f"SUCCESS: Limit Sell Order placed successfully! Order ID: {order_id}")
        else:
            print(f"ERROR: Limit Sell Order placement failed. Reason: {response.get('retMsg')}")

    except Exception as e:
        print(f"An exception occurred during limit sell order placement: {e}")


# Standard entry point to run the script
if __name__ == "__main__":
    # Start the WebSocket listener in a separate thread
    websocket_thread = threading.Thread(target=start_websocket_listener)
    websocket_thread.daemon = True  # Allow the main program to exit even if thread is running
    websocket_thread.start()

    # Wait until the WebSocket connection is established and ready to receive messages
    print("Waiting for WebSocket connection to be fully established...")
    websocket_ready.wait(timeout=10) # Wait up to 10 seconds for the connection to be ready

    if not websocket_ready.is_set():
        print("Warning: WebSocket connection might not be fully established. Proceeding with order placement.")
    else:
        print("WebSocket connection established and ready.")

    # Now, place the market buy order
    place_bybit_market_buy_order()

    print("\nMarket Buy Order placed. Continuing to listen for WebSocket messages and potential Limit Sell trigger. Press Ctrl+C to exit.")
    # Keep the main thread alive to allow the WebSocket thread to continue listening
    while True:
        try:
            sleep(1)
        except KeyboardInterrupt:
            print("\nExiting...")
            # Close the WebSocket connection gracefully
            if ws:
                ws.exit()
            break