from pybit.unified_trading import WebSocket, HTTP
from time import sleep
from collections import deque
import json
import threading
import uuid # For generating unique orderLinkIds
import math # For precision calculations
from decimal import Decimal, ROUND_DOWN # For precise decimal arithmetic
import logging
import numpy as np # <--- MAKE SURE THIS LINE IS PRESENT AND AT THE TOP

# --- Configure Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__) # Use a specific logger

# --- PASTE YOUR DEMO ACCOUNT API KEYS HERE ---
# These keys must be generated from the API page while in Demo Trading mode.
# For the trader part, these keys MUST have "Read-Write" permissions for Orders/Trade.
API_KEY = "api_key"  # <<< REPLACE WITH YOUR ACTUAL API KEY
API_SECRET = "api_secret"  # <<< REPLACE WITH YOUR ACTUAL API SECRET

# --- ANALYST CONFIGURATION ---
ANALYST_TESTNET = False
ANALYST_CHANNEL_TYPE = "spot"
ANALYST_SYMBOL = "BTCUSDT" # Changed from POPCATUSDT to BTCUSDT based on your trader script
ANALYST_WLR_WINDOW = 2500
ANALYST_STD_DEV_AWAY_THRESHOLD = 2.25 # Configurable threshold for Std Devs Away flag

# --- TRADER CONFIGURATION ---
TRADER_CATEGORY = "spot"
TRADER_SYMBOL = "BTCUSDT"
# For a Market Buy order on the Spot market, 'QUANTITY' typically refers to the
# amount of the QUOTE CURRENCY (USDT in BTCUSDT) you want to spend.
TRADER_BUY_QUANTITY_USDT = "100" # Spend 100 USDT to buy BTC at market price
TRADER_SELL_SIDE = "Sell"
TRADER_ORDER_TYPE_LIMIT = "Limit"
TRADER_ORDER_TYPE_MARKET = "Market"

# --- GLOBAL STATE (SHARED BETWEEN ANALYST AND TRADER THREADS) ---
# Analyst State
prices_calc = deque(maxlen=ANALYST_WLR_WINDOW)
ws_analyst = None # Analyst WebSocket instance

# Streak detection variables (modified from analyst script)
_streak_active = False
_first_wlr_in_streak = None # Stores WLR when streak *starts*
_last_price_at_streak_end = None # Stores Price when streak *ends*

# Trader State
ws_trader = None # Trader WebSocket instance
websocket_ready = threading.Event() # Signals when trader websocket is connected

# Global dictionary to track the market buy order's status and filled quantity
market_buy_order_info = {
    "order_id": None,
    "order_link_id": None,
    "is_filled": False,
    "filled_qty": "0" # Will store the cumExecQty once the buy order is filled
}

# New global variable to store the captured sell order data
captured_sell_order_data = None
sell_order_captured_event = threading.Event() # Event to signal that sell order data has been captured

# --- Control Flags for Trading Logic ---
# Protect shared state with a lock
shared_state_lock = threading.Lock()

# Flags for controlling trading flow based on analyst signals and trade progress
trade_cycle_in_progress = False # True from Market Buy signal until Limit Sell is filled
prepared_sell_price = None # Stores the WLR from streak start, for the limit sell order

# --- ANALYST FUNCTIONS ---

def calculate_wlr(y_values, window_size):
    """
    Calculates the Weighted Linear Regression value at the last point.
    Uses linear weights (1, 2, ..., window_size) where the most recent point
    has the highest weight (window_size).
    Returns the predicted y-value for the last x-value in the window.
    """
    if len(y_values) != window_size:
        return None

    y_values_list = list(y_values)
    x_values = list(range(window_size))
    weights = list(range(1, window_size + 1))
    W = sum(weights)

    if W == 0:
        logger.warning("Analyst: Sum of weights is zero in WLR calculation.")
        return None

    try:
        x_bar_w = sum(w * x for w, x in zip(weights, x_values)) / W
        y_bar_w = sum(w * y for w, y in zip(weights, y_values_list)) / W
        ss_xy_w = sum(w * (x - x_bar_w) * (y - y_bar_w) for w, x, y in zip(weights, x_values, y_values_list))
        ss_xx_w = sum(w * (x - x_bar_w)**2 for w, x in zip(weights, x_values))

        if ss_xx_w == 0:
            logger.warning("Analyst: Weighted SS_xx is zero in WLR calculation. Cannot compute slope.")
            return y_bar_w

        m = ss_xy_w / ss_xx_w
        c = y_bar_w - m * x_bar_w
        y_pred_last = m * (window_size - 1) + c
        return y_pred_last
    except Exception as e:
        logger.error(f"Analyst: Error during WLR calculation: {e}", exc_info=True)
        return None

def handle_analyst_message(message):
    """
    Processes incoming WebSocket messages for the analyst.
    Detects streaks and updates global state variables for the trader.
    """
    global prices_calc, _streak_active, _first_wlr_in_streak, _last_price_at_streak_end, trade_cycle_in_progress, prepared_sell_price

    try:
        if message.get("topic") == f"tickers.{ANALYST_SYMBOL}":
            if message.get("type") in ["snapshot", "delta"] and "data" in message:
                data = message["data"]
                usd_index_price_str = data.get("usdIndexPrice")

                if usd_index_price_str is not None and usd_index_price_str != '':
                    try:
                        current_price = float(usd_index_price_str)

                        wlr_value = None
                        with shared_state_lock: # Use shared lock for all global state access
                            prices_calc.append(current_price)

                            # Progress logging for WLR_WINDOW
                            current_prices_calc_len = len(prices_calc)
                            if current_prices_calc_len < ANALYST_WLR_WINDOW and current_prices_calc_len % 50 == 0:
                                logger.info(f"Analyst: WLR_WINDOW progress: {current_prices_calc_len}/{ANALYST_WLR_WINDOW}")
                            elif current_prices_calc_len == ANALYST_WLR_WINDOW:
                                if not hasattr(handle_analyst_message, 'wlr_window_filled'):
                                    logger.info(f"Analyst: WLR_WINDOW ({ANALYST_WLR_WINDOW}) is now full. Starting WLR calculations.")
                                    handle_analyst_message.wlr_window_filled = True

                            if current_prices_calc_len == ANALYST_WLR_WINDOW:
                                wlr_value = calculate_wlr(deque(prices_calc), ANALYST_WLR_WINDOW)

                            # --- Streak Detection Logic (Analyst) ---
                            if wlr_value is not None:
                                price_array = np.array(list(prices_calc))
                                std_dev = np.std(price_array)

                                price_difference = current_price - wlr_value # Current Price - WLR

                                if std_dev != 0:
                                    std_devs_away = price_difference / std_dev
                                    logger.info(f"Analyst: Price: {current_price:.4f}, WLR ({ANALYST_WLR_WINDOW}): {wlr_value:.4f}, Std Dev: {std_dev:.4f}, Diff: {price_difference:.4f}, Std Devs Away: {std_devs_away:.4f}")

                                    if abs(std_devs_away) >= ANALYST_STD_DEV_AWAY_THRESHOLD:
                                        # Currently in a warning state (threshold exceeded)
                                        logger.warning(f"Analyst: !!! Std Devs Away ({std_devs_away:.4f}) >= Configured Threshold ({ANALYST_STD_DEV_AWAY_THRESHOLD:.4f}) at price {current_price:.4f} !!!")
                                        if not _streak_active: # If this is the start of a new streak
                                            _first_wlr_in_streak = wlr_value # Capture WLR at streak beginning
                                            _streak_active = True
                                            logger.info(f"Analyst: --- WARNING STREAK BEGAN. First WLR: {_first_wlr_in_streak:.4f} ---")

                                            # --- TRADER SIGNAL: Prepare Limit Sell (if conditions met) ---
                                            # Only if STD_DEV_AWAY_THRESHOLD (i.e. price_difference) is negative
                                            # and no trade cycle is in progress
                                            if price_difference < 0 and not trade_cycle_in_progress:
                                                prepared_sell_price = _first_wlr_in_streak
                                                logger.info(f"Trader: Signal detected to PREPARE Limit Sell at {prepared_sell_price:.4f}. Waiting for streak to end.")
                                            else:
                                                logger.info(f"Trader: Buy signal not met (price_diff not negative or trade already active).")

                                    else:
                                        # Not in a warning state. Check if a streak just ended.
                                        if _streak_active:
                                            logger.info(f"Analyst: --- WARNING STREAK ENDED ---")
                                            _last_price_at_streak_end = current_price # Price at streak end
                                            # _last_wlr_at_streak_end is implicitly _first_wlr_in_streak
                                            logger.info(f"Analyst: Captured values at streak end: Price (end)={_last_price_at_streak_end:.4f}, WLR (start)={_first_wlr_in_streak:.4f}")
                                            
                                            # --- TRADER SIGNAL: Place Market Buy (if conditions met) ---
                                            # Only if a sell price was prepared and no trade cycle is active
                                            if prepared_sell_price is not None and not trade_cycle_in_progress:
                                                logger.info("Trader: Streak ended. Placing Market Buy Order!")
                                                trade_cycle_in_progress = True # Set halt flag
                                                # Call the function from the trader part
                                                # Need to release lock before calling blocking function
                                                # Or call it from the trader thread, which is better
                                                threading.Thread(target=place_bybit_market_buy_order, args=()).start()
                                            elif trade_cycle_in_progress:
                                                logger.info("Trader: Streak ended, but trade cycle already in progress. No new buy order.")
                                            else:
                                                logger.info("Trader: Streak ended, but no sell price was prepared. No buy order.")

                                            _streak_active = False
                                            _first_wlr_in_streak = None # Reset for next streak
                                            # prepared_sell_price = None  Reset prepared sell price
                                else:
                                    # Standard deviation is zero. This implicitly ends any streak if active.
                                    if _streak_active:
                                        logger.info(f"Analyst: --- WARNING STREAK ENDED (due to zero Std Dev) ---")
                                        _last_price_at_streak_end = current_price
                                        logger.info(f"Analyst: Captured values at streak end: Price (end)={_last_price_at_streak_end:.4f}, WLR (start)={_first_wlr_in_streak:.4f}")

                                        # --- TRADER SIGNAL: End streak, but no buy order if trade active ---
                                        if prepared_sell_price is not None and not trade_cycle_in_progress:
                                            logger.info("Trader: Streak ended (zero Std Dev). Placing Market Buy Order!")
                                            trade_cycle_in_progress = True # Set halt flag
                                            threading.Thread(target=place_bybit_market_buy_order, args=()).start()
                                        elif trade_cycle_in_progress:
                                            logger.info("Trader: Streak ended (zero Std Dev), but trade cycle already in progress. No new buy order.")
                                        else:
                                            logger.info("Trader: Streak ended (zero Std Dev), but no sell price was prepared. No buy order.")

                                        _streak_active = False
                                        _first_wlr_in_streak = None
                                        prepared_sell_price = None # Reset prepared sell price

                                    logger.warning("Analyst: Standard deviation is zero, cannot calculate standard deviations away from WLR.")
                                    logger.info(f"Analyst: Price: {current_price:.4f}, WLR ({ANALYST_WLR_WINDOW}): {wlr_value:.4f}, Std Dev: {std_dev:.4f}, Diff: {price_difference:.4f}")

                            # --- End Streak Detection Logic ---

                    except ValueError:
                        logger.warning(f"Analyst: Could not convert usdIndexPrice '{usd_index_price_str}' to float.")
                    except Exception as e:
                        logger.error(f"Analyst: Error processing price data: {e}", exc_info=True)

    except Exception as e:
        logger.error(f"Analyst: Error in handle_analyst_message: {e}", exc_info=True)
        logger.error(f"Analyst: Problematic message: {message}")


def start_analyst_websocket_connection():
    """Initializes and starts the Bybit WebSocket listener for the analyst."""
    global ws_analyst
    logger.info(f"Analyst: Connecting to Bybit {ANALYST_CHANNEL_TYPE} WebSocket ({'Testnet' if ANALYST_TESTNET else 'Mainnet'})...")
    logger.info(f"Analyst: Subscribing to {ANALYST_SYMBOL} tickers and calculating {ANALYST_WLR_WINDOW}-period WLR.")

    ws_analyst = WebSocket(
        testnet=ANALYST_TESTNET,
        channel_type=ANALYST_CHANNEL_TYPE
    )

    ws_analyst.ticker_stream(
        symbol=ANALYST_SYMBOL,
        callback=handle_analyst_message
    )

    while True:
        try:
            sleep(60)
        except Exception as e:
            logger.error(f"Analyst: Error in WebSocket monitoring loop: {e}", exc_info=True)
            sleep(5)


# --- TRADER FUNCTIONS ---

def handle_trader_message(message):
    """
    This function will be called for each message received from the Trader WebSocket.
    It filters and prints specific fields from the order update.
    It also checks for the market buy order's 'Filled' status to trigger a limit sell order.
    Additionally, it captures data for a 'Filled' Limit 'Sell' order.
    """
    global captured_sell_order_data
    global trade_cycle_in_progress # ADD THIS LINE
    global prepared_sell_price     # ADD THIS LINE

    logger.info("\nTrader: --- Received Filtered WebSocket Message ---")
    if 'data' in message and isinstance(message['data'], list):
        for order_data in message['data']:
            filtered_data = {
                "symbol": order_data.get("symbol"),
                "orderId": order_data.get("orderId"),
                "orderLinkId": order_data.get("orderLinkId"),
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
            logger.info(json.dumps(filtered_data, indent=2))

            # --- Logic to trigger Limit Sell after Market Buy is Filled ---
            with shared_state_lock: # Protect access to market_buy_order_info
                if market_buy_order_info["order_link_id"] and \
                   order_data.get("orderLinkId") == market_buy_order_info["order_link_id"]:

                    current_status = order_data.get("orderStatus")
                    current_cum_exec_qty = order_data.get("cumExecQty")

                    if current_status == "Filled" and not market_buy_order_info["is_filled"]:
                        market_buy_order_info["is_filled"] = True
                        market_buy_order_info["filled_qty"] = current_cum_exec_qty
                        market_buy_order_info["order_id"] = order_data.get("orderId")

                        logger.info(f"\nTrader: Market Buy Order (OrderLinkID: {market_buy_order_info['order_link_id']}) is FILLED!")
                        logger.info(f"Trader: Filled Quantity: {market_buy_order_info['filled_qty']}")

                        # Place the prepared Limit Sell Order now
                        if prepared_sell_price is not None:
                            logger.info(f"Trader: Market Buy Filled. Now placing PREPARED Limit Sell Order for {market_buy_order_info['filled_qty']} at {prepared_sell_price:.4f}...")
                            threading.Thread(
                                target=place_bybit_limit_sell_order,
                                args=(market_buy_order_info["filled_qty"], prepared_sell_price)
                            ).start()
                        else:
                            logger.warning("Trader: Market Buy Filled, but no prepared sell price found. Cannot place limit sell.")
                            trade_cycle_in_progress = False # Reset if no sell can be placed

                    elif current_status == "Rejected":
                        logger.warning(f"Trader: Market Buy Order (OrderLinkID: {market_buy_order_info['order_link_id']}) was REJECTED. Not placing sell order.")
                        market_buy_order_info["is_filled"] = True
                        trade_cycle_in_progress = False # Reset halt on rejection
                        prepared_sell_price = None # <--- ADD THIS HERE!
                    elif current_status == "Cancelled":
                        logger.warning(f"Trader: Market Buy Order (OrderLinkID: {market_buy_order_info['order_link_id']}) was CANCELLED. Not placing sell order.")
                        market_buy_order_info["is_filled"] = True
                        trade_cycle_in_progress = False # Reset halt on cancellation
                        prepared_sell_price = None # <--- ADD THIS HERE!

                # --- Logic to capture specific Sell Limit Filled order data ---
                # Ensure we haven't already captured this data to avoid duplicates
                if not sell_order_captured_event.is_set() and \
                   order_data.get("side") == "Sell" and \
                   order_data.get("orderStatus") == "Filled" and \
                   order_data.get("orderType") == "Limit":

                    captured_sell_order_data = filtered_data
                    sell_order_captured_event.set() # Signal that data has been captured
                    logger.info("\nTrader: --- Captured Sell Limit Order (Filled) Data! ---")
                    logger.info(json.dumps(captured_sell_order_data, indent=2))
                    logger.info("Trader: --------------------------------------------------\n")
                    # Trade cycle completed, reset halt flag
                    trade_cycle_in_progress = False
                    prepared_sell_price = None # <--- ADD THIS HERE!
                    # Reset market buy info for next cycle
                    market_buy_order_info["order_id"] = None
                    market_buy_order_info["order_link_id"] = None
                    market_buy_order_info["is_filled"] = False
                    market_buy_order_info["filled_qty"] = "0"


    else:
        logger.info("Trader: No 'data' array found or 'data' is not a list in the message.")
        logger.info(json.dumps(message, indent=2))

    logger.info("Trader: -------------------------------------------\n")
    if not websocket_ready.is_set():
        websocket_ready.set()

def start_trader_websocket_listener():
    """
    Initializes and starts the Bybit WebSocket listener for the trader (private stream).
    """
    global ws_trader
    logger.info("Trader: Attempting to connect to DEMO TRADING private stream...")
    try:
        ws_trader = WebSocket(
            testnet=False,
            demo=True,
            channel_type="private",
            api_key=API_KEY,
            api_secret=API_SECRET,
        )
        ws_trader.order_stream(callback=handle_trader_message)
        logger.info("Trader: WebSocket listener started. Waiting for connection confirmation...")
        sleep(2) # Give a moment for initial messages
        if not websocket_ready.is_set():
            logger.warning("Trader: No initial WebSocket message received, proceeding anyway. Monitor console for updates.")
            websocket_ready.set()

        # Added loop to keep this thread alive
        while True:
            sleep(60) # Keep thread alive, pybit handles callback dispatch
    except Exception as e:
        logger.error(f"Trader: FATAL ERROR in WebSocket listener: {e}", exc_info=True) # Log full traceback
        websocket_ready.set()

def place_bybit_market_buy_order():
    """
    Connects to the Bybit API and places a single market buy order.
    Stores the orderId in global state for tracking.
    This function is called by the analyst thread when conditions are met.
    """
    if "api_key" in API_KEY or "api_secret" in API_SECRET:
        logger.error("Trader: ERROR: Please open the script and replace the placeholder API_KEY and API_SECRET with your actual demo keys.")
        with shared_state_lock:
            trade_cycle_in_progress = False # Release halt if keys are missing
        return

    logger.info("\nTrader: --- Placing Market Buy Order ---")
    logger.info("Trader: Connecting to Bybit Demo Account HTTP API...")

    session = HTTP(
        testnet=False,
        demo=True,
        api_key=API_KEY,
        api_secret=API_SECRET,
    )

    client_order_link_id = f"buy-{uuid.uuid4()}"
    with shared_state_lock: # Protect market_buy_order_info update
        market_buy_order_info["order_link_id"] = client_order_link_id
        market_buy_order_info["is_filled"] = False # Reset for new order

    logger.info(f"Trader: Attempting to place a Buy Market order for {TRADER_BUY_QUANTITY_USDT} USDT worth of {TRADER_SYMBOL} (OrderLinkID: {client_order_link_id})...")

    try:
        order_params = {
            "category": TRADER_CATEGORY,
            "symbol": TRADER_SYMBOL,
            "side": "Buy", # Always Buy for this market order
            "orderType": TRADER_ORDER_TYPE_MARKET,
            "qty": TRADER_BUY_QUANTITY_USDT, # This is quoteCoin qty for spot market buy
            "orderLinkId": client_order_link_id,
        }

        if TRADER_CATEGORY == "spot" and "Buy" == "Buy" and TRADER_ORDER_TYPE_MARKET == "Market":
            order_params["marketUnit"] = "quoteCoin" # Buy in terms of USDT spent

        response = session.place_order(**order_params)

        logger.info("\nTrader: --- Response from Bybit HTTP API (Market Buy) ---")
        logger.info(json.dumps(response, indent=2))
        logger.info("Trader: --------------------------------------------------\n")

        if response.get("retCode") == 0:
            order_id = response.get("result", {}).get("orderId")
            with shared_state_lock: # Protect order_id update
                market_buy_order_info["order_id"] = order_id
            logger.info(f"Trader: SUCCESS: Market Buy Order placed successfully! Order ID: {order_id}")
        else:
            logger.error(f"Trader: ERROR: Market Buy Order placement failed. Reason: {response.get('retMsg')}")
            with shared_state_lock:
                trade_cycle_in_progress = False # Release halt on failure

    except Exception as e:
        logger.error(f"Trader: An exception occurred during market buy order placement: {e}")
        with shared_state_lock:
            trade_cycle_in_progress = False # Release halt on exception

def place_bybit_limit_sell_order(quantity_to_sell, sell_price):
    """
    Connects to the Bybit API and places a single limit sell order.
    This function is called once the market buy order is confirmed as 'Filled'.
    It fetches instrument info to apply correct price and quantity precision.
    """
    if "api_key" in API_KEY or "api_secret" in API_SECRET:
        logger.error("Trader: ERROR: Please open the script and replace the placeholder API_KEY and API_SECRET with your actual demo keys.")
        with shared_state_lock:
            trade_cycle_in_progress = False # Release halt if keys are missing
        return

    logger.info(f"\nTrader: --- Preparing Limit Sell Order for {quantity_to_sell} {TRADER_SYMBOL} at {sell_price} ---")
    logger.info("Trader: Connecting to Bybit Demo Account HTTP API for Limit Sell...")

    session = HTTP(
        testnet=False,
        demo=True,
        api_key=API_KEY,
        api_secret=API_SECRET,
    )

    try:
        # --- Fetch instrument info for precision ---
        instrument_info_response = session.get_instruments_info(
            category=TRADER_CATEGORY,
            symbol=TRADER_SYMBOL
        )

        logger.info("\nTrader: --- Instrument Info Response ---")
        logger.info(json.dumps(instrument_info_response, indent=2))
        logger.info("Trader: ----------------------------------\n")

        tick_size = None
        min_qty = None
        base_precision = None

        if instrument_info_response and instrument_info_response.get("retCode") == 0:
            list_data = instrument_info_response.get("result", {}).get("list", [])
            if list_data:
                instrument = list_data[0]
                if 'priceFilter' in instrument:
                    tick_size = float(instrument['priceFilter'].get('tickSize'))
                if 'lotSizeFilter' in instrument:
                    min_qty = float(instrument['lotSizeFilter'].get('minOrderQty'))
                    base_precision = float(instrument['lotSizeFilter'].get('basePrecision'))

        # --- Apply Price Precision ---
        rounded_price_str = str(sell_price)
        if tick_size is not None:
            # Safely calculate number of decimal places for tick_size
            num_decimal_places_price = 0
            if tick_size < 1:
                # Count decimal places for tick_size like 0.01 (2), 0.0001 (4)
                num_decimal_places_price = len(str(tick_size).split('.')[-1])
            rounded_price = round(float(sell_price), num_decimal_places_price)
            rounded_price_str = f"{rounded_price:.{num_decimal_places_price}f}"
            logger.info(f"Trader: Applying price precision: {num_decimal_places_price} decimals (tickSize: {tick_size}) -> {rounded_price_str}")
        else:
            logger.warning("Trader: Could not fetch tickSize for price. Using default rounding (2 decimal places).")
            rounded_price_str = f"{float(sell_price):.2f}"


        # --- Apply Quantity Precision using Decimal ---
        rounded_qty_str = str(quantity_to_sell)
        if base_precision is not None:
            qty_decimal = Decimal(str(quantity_to_sell))
            # Determine the number of decimal places for base_precision
            num_decimal_places_qty = 0
            if base_precision < 1:
                num_decimal_places_qty = len(str(base_precision).split('.')[-1])
            
            # Create a quantize template, e.g., Decimal('0.00000001') for 8 decimal places
            quantize_template = Decimal('1e-' + str(num_decimal_places_qty))
            
            rounded_qty_decimal = qty_decimal.quantize(quantize_template, rounding=ROUND_DOWN)
            rounded_qty_str = str(rounded_qty_decimal)

            logger.info(f"Trader: Applying quantity precision: {num_decimal_places_qty} decimals (basePrecision: {base_precision}) -> {rounded_qty_str}")
        else:
            logger.warning("Trader: Could not fetch quantity precision (basePrecision). Using default rounding (8 decimal places).")
            rounded_qty_str = f"{float(quantity_to_sell):.8f}"


        # --- Validation before placing order ---
        final_qty = float(rounded_qty_str)
        if final_qty <= 0:
            logger.warning("Trader: Cannot place sell order with zero or negative quantity after rounding. Skipping.")
            with shared_state_lock:
                trade_cycle_in_progress = False # Release halt if quantity is invalid
            return

        if min_qty is not None and final_qty < min_qty:
            logger.warning(f"Trader: Rounded quantity {final_qty} is less than minimum order quantity {min_qty}. Skipping sell order.")
            with shared_state_lock:
                trade_cycle_in_progress = False # Release halt if quantity is too small
            return

        logger.info(f"Trader: Attempting to place a {TRADER_SELL_SIDE} {TRADER_ORDER_TYPE_LIMIT} order for {rounded_qty_str} {TRADER_SYMBOL} at a price of {rounded_price_str}...")

        response = session.place_order(
            category=TRADER_CATEGORY,
            symbol=TRADER_SYMBOL,
            side=TRADER_SELL_SIDE,
            orderType=TRADER_ORDER_TYPE_LIMIT,
            qty=rounded_qty_str,
            price=rounded_price_str,
        )

        logger.info("\nTrader: --- Response from Bybit HTTP API (Limit Sell) ---")
        logger.info(json.dumps(response, indent=2))
        logger.info("Trader: --------------------------------------------------\n")

        if response.get("retCode") == 0:
            order_id = response.get("result", {}).get("orderId")
            logger.info(f"Trader: SUCCESS: Limit Sell Order placed successfully! Order ID: {order_id}")
        else:
            logger.error(f"Trader: ERROR: Limit Sell Order placement failed. Reason: {response.get('retMsg')}")
            with shared_state_lock:
                trade_cycle_in_progress = False # Release halt on failure

    except Exception as e:
        logger.error(f"Trader: An exception occurred during limit sell order placement: {e}")
        with shared_state_lock:
            trade_cycle_in_progress = False # Release halt on exception


# --- Main Execution ---
if __name__ == "__main__":
    # Initialize the flag for WLR window completion progress in analyst
    handle_analyst_message.wlr_window_filled = False

    # Start Analyst WebSocket in a separate daemon thread
    analyst_thread = threading.Thread(target=start_analyst_websocket_connection, daemon=True)
    analyst_thread.start()
    logger.info("Main: Analyst thread started.")

    # Start Trader WebSocket listener in a separate daemon thread
    trader_websocket_thread = threading.Thread(target=start_trader_websocket_listener, daemon=True)
    trader_websocket_thread.start()
    logger.info("Main: Trader WebSocket listener started. Waiting for connection...")

    # Wait for Trader WebSocket connection to be fully established (optional, but good practice)
    websocket_ready.wait(timeout=15)
    if not websocket_ready.is_set():
        logger.warning("Main: Trader WebSocket connection might not be fully established within timeout.")
    else:
        logger.info("Main: Trader WebSocket connection established and ready.")

    logger.info("Main: Script started. Monitoring WebSocket data and awaiting trading signals. Press Ctrl+C to exit.")

    try:
        # Keep the main thread alive indefinitely until Ctrl+C is pressed
        while True:
            sleep(1) # Sleep briefly to prevent high CPU usage
            # Optional: Add checks if you want to exit if a specific thread dies unexpectedly
            if not analyst_thread.is_alive():
                logger.error("Main: Analyst thread died unexpectedly. Exiting main script.")
                break
            if not trader_websocket_thread.is_alive():
                logger.error("Main: Trader WebSocket thread died unexpectedly. Exiting main script.")
                break

    except KeyboardInterrupt:
        logger.info("Main: Script interrupted by user (Ctrl+C). Exiting.")
    except Exception as e:
        logger.error(f"Main: Error in main loop: {e}", exc_info=True)
    finally:
        logger.info("Main: Script finished.")
        # Attempt to gracefully close WebSockets
        if ws_analyst:
            try:
                ws_analyst.exit()
                logger.info("Main: Analyst WebSocket closed.")
            except Exception as e:
                logger.error(f"Main: Error closing Analyst WebSocket: {e}")
        if ws_trader:
            try:
                ws_trader.exit()
                logger.info("Main: Trader WebSocket closed.")
            except Exception as e:
                logger.error(f"Main: Error closing Trader WebSocket: {e}")

        # Final state of trade cycle for debugging if script exits during a trade
        with shared_state_lock:
            if trade_cycle_in_progress:
                logger.info(f"Main: Script exited with trade_cycle_in_progress = True. Last Market Buy OrderLinkID: {market_buy_order_info['order_link_id']}")
            if captured_sell_order_data:
                logger.info("\nMain: --- FINAL CAPTURED SELL ORDER DATA ---")
                logger.info(json.dumps(captured_sell_order_data, indent=2))
                logger.info("Main: ------------------------------------------\n")