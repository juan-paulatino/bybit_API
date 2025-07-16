from pybit.unified_trading import WebSocket
from time import sleep
from collections import deque
import logging
import threading
import numpy as np

# --- Configuration ---
TESTNET = False
CHANNEL_TYPE = "spot"
SYMBOL = "BTCUSDT"
WLR_WINDOW = 3000      # Window size for Weighted Linear Regression
STD_DEV_AWAY_THRESHOLD = 2.35 # Configurable threshold for Std Devs Away flag

# --- Global State ---
prices_calc = deque(maxlen=WLR_WINDOW)
ws = None
lock = threading.Lock()

# New global variables for streak detection and value storage
_streak_active = False
_first_wlr_in_streak = None # NEW: To store WLR at the start of the streak
_last_wlr_at_streak_end = None # This will now store _first_wlr_in_streak when streak ends
_last_price_at_streak_end = None # Keeps storing price at the end of the streak

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Weighted Linear Regression Calculation ---
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
        logger.warning("Sum of weights is zero in WLR calculation.")
        return None

    try:
        x_bar_w = sum(w * x for w, x in zip(weights, x_values)) / W
        y_bar_w = sum(w * y for w, y in zip(weights, y_values_list)) / W
        ss_xy_w = sum(w * (x - x_bar_w) * (y - y_bar_w) for w, x, y in zip(weights, x_values, y_values_list))
        ss_xx_w = sum(w * (x - x_bar_w)**2 for w, x in zip(weights, x_values))

        if ss_xx_w == 0:
            logger.warning("Weighted SS_xx is zero in WLR calculation. Cannot compute slope.")
            return y_bar_w

        m = ss_xy_w / ss_xx_w
        c = y_bar_w - m * x_bar_w
        y_pred_last = m * (window_size - 1) + c
        return y_pred_last
    except Exception as e:
        logger.error(f"Error during WLR calculation: {e}", exc_info=True)
        return None

# --- WebSocket Message Handler ---
def handle_message(message):
    """Processes incoming WebSocket messages."""
    global prices_calc, _streak_active, _first_wlr_in_streak, _last_wlr_at_streak_end, _last_price_at_streak_end

    try:
        if message.get("topic") == f"tickers.{SYMBOL}":
            if message.get("type") in ["snapshot", "delta"] and "data" in message:
                data = message["data"]
                usd_index_price_str = data.get("usdIndexPrice")

                if usd_index_price_str is not None and usd_index_price_str != '':
                    try:
                        current_price = float(usd_index_price_str)

                        wlr_value = None
                        with lock:
                            prices_calc.append(current_price)

                            # Progress logging for WLR_WINDOW
                            current_prices_calc_len = len(prices_calc)
                            if current_prices_calc_len < WLR_WINDOW and current_prices_calc_len % 50 == 0:
                                logger.info(f"WLR_WINDOW progress: {current_prices_calc_len}/{WLR_WINDOW}")
                            elif current_prices_calc_len == WLR_WINDOW:
                                if not hasattr(handle_message, 'wlr_window_filled'):
                                    logger.info(f"WLR_WINDOW ({WLR_WINDOW}) is now full. Starting WLR calculations.")
                                    handle_message.wlr_window_filled = True

                            if current_prices_calc_len == WLR_WINDOW: # Only calculate if window is full
                                wlr_value = calculate_wlr(deque(prices_calc), WLR_WINDOW)

                            # --- Streak Detection Logic ---
                            if wlr_value is not None: # Ensure WLR is calculated before checking streak
                                price_array = np.array(list(prices_calc))
                                std_dev = np.std(price_array)

                                price_difference = current_price - wlr_value

                                if std_dev != 0:
                                    std_devs_away = price_difference / std_dev
                                    logger.info(f"Current Price: {current_price:.4f}, WLR ({WLR_WINDOW}): {wlr_value:.4f}, Std Dev: {std_dev:.4f}, Price Difference: {price_difference:.4f}, Std Devs Away: {std_devs_away:.4f}")

                                    if abs(std_devs_away) >= STD_DEV_AWAY_THRESHOLD:
                                        # Currently in a warning state (threshold exceeded)
                                        if not _streak_active: # If this is the start of a new streak
                                            _first_wlr_in_streak = wlr_value # Capture WLR at streak beginning
                                            logger.info(f"--- WARNING STREAK BEGAN. First WLR: {_first_wlr_in_streak:.4f} ---")
                                        _streak_active = True
                                    else:
                                        # Not in a warning state. Check if a streak just ended.
                                        if _streak_active:
                                            logger.info(f"--- WARNING STREAK ENDED ---")
                                            _last_wlr_at_streak_end = _first_wlr_in_streak # Use WLR from streak beginning
                                            _last_price_at_streak_end = current_price # Use Price from streak ending
                                            logger.info(f"Captured values at streak end: Price (end)={_last_price_at_streak_end:.4f}, WLR (start)={_last_wlr_at_streak_end:.4f}")
                                            _streak_active = False
                                            _first_wlr_in_streak = None # Reset for next streak
                                else:
                                    # Standard deviation is zero. This implicitly ends any streak if active.
                                    if _streak_active:
                                        logger.info(f"--- WARNING STREAK ENDED (due to zero Std Dev) ---")
                                        _last_wlr_at_streak_end = _first_wlr_in_streak # Use WLR from streak beginning
                                        _last_price_at_streak_end = current_price # Use Price from streak ending
                                        logger.info(f"Captured values at streak end: Price (end)={_last_price_at_streak_end:.4f}, WLR (start)={_last_wlr_at_streak_end:.4f}")
                                        _streak_active = False
                                        _first_wlr_in_streak = None # Reset for next streak
                                    logger.warning("Standard deviation is zero, cannot calculate standard deviations away from WLR.")
                                    logger.info(f"Current Price: {current_price:.4f}, WLR ({WLR_WINDOW}): {wlr_value:.4f}, Std Dev: {std_dev:.4f}, Price Difference: {price_difference:.4f}")
                            # --- End Streak Detection Logic ---

                    except ValueError:
                        logger.warning(f"Could not convert usdIndexPrice '{usd_index_price_str}' to float.")
                    except Exception as e:
                        logger.error(f"Error processing price data: {e}", exc_info=True)

    except Exception as e:
        logger.error(f"Error in handle_message: {e}", exc_info=True)
        logger.error(f"Problematic message: {message}")


# --- WebSocket Connection Function ---
def start_websocket_connection():
    global ws
    logger.info(f"Connecting to Bybit {CHANNEL_TYPE} WebSocket ({'Testnet' if TESTNET else 'Mainnet'})...")
    logger.info(f"Subscribing to {SYMBOL} tickers and calculating {WLR_WINDOW}-period WLR.")

    ws = WebSocket(
        testnet=TESTNET,
        channel_type=CHANNEL_TYPE
    )

    ws.ticker_stream(
        symbol=SYMBOL,
        callback=handle_message
    )

    while True:
        try:
            sleep(60)
        except Exception as e:
            logger.error(f"Error in WebSocket monitoring loop: {e}", exc_info=True)
            sleep(5)


# --- Main Execution ---
if __name__ == "__main__":
    # Initialize the flag for WLR window completion progress
    handle_message.wlr_window_filled = False

    ws_thread = threading.Thread(target=start_websocket_connection, daemon=True)
    ws_thread.start()

    logger.info("Script started. Monitoring WebSocket data...")

    try:
        while ws_thread.is_alive():
            sleep(1)
    except KeyboardInterrupt:
        logger.info("Script interrupted by user (Ctrl+C). Exiting.")
    except Exception as e:
        logger.error(f"Error in main loop: {e}", exc_info=True)
    finally:
        logger.info("Script finished.")
        # You can access _last_wlr_at_streak_end and _last_price_at_streak_end here
        # if you need to do something with them after the script exits.
        if _last_wlr_at_streak_end is not None:
            logger.info(f"Final captured values (from last streak end): Price (end)={_last_price_at_streak_end:.4f}, WLR (start)={_last_wlr_at_streak_end:.4f}")