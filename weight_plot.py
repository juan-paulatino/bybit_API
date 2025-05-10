# /Users/juancarlospaulinofonsecagomez/Bybit_Socket/weight.py
# Modified for Weighted Linear Regression with Plotting

from pybit.unified_trading import WebSocket
from time import sleep
from collections import deque
import logging
import matplotlib.pyplot as plt
import matplotlib.animation as animation # Can use this for smoother updates, but plt.pause is simpler for this case
import threading # To run WebSocket in a background thread

# --- Configuration ---
TESTNET = False # Set back to False as per your file
CHANNEL_TYPE = "spot"
SYMBOL = "BTCUSDT"
WLR_WINDOW = 1250      # Window size for Weighted Linear Regression (Reduced for faster initial plot)
PLOT_HISTORY = 1250   # How many points to show on the plot

# --- Global State ---
# Deques for calculation
prices_calc = deque(maxlen=WLR_WINDOW)
# Deques for plotting (can store more history)
plot_x = deque(maxlen=PLOT_HISTORY)
plot_prices = deque(maxlen=PLOT_HISTORY)
plot_wlr = deque(maxlen=PLOT_HISTORY)
plot_counter = 0 # Simple counter for x-axis

ws = None # Initialize WebSocket object reference
lock = threading.Lock() # To safely access shared data between threads

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__) # Use a specific logger

# --- Weighted Linear Regression Calculation ---
def calculate_wlr(y_values, window_size):
    """
    Calculates the Weighted Linear Regression value at the last point.
    Uses linear weights (1, 2, ..., window_size) where the most recent point
    has the highest weight (window_size).
    Returns the predicted y-value for the last x-value in the window.
    """
    # Ensure we have exactly the right number of points for calculation
    if len(y_values) != window_size:
        # logger.debug(f"Not enough data for WLR calc: got {len(y_values)}, need {window_size}")
        return None

    # Convert to list for consistent indexing if needed (deque is iterable)
    y_values_list = list(y_values)
    x_values = list(range(window_size))
    weights = list(range(1, window_size + 1))

    # --- Use basic Python loops for WLR calculation ---
    W = sum(weights)
    if W == 0:
        logger.warning("Sum of weights is zero in WLR calculation.")
        return None

    try:
        # Weighted means
        x_bar_w = sum(w * x for w, x in zip(weights, x_values)) / W
        y_bar_w = sum(w * y for w, y in zip(weights, y_values_list)) / W

        # Weighted sums of squares and cross-products
        ss_xy_w = sum(w * (x - x_bar_w) * (y - y_bar_w)
                      for w, x, y in zip(weights, x_values, y_values_list))
        ss_xx_w = sum(w * (x - x_bar_w)**2
                      for w, x in zip(weights, x_values))

        if ss_xx_w == 0:
            logger.warning("Weighted SS_xx is zero in WLR calculation. Cannot compute slope.")
            return y_bar_w # Return weighted average as fallback

        # Calculate slope (m) and intercept (c)
        m = ss_xy_w / ss_xx_w
        c = y_bar_w - m * x_bar_w

        # Calculate the fitted value at the *last* point (x = window_size - 1)
        y_pred_last = m * (window_size - 1) + c
        return y_pred_last
    except Exception as e:
        logger.error(f"Error during WLR calculation: {e}", exc_info=True)
        return None


# --- WebSocket Message Handler ---
def handle_message(message):
    """Processes incoming WebSocket messages."""
    global prices_calc, plot_x, plot_prices, plot_wlr, plot_counter

    try:
        if message.get("topic") == f"tickers.{SYMBOL}":
            if message.get("type") in ["snapshot", "delta"] and "data" in message:
                data = message["data"]
                usd_index_price_str = data.get("usdIndexPrice")

                if usd_index_price_str is not None and usd_index_price_str != '':
                    try:
                        current_price = float(usd_index_price_str)
                        # logger.info(f"Real-time {SYMBOL} usdIndexPrice: {current_price:.2f}") # Reduce logging noise

                        wlr_value = None
                        # --- Safely update shared data ---
                        with lock:
                            # Update deque for calculation
                            prices_calc.append(current_price)

                            # Calculate WLR if we have enough data points IN THE CALCULATION DEQUE
                            if len(prices_calc) == WLR_WINDOW:
                                # Pass a copy to the calculation function
                                wlr_value = calculate_wlr(deque(prices_calc), WLR_WINDOW) # Pass a copy

                            # Update deques for plotting
                            plot_counter += 1
                            plot_x.append(plot_counter)
                            plot_prices.append(current_price)
                            # Append WLR value or NaN if not calculated yet/failed
                            plot_wlr.append(wlr_value if wlr_value is not None else float('nan'))
                        # --- End lock ---

                        if wlr_value is not None:
                             logger.info(f"WLR ({WLR_WINDOW} periods): {wlr_value:.2f}")
                        # else:
                        #      logger.info(f"Collecting data... ({len(prices_calc)}/{WLR_WINDOW} points for WLR)")


                    except ValueError:
                        logger.warning(f"Could not convert usdIndexPrice '{usd_index_price_str}' to float.")
                    except Exception as e:
                         logger.error(f"Error processing price data: {e}", exc_info=True)

    except Exception as e:
        logger.error(f"Error in handle_message: {e}", exc_info=True)
        logger.error(f"Problematic message: {message}")

# --- Plotting Function ---
fig, ax = plt.subplots()
line_price, = ax.plot([], [], 'r-', label=f'{SYMBOL} usdIndexPrice')
line_wlr, = ax.plot([], [], 'b--', label=f'WLR ({WLR_WINDOW})')

def init_plot():
    """Initializes the plot elements."""
    ax.set_xlabel("Time Step")
    ax.set_ylabel("Price (USD)")
    ax.set_title(f"{SYMBOL} Real-time Price and WLR ({WLR_WINDOW}-period)")
    ax.legend()
    ax.grid(True)
    line_price.set_data([], [])
    line_wlr.set_data([], [])
    return line_price, line_wlr

def update_plot(frame):
    """Updates the plot with new data."""
    with lock: # Access shared data safely
        x_data = list(plot_x)
        y_price_data = list(plot_prices)
        y_wlr_data = list(plot_wlr)

    if not x_data: # No data yet
        return line_price, line_wlr

    # Update data for the lines
    line_price.set_data(x_data, y_price_data)
    line_wlr.set_data(x_data, y_wlr_data)

    # Adjust plot limits
    ax.relim()
    ax.autoscale_view()

    # Optional: Add dynamic Y-axis zooming around the latest price
    # if y_price_data:
    #     latest_price = y_price_data[-1]
    #     padding = (max(y_price_data) - min(y_price_data)) * 0.1 + 5 # Add some padding
    #     ax.set_ylim(latest_price - padding, latest_price + padding)

    fig.canvas.draw()
    fig.canvas.flush_events()
    return line_price, line_wlr


# --- WebSocket Connection Function ---
def start_websocket_connection():
    global ws
    logger.info(f"Connecting to Bybit {CHANNEL_TYPE} WebSocket ({'Testnet' if TESTNET else 'Mainnet'})...")
    logger.info(f"Subscribing to {SYMBOL} tickers and calculating {WLR_WINDOW}-period WLR.")

    # Initialize WebSocket WITHOUT the message_handler argument
    ws = WebSocket(
        testnet=TESTNET,
        channel_type=CHANNEL_TYPE
        # REMOVED: message_handler=handle_message
    )

    # Use the specific stream method to subscribe AND provide the callback
    ws.ticker_stream(
        symbol=SYMBOL,
        callback=handle_message
    )
    # REMOVED: ws.subscribe(["tickers." + SYMBOL])

    # Keep the WebSocket connection running in this thread
    # The pybit library handles the underlying loop and message dispatching
    # when using stream methods like ticker_stream with a callback.
    # We just need to keep this thread alive.
    while True:
        try:
            # You can add checks here if needed, e.g., check ws connection status
            # but the core message handling is done via the callback now.
            sleep(60) # Sleep for a longer time, the callback handles real-time events
        except Exception as e:
            logger.error(f"Error in WebSocket monitoring loop: {e}", exc_info=True)
            # Consider adding reconnection logic here if needed
            sleep(5)



# --- Main Execution ---
if __name__ == "__main__":
    # Start WebSocket in a separate thread
    ws_thread = threading.Thread(target=start_websocket_connection, daemon=True)
    ws_thread.start()

    # Set up plot
    plt.ion() # Turn on interactive mode
    init_plot()
    plt.show() # Display the plot window

    # Main loop for plot updates (using plt.pause)
    try:
        while True:
            update_plot(None) # Update the plot with current data
            plt.pause(0.5)  # Pause for 0.5 seconds, allows plot to refresh & prevents high CPU
            # Check if the plot window was closed
            if not plt.fignum_exists(fig.number):
                logger.info("Plot window closed by user. Exiting.")
                break
            # Optional: Check if WebSocket thread is alive
            if not ws_thread.is_alive():
                 logger.warning("WebSocket thread seems to have stopped. Exiting.")
                 break
    except KeyboardInterrupt:
        logger.info("Script interrupted by user. Exiting.")
    except Exception as e:
        logger.error(f"Error in main plotting loop: {e}", exc_info=True)
    finally:
        plt.ioff() # Turn off interactive mode
        plt.close(fig) # Ensure plot is closed
        # Note: The WebSocket connection might not be gracefully closed here
        # as the thread is a daemon. For production, more robust shutdown is needed.
        logger.info("Script finished.")
