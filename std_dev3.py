from pybit.unified_trading import WebSocket
from time import sleep
from collections import deque
import logging
import matplotlib.pyplot as plt
import matplotlib.animation as animation
import threading
import numpy as np

# --- Configuration ---
TESTNET = False
CHANNEL_TYPE = "spot"
SYMBOL = "BTCUSDT"
WLR_WINDOW = 999      # Window size for Weighted Linear Regression
PLOT_HISTORY = 1250   # How many points to show on the plot
STD_DEV_AWAY_THRESHOLD = 2.35 # Configurable threshold for Std Devs Away flag

# --- Global State ---
prices_calc = deque(maxlen=WLR_WINDOW)
plot_x = deque(maxlen=PLOT_HISTORY)
plot_prices = deque(maxlen=PLOT_HISTORY)
plot_wlr = deque(maxlen=PLOT_HISTORY)

# CHANGE 1: Remove maxlen from flag deques
plot_std_dev_flags_x = deque() # For storing x-coordinates of flags
plot_std_dev_flags_y = deque() # For storing y-coordinates of flags

plot_counter = 0

ws = None
lock = threading.Lock()

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
    global prices_calc, plot_x, plot_prices, plot_wlr, plot_counter, plot_std_dev_flags_x, plot_std_dev_flags_y

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

                            if len(prices_calc) == WLR_WINDOW:
                                wlr_value = calculate_wlr(deque(prices_calc), WLR_WINDOW)

                            plot_counter += 1
                            plot_x.append(plot_counter)
                            plot_prices.append(current_price)
                            plot_wlr.append(wlr_value if wlr_value is not None else float('nan'))

                            if wlr_value is not None:
                                price_array = np.array(list(prices_calc))
                                std_dev = np.std(price_array)

                                price_difference = current_price - wlr_value

                                if std_dev != 0:
                                    std_devs_away = price_difference / std_dev
                                    # logger.info(f"Current Price: {current_price:.2f}, WLR ({WLR_WINDOW}): {wlr_value:.2f}, Std Dev: {std_dev:.2f}, Price Difference: {price_difference:.2f}, Std Devs Away: {std_devs_away:.2f}")

                                    if abs(std_devs_away) >= STD_DEV_AWAY_THRESHOLD:
                                        plot_std_dev_flags_x.append(plot_counter)
                                        plot_std_dev_flags_y.append(current_price)
                                        logger.warning(f"!!! Std Devs Away ({std_devs_away:.2f}) >= Configured Threshold ({STD_DEV_AWAY_THRESHOLD:.2f}) at price {current_price:.2f} !!!")
                                else:
                                    logger.warning("Standard deviation is zero, cannot calculate standard deviations away from WLR.")
                                    # logger.info(f"Current Price: {current_price:.2f}, WLR ({WLR_WINDOW}): {wlr_value:.2f}, Std Dev: {std_dev:.2f}, Price Difference: {price_difference:.2f}")

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
scatter_std_dev_flags, = ax.plot([], [], 'go', markersize=8, label=f'Std Devs Away >= $\\pm${STD_DEV_AWAY_THRESHOLD}', alpha=0.7)

def init_plot():
    """Initializes the plot elements."""
    ax.set_xlabel("Time Step")
    ax.set_ylabel("Price (USD)")
    ax.set_title(f"{SYMBOL} Real-time Price and WLR ({WLR_WINDOW}-period)")
    ax.legend()
    ax.grid(True)
    line_price.set_data([], [])
    line_wlr.set_data([], [])
    scatter_std_dev_flags.set_data([], [])
    return line_price, line_wlr, scatter_std_dev_flags

def update_plot(frame):
    """Updates the plot with new data."""
    with lock:
        x_data = list(plot_x)
        y_price_data = list(plot_prices)
        y_wlr_data = list(plot_wlr)
        all_x_flags_data = list(plot_std_dev_flags_x) # Get ALL flag x-data
        all_y_flags_data = list(plot_std_dev_flags_y) # Get ALL flag y-data

    if not x_data:
        return line_price, line_wlr, scatter_std_dev_flags

    # Determine the visible x-range
    min_x_visible = x_data[0]
    max_x_visible = x_data[-1]

    # CHANGE 2: Filter flags to only show within the visible x-range
    filtered_x_flags = []
    filtered_y_flags = []
    for i in range(len(all_x_flags_data)):
        if min_x_visible <= all_x_flags_data[i] <= max_x_visible:
            filtered_x_flags.append(all_x_flags_data[i])
            filtered_y_flags.append(all_y_flags_data[i])

    # Update data for the lines
    line_price.set_data(x_data, y_price_data)
    line_wlr.set_data(x_data, y_wlr_data)
    scatter_std_dev_flags.set_data(filtered_x_flags, filtered_y_flags) # Use filtered data

    # Adjust plot limits
    ax.relim()
    ax.autoscale_view()

    fig.canvas.draw()
    fig.canvas.flush_events()
    return line_price, line_wlr, scatter_std_dev_flags

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
    ws_thread = threading.Thread(target=start_websocket_connection, daemon=True)
    ws_thread.start()

    plt.ion()
    init_plot()
    plt.show()

    try:
        while True:
            update_plot(None)
            plt.pause(0.5)
            if not plt.fignum_exists(fig.number):
                logger.info("Plot window closed by user. Exiting.")
                break
            if not ws_thread.is_alive():
                logger.warning("WebSocket thread seems to have stopped. Exiting.")
                break
    except KeyboardInterrupt:
        logger.info("Script interrupted by user. Exiting.")
    except Exception as e:
        logger.error(f"Error in main plotting loop: {e}", exc_info=True)
    finally:
        plt.ioff()
        plt.close(fig)
        logger.info("Script finished.")