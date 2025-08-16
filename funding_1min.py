from pybit.unified_trading import WebSocket
from time import sleep
from datetime import datetime, timedelta
import threading

# --- Global State for Data Aggregation ---
data_lock = threading.Lock()
funding_rates_for_current_minute = []
symbol_name = "POPCATUSDT"  # Store symbol globally to access in main loop

ws = WebSocket(
    testnet=False,
    channel_type="linear",
)

def handle_message(message):
    """
    Callback to handle incoming messages. It safely appends the
    latest funding rate to a shared list.
    """
    global funding_rates_for_current_minute
    try:
        # Safely get the funding rate string from the message
        funding_rate_str = message.get('data', {}).get('fundingRate')
        if funding_rate_str:
            with data_lock:
                funding_rates_for_current_minute.append(float(funding_rate_str))

    except (KeyError, TypeError, ValueError):
        # This will catch messages that don't have the expected structure,
        # are missing data, or have non-numeric funding rates. We can ignore them.
        pass

ws.ticker_stream(
    symbol=symbol_name,
    callback=handle_message
)

print(f"Listening for {symbol_name} funding rate updates... Averaging every minute.")
print("Press Ctrl+C to exit.")

last_checked_minute = datetime.now().minute

while True:
    try:
        sleep(1)
        now = datetime.now()
        if now.minute != last_checked_minute:
            with data_lock:
                # Copy the list to process and clear the original immediately
                rates_to_process = funding_rates_for_current_minute.copy()
                funding_rates_for_current_minute.clear()

            if rates_to_process:
                average_rate = sum(rates_to_process) / len(rates_to_process)
                # Display the timestamp for the minute that just completed
                minute_timestamp = now.replace(second=0, microsecond=0) - timedelta(minutes=1)
                formatted_time = minute_timestamp.strftime('%Y-%m-%d %H:%M')
                print(f"[{formatted_time}] Symbol: {symbol_name}, Average Funding Rate: {average_rate:.8f}")

            last_checked_minute = now.minute

    except KeyboardInterrupt:
        print("\nExiting...")
        ws.exit()
        break