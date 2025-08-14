import json
import time
import threading
from collections import deque
from pybit.unified_trading import WebSocket
from websocket import WebSocketConnectionClosedException # Import the specific exception

# --- Configuration and Global State ---
# This lock is used to prevent race conditions when the WebSocket thread
# and the calculation thread access the shared data queue.
data_lock = threading.Lock()

# A deque (double-ended queue) is used to store a rolling window of data.
# This is an efficient way to manage data points over a specific time period.
# Each item will be a tuple: (timestamp, mark_price, index_price).
# We will use a 1-hour (3600 seconds) window for this example TWAP calculation.
data_window_seconds = 3600
data_points = deque()

# --- WebSocket Message Handler ---
def handle_message(message):
    """
    This function is called every time the WebSocket receives a new message.
    It parses the message, extracts the necessary prices, and adds them to
    our data queue.
    """
    try:
        # Parse the JSON message from the WebSocket
        msg = json.loads(message)
        
        # We are only interested in 'snapshot' or 'delta' updates for the ticker
        if 'data' in msg and isinstance(msg['data'], dict):
            symbol = msg['data']['symbol']
            
            # Extract the real-time prices
            mark_price = float(msg['data'].get('markPrice', '0'))
            index_price = float(msg['data'].get('indexPrice', '0'))
            timestamp = time.time()  # Use current system time for a simple TWAP
            
            if mark_price > 0 and index_price > 0:
                with data_lock:
                    # Add the new data point to the deque
                    data_points.append((timestamp, mark_price, index_price))
                    
                    # Remove old data points to maintain the rolling time window.
                    # We check the timestamp to ensure we are only keeping data
                    # within the last `data_window_seconds`.
                    while data_points and data_points[0][0] < timestamp - data_window_seconds:
                        data_points.popleft()
            
    except (json.JSONDecodeError, KeyError, ValueError) as e:
        print(f"Error processing message: {e}")
        
# --- Funding Rate Calculation Logic ---
def calculate_funding_rate():
    """
    This function runs in a separate thread. It periodically calculates the
    Time-Weighted Average Price (TWAP) for both the mark price and index price,
    then uses those to determine a real-time funding rate.
    """
    print("Starting funding rate calculation thread...")
    while True:
        # Wait for the data queue to fill up to ensure we have enough data
        # for a meaningful TWAP calculation.
        with data_lock:
            if len(data_points) < 2:
                time.sleep(1)
                continue
        
        # Calculate TWAPs
        mark_price_twap = 0
        index_price_twap = 0
        total_time = 0
        
        with data_lock:
            current_time = time.time()
            # Iterate through the data points to calculate TWAP
            for i in range(len(data_points) - 1):
                t1, mp1, ip1 = data_points[i]
                t2, mp2, ip2 = data_points[i+1]
                
                # Time difference between this point and the next
                time_diff = t2 - t1
                
                # Average price during this time interval
                avg_mp = (mp1 + mp2) / 2
                avg_ip = (ip1 + ip2) / 2
                
                # Add to total TWAP calculation
                mark_price_twap += avg_mp * time_diff
                index_price_twap += avg_ip * time_diff
                total_time += time_diff
        
        # Calculate the final TWAP if there's enough data
        if total_time > 0:
            mark_price_twap /= total_time
            index_price_twap /= total_time
            
            # --- The Core Funding Rate Formula ---
            # This is a simplified version of the formula. Exchanges might have
            # additional components like an interest rate component.
            # funding_rate = (mark_price_twap - index_price_twap) / index_price_twap
            
            # The more common formula for the funding rate is to take the
            # TWAP of the price difference and normalize it.
            funding_rate_raw = (mark_price_twap - index_price_twap) / index_price_twap
            
            # To get an annualized rate (like on Coinglass)
            annualized_funding_rate = funding_rate_raw * (365 * 24 / (data_window_seconds / 3600))
            
            print(f"[{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}] - "
                  f"Mark Price TWAP: {mark_price_twap:.2f} | "
                  f"Index Price TWAP: {index_price_twap:.2f} | "
                  f"Real-Time Funding Rate: {funding_rate_raw:.8f} | "
                  f"Annualized Rate: {annualized_funding_rate:.4f}%")
        
        # Wait for a bit before the next calculation to avoid excessive CPU usage
        time.sleep(15)  # Calculate every 15 seconds

# --- Main Script Execution ---
if __name__ == "__main__":
    try:
        # Connect to the testnet WebSocket
        ws = WebSocket(
            testnet=True,
            channel_type="linear",
        )
        
        # Subscribe to the ticker stream for POPCATPERP.
        # This will send real-time price updates.
        ws.ticker_stream(
            symbol="POPCATPERP",
            callback=handle_message
        )
        
        print("Connected to WebSocket. Subscribed to POPCATPERP ticker stream.")
        
        # Start the funding rate calculation in a separate thread
        calculation_thread = threading.Thread(target=calculate_funding_rate)
        calculation_thread.daemon = True  # Ensure the thread exits when the main program does
        calculation_thread.start()
        
        print("Waiting for data. Calculation will start soon...")

        # Keep the main thread alive to listen for WebSocket messages
        while True:
            time.sleep(1)

    except KeyboardInterrupt:
        print("Script terminated by user.")
    finally:
        # Gracefully handle the WebSocket close.
        # The WebSocketConnectionClosedException can occur here if the internal
        # ping thread tries to send a message after the connection is closed.
        print("Closing WebSocket connection.")
        try:
            ws.exit()
        except WebSocketConnectionClosedException:
            print("WebSocket was already closed, proceeding with exit.")
