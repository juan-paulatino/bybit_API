import json
import time
import threading
from collections import deque
from pybit.unified_trading import WebSocket
from websocket import WebSocketConnectionClosedException

class BybitFundingCalculator:
    """
    A class to connect to the Bybit WebSocket, collect real-time market data,
    and calculate a real-time funding rate based on a Time-Weighted Average Price (TWAP).
    """

    def __init__(self, symbol, testnet=True):
        """
        Initializes the calculator with a symbol, a WebSocket connection,
        and a data queue for TWAP calculation.
        """
        self.symbol = symbol
        self.running = True
        self.data_lock = threading.Lock()
        self.data_points = deque()
        self.ws = WebSocket(testnet=testnet, channel_type="linear")
        self.calculation_thread = threading.Thread(target=self._calculate_funding_rate, daemon=True)
        self.data_window_seconds = 3600  # 1-hour TWAP window

    def _handle_message(self, message):
        """
        This is the callback function for the WebSocket. It parses incoming
        messages and adds the latest price data to the data queue.
        """
        try:
            msg = json.loads(message)
            if 'data' in msg and isinstance(msg['data'], dict):
                mark_price = float(msg['data'].get('markPrice', '0'))
                index_price = float(msg['data'].get('indexPrice', '0'))
                timestamp = time.time()
                
                if mark_price > 0 and index_price > 0:
                    with self.data_lock:
                        self.data_points.append((timestamp, mark_price, index_price))
                        
                        while self.data_points and self.data_points[0][0] < timestamp - self.data_window_seconds:
                            self.data_points.popleft()
        
        except (json.JSONDecodeError, KeyError, ValueError) as e:
            print(f"Error processing message: {e}")

    def _calculate_funding_rate(self):
        """
        This function runs in a separate thread. It periodically calculates the
        TWAP for mark price and index price, then computes the funding rate.
        """
        print("Starting funding rate calculation thread...")
        while self.running:
            with self.data_lock:
                if len(self.data_points) < 2:
                    time.sleep(1)
                    continue
            
            mark_price_twap = 0
            index_price_twap = 0
            total_time = 0
            
            with self.data_lock:
                for i in range(len(self.data_points) - 1):
                    t1, mp1, ip1 = self.data_points[i]
                    t2, mp2, ip2 = self.data_points[i+1]
                    
                    time_diff = t2 - t1
                    avg_mp = (mp1 + mp2) / 2
                    avg_ip = (ip1 + ip2) / 2
                    
                    mark_price_twap += avg_mp * time_diff
                    index_price_twap += avg_ip * time_diff
                    total_time += time_diff
            
            if total_time > 0:
                mark_price_twap /= total_time
                index_price_twap /= total_time
                
                funding_rate_raw = (mark_price_twap - index_price_twap) / index_price_twap
                
                annualized_funding_rate = funding_rate_raw * (365 * 24 / (self.data_window_seconds / 3600))
                
                print(f"[{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}] - "
                      f"Mark Price TWAP: {mark_price_twap:.2f} | "
                      f"Index Price TWAP: {index_price_twap:.2f} | "
                      f"Real-Time Funding Rate: {funding_rate_raw:.8f} | "
                      f"Annualized Rate: {annualized_funding_rate:.4f}%")
            
            time.sleep(15)

    def run(self):
        """
        Starts the WebSocket connection and the calculation thread.
        """
        try:
            self.ws.ticker_stream(self.symbol, callback=self._handle_message)
            print(f"Connected to WebSocket. Subscribed to {self.symbol} ticker stream.")
            self.calculation_thread.start()
            print("Waiting for data. Calculation will start soon...")

            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            self.stop()
        except WebSocketConnectionClosedException:
            print("WebSocket connection was unexpectedly closed.")
            self.stop()
        finally:
            self.stop()

    def stop(self):
        """
        Gracefully shuts down all threads and the WebSocket connection.
        This version includes a delay to prevent a race condition during shutdown.
        """
        if self.running:
            print("Script terminated by user. Initiating graceful shutdown.")
            self.running = False
            if self.calculation_thread.is_alive():
                # Give the calculation thread a chance to finish its last loop
                self.calculation_thread.join(timeout=1)
            
            print("Allowing time for internal library threads to shut down...")
            time.sleep(2) # Wait a bit to let the internal ping thread terminate
            
            self.ws.exit()
            print("WebSocket connection closed.")

# --- Main Script Execution ---
if __name__ == "__main__":
    calculator = BybitFundingCalculator(symbol="POPCATPERP", testnet=True)
    calculator.run()