Import deque: Added from collections import deque.
Configuration Constants: Defined TESTNET, CHANNEL_TYPE, SYMBOL, and SMA_WINDOW at the top for easy modification.
Global prices Deque: Initialized prices = deque(maxlen=SMA_WINDOW). This deque will automatically keep only the latest SMA_WINDOW prices.
calculate_sma Function: Created a helper function to calculate the SMA, handling cases where the deque might be empty or contain non-numeric data (though the main handler tries to prevent this).
Modified handle_message:
Added global prices to modify the deque defined outside the function.
Includes checks:
message.get("topic") == f"tickers.{SYMBOL}": Ensures it's the correct topic.
message.get("type") in ["snapshot", "delta"]: Ensures it's a data update.
"data" in message: Checks if the data field exists.
data.get("usdIndexPrice"): Safely retrieves the index price string.
Checks if usd_index_price_str is not None and not empty ('').
Error Handling: Uses a try-except ValueError block to handle potential issues when converting the price string to a float.
Append to Deque: prices.append(current_price) adds the new price. deque handles removing the oldest if maxlen is reached.
Calculate & Print SMA: Calls calculate_sma only when the deque is full (len(prices) == SMA_WINDOW) and prints the result. It also prints a status message while collecting initial data.
Logging: Added basic logging for clearer output and error reporting instead of just print.
Main Block (if __name__ == "__main__":): Standard Python practice to ensure code runs only when the script is executed directly. Added basic info messages and KeyboardInterrupt handling for graceful shutdown.
