import requests
import concurrent.futures
import time

# API key and base URL for RIT
API_KEY = {'X-API-key': 'MW0YJ28H'}  # Replace 'MW0YJ28H' with your actual API key
BASE_URL = 'http://localhost:9999/v1'

# Function to get bid/ask prices for a given ticker
def get_bid_ask(ticker):
    response = requests.get(f"{BASE_URL}/securities/book?ticker={ticker}", headers=API_KEY)
    if response.ok:
        book = response.json()
        bid_price = book['bids'][0]['price'] if book['bids'] else None
        ask_price = book['asks'][0]['price'] if book['asks'] else None
        return bid_price, ask_price
    else:
        print(f"Error fetching bid/ask for {ticker}: {response.status_code}")
        return None, None

# Function to place an order
def place_order(action, ticker, quantity, order_type="MARKET", price=None):
    order_params = {
        'ticker': ticker,
        'type': order_type,
        'quantity': quantity,
        'action': action
    }
    if price:
        order_params['price'] = price

    response = requests.post(f"{BASE_URL}/orders", params=order_params, headers=API_KEY)
    if response.ok:
        print(f"{action} order placed for {quantity} shares of {ticker}")
    else:
        print(f"Error placing {action} order: {response.status_code}")

# Function to place buy and sell orders in parallel
def place_buy_sell_in_parallel(buy_params, sell_params):
    with concurrent.futures.ThreadPoolExecutor() as executor:
        buy_future = executor.submit(place_order, **buy_params)
        sell_future = executor.submit(place_order, **sell_params)

        buy_result = buy_future.result()  # Wait for buy order to complete
        sell_result = sell_future.result()  # Wait for sell order to complete

        return buy_result, sell_result

# Function to cancel unfilled limit orders
def cancel_unfilled_limit_orders():
    cancel_params = {'all': 1}
    response = requests.post(f"{BASE_URL}/commands/cancel", params=cancel_params, headers=API_KEY)
    if response.ok:
        print("Cancelled all unfilled limit orders")
    else:
        print(f"Error cancelling orders: {response.status_code}")

# Function to track positions (optional, can be expanded to track net positions)
def track_positions():
    response = requests.get(f"{BASE_URL}/securities", headers=API_KEY)
    if response.ok:
        securities = response.json()
        for sec in securities:
            if sec['ticker'] == 'CRZY':
                print(f"Current position in CRZY: {sec['position']}")
    else:
        print(f"Error fetching positions: {response.status_code}")

# Main arbitrage function
def arbitrage():
    while True:
        try:
            # Get bid/ask for both markets
            main_bid, main_ask = get_bid_ask('CRZY_M')
            alt_bid, alt_ask = get_bid_ask('CRZY_A')

            if main_bid is None or main_ask is None or alt_bid is None or alt_ask is None:
                print("Failed to retrieve bid/ask prices, retrying...")
                continue

            # Check for arbitrage opportunity (buy on one, sell on the other)
            if alt_ask < main_bid:  # Buy on Alternative, Sell on Main
                print(f"Arbitrage Opportunity: Buy CRZY_A at {alt_ask}, Sell CRZY_M at {main_bid}")
                buy_params = {'action': 'BUY', 'ticker': 'CRZY_A', 'quantity': 10000}  # Adjust quantity as needed
                sell_params = {'action': 'SELL', 'ticker': 'CRZY_M', 'quantity': 10000}  # Adjust quantity as needed
                place_buy_sell_in_parallel(buy_params, sell_params)

            elif main_ask < alt_bid:  # Buy on Main, Sell on Alternative
                print(f"Arbitrage Opportunity: Buy CRZY_M at {main_ask}, Sell CRZY_A at {alt_bid}")
                buy_params = {'action': 'BUY', 'ticker': 'CRZY_M', 'quantity': 10000}  # Adjust quantity as needed
                sell_params = {'action': 'SELL', 'ticker': 'CRZY_A', 'quantity': 10000}  # Adjust quantity as needed
                place_buy_sell_in_parallel(buy_params, sell_params)

            # Track positions to ensure we're balanced
            track_positions()

            # Cancel any unfilled limit orders if they exist
            cancel_unfilled_limit_orders()

            # Pause briefly before checking again
            time.sleep(1)

        except KeyboardInterrupt:
            print("Arbitrage algorithm stopped.")
            break

# Run the arbitrage function
if __name__ == "__main__":
    arbitrage()
