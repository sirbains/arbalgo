import requests
import concurrent.futures
import time
import asyncio

# API key and base URL for RIT
API_KEY = {'X-API-key': 'MW0YJ28H'}  # Replace 'MW0YJ28H' with your actual API key
BASE_URL = 'http://localhost:9999/v1'

# Configurable parameters for strategy
MAX_ORDER_SIZE = 10000  # Maximum order size allowed
SPREAD_THRESHOLD = 0.01  # Threshold to switch between market and limit orders
CANCEL_TIMEOUT = 2  # Time in seconds before canceling unfilled limit orders
MIN_LIQUIDITY = 5000  # Minimum liquidity required to place an order
MAX_RETRY = 3  # Maximum retries for failed API calls

# Function to get bid/ask prices and depth for a given ticker
def get_bid_ask(ticker):
    try:
        response = requests.get(f"{BASE_URL}/securities/book?ticker={ticker}", headers=API_KEY)
        if response.ok:
            book = response.json()
            bid_price = book['bids'][0]['price'] if book['bids'] else None
            ask_price = book['asks'][0]['price'] if book['asks'] else None
            bid_size = book['bids'][0]['quantity'] if book['bids'] else 0
            ask_size = book['asks'][0]['quantity'] if book['asks'] else 0
            return bid_price, ask_price, bid_size, ask_size
        else:
            print(f"Error fetching bid/ask for {ticker}: {response.status_code}")
            return None, None, 0, 0
    except Exception as e:
        print(f"Exception in fetching bid/ask: {str(e)}")
        return None, None, 0, 0

# Function to place an order
def place_order(action, ticker, quantity, order_type="MARKET", price=None):
    try:
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
            return True
        else:
            print(f"Error placing {action} order: {response.status_code}")
            return False
    except Exception as e:
        print(f"Exception in placing order: {str(e)}")
        return False

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

# Function to track positions (expanded to track net positions)
def track_positions():
    try:
        response = requests.get(f"{BASE_URL}/securities", headers=API_KEY)
        if response.ok:
            securities = response.json()
            for sec in securities:
                if sec['ticker'] == 'CRZY':
                    print(f"Current position in CRZY: {sec['position']}")
                    return sec['position']
        else:
            print(f"Error fetching positions: {response.status_code}")
            return 0
    except Exception as e:
        print(f"Exception in tracking positions: {str(e)}")
        return 0

# Function to place orders with checks for liquidity and spread
def place_order_with_liquidity_check(action, ticker, quantity, price, available_liquidity):
    order_type = "LIMIT" if available_liquidity >= MIN_LIQUIDITY and price else "MARKET"
    if available_liquidity >= quantity:
        return place_order(action, ticker, quantity, order_type, price if order_type == "LIMIT" else None)
    else:
        print(f"Insufficient liquidity for {action} {quantity} shares of {ticker}")
        return False

# Main arbitrage function with enhanced logic
async def arbitrage():
    retry_count = 0
    while True:
        try:
            # Get bid/ask for both markets
            main_bid, main_ask, main_bid_size, main_ask_size = get_bid_ask('CRZY_M')
            alt_bid, alt_ask, alt_bid_size, alt_ask_size = get_bid_ask('CRZY_A')

            if main_bid is None or main_ask is None or alt_bid is None or alt_ask is None:
                print("Failed to retrieve bid/ask prices, retrying...")
                if retry_count < MAX_RETRY:
                    retry_count += 1
                    continue
                else:
                    print("Max retries reached, exiting.")
                    break

            # Check for arbitrage opportunity (buy on one, sell on the other)
            if alt_ask < main_bid:  # Buy on Alternative, Sell on Main
                spread = main_bid - alt_ask
                print(f"Arbitrage Opportunity: Buy CRZY_A at {alt_ask}, Sell CRZY_M at {main_bid}, Spread: {spread}")
                
                # Check spread and liquidity to decide limit or market orders
                buy_params = {
                    'action': 'BUY', 'ticker': 'CRZY_A', 
                    'quantity': min(MAX_ORDER_SIZE, alt_ask_size), 
                    'price': alt_ask if spread >= SPREAD_THRESHOLD else None
                }
                sell_params = {
                    'action': 'SELL', 'ticker': 'CRZY_M', 
                    'quantity': min(MAX_ORDER_SIZE, main_bid_size), 
                    'price': main_bid if spread >= SPREAD_THRESHOLD else None
                }

                place_buy_sell_in_parallel(buy_params, sell_params)

            elif main_ask < alt_bid:  # Buy on Main, Sell on Alternative
                spread = alt_bid - main_ask
                print(f"Arbitrage Opportunity: Buy CRZY_M at {main_ask}, Sell CRZY_A at {alt_bid}, Spread: {spread}")

                buy_params = {
                    'action': 'BUY', 'ticker': 'CRZY_M', 
                    'quantity': min(MAX_ORDER_SIZE, main_ask_size), 
                    'price': main_ask if spread >= SPREAD_THRESHOLD else None
                }
                sell_params = {
                    'action': 'SELL', 'ticker': 'CRZY_A', 
                    'quantity': min(MAX_ORDER_SIZE, alt_bid_size), 
                    'price': alt_bid if spread >= SPREAD_THRESHOLD else None
                }

                place_buy_sell_in_parallel(buy_params, sell_params)

            # Track positions to ensure we're balanced
            current_position = track_positions()

            # Cancel any unfilled limit orders if they exist after a timeout
            await asyncio.sleep(CANCEL_TIMEOUT)
            cancel_unfilled_limit_orders()

            # Pause briefly before checking again
            await asyncio.sleep(1)

        except KeyboardInterrupt:
            print("Arbitrage algorithm stopped.")
            break
        except Exception as e:
            print(f"Error in arbitrage execution: {str(e)}")
            continue

# Run the arbitrage function asynchronously
if __name__ == "__main__":
    try:
        asyncio.run(arbitrage())
    except Exception as e:
        print(f"Error running the arbitrage algorithm: {str(e)}")
