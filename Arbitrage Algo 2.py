import requests
import concurrent.futures
import time
import asyncio
from collections import deque

# API key and base URL for RIT
API_KEY = {'X-API-key': 'MW0YJ28H'}
BASE_URL = 'http://localhost:9999/v1'

# Configurable parameters for strategy
MAX_ORDER_SIZE = 10000  # Maximum order size allowed
SPREAD_THRESHOLD = 0.01  # Threshold to switch between market and limit orders
CANCEL_TIMEOUT = 2  # Time in seconds before canceling unfilled limit orders
MIN_LIQUIDITY = 5000  # Minimum liquidity required to place an order
MAX_RETRY = 3  # Maximum retries for failed API calls
ORDER_FLOW_HISTORY_SIZE = 200  # How many trades to consider in historical order flow analysis
PREDICTIVE_THRESHOLD = 1.2  # Threshold for predicting arbitrage based on buy/sell imbalance

# Global market data for order flow
market_data = {
    'main_market': {'buy_volume': 0, 'sell_volume': 0, 'pressure': '', 'bullish': 0, 'bearish': 0},
    'alt_market': {'buy_volume': 0, 'sell_volume': 0, 'pressure': '', 'bullish': 0, 'bearish': 0},
    'trades': []
}

# Store recent order flow to analyze trends
order_flow_history = {
    'main_market': deque(maxlen=ORDER_FLOW_HISTORY_SIZE),
    'alt_market': deque(maxlen=ORDER_FLOW_HISTORY_SIZE)
}

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

# Function to track positions
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

# Function to fetch time and sales data
def fetch_time_and_sales(ticker, limit=100):
    params = {'limit': limit}
    response = requests.get(f"{BASE_URL}/securities/tas?ticker={ticker}", headers=API_KEY, params=params)
    if response.ok:
        return response.json()
    return []

# Function to analyze and store order flow from time and sales data
def analyze_order_flow(ticker):
    trades = fetch_time_and_sales(ticker)
    
    if ticker == 'CRZY_M':
        market_key = 'main_market'
    else:
        market_key = 'alt_market'
    
    # Reset buy/sell volumes before processing trades
    market_data[market_key]['buy_volume'] = 0
    market_data[market_key]['sell_volume'] = 0

    for trade in trades:
        # Match trade price to bid or ask
        bid_price, ask_price = get_bid_ask(ticker)[:2]
        if trade['price'] == ask_price:
            market_data[market_key]['buy_volume'] += trade['quantity']
        elif trade['price'] == bid_price:
            market_data[market_key]['sell_volume'] += trade['quantity']

    # Calculate buy/sell pressure based on recent trades
    buy_volume = market_data[market_key]['buy_volume']
    sell_volume = market_data[market_key]['sell_volume']
    
    if buy_volume > sell_volume * PREDICTIVE_THRESHOLD:
        market_data[market_key]['pressure'] = 'Buy Pressure'
    elif sell_volume > buy_volume * PREDICTIVE_THRESHOLD:
        market_data[market_key]['pressure'] = 'Sell Pressure'
    else:
        market_data[market_key]['pressure'] = 'Neutral'
    
    # Store in historical order flow for trend analysis
    order_flow_history[market_key].append({
        'buy_volume': buy_volume,
        'sell_volume': sell_volume,
        'pressure': market_data[market_key]['pressure']
    })

# Predict arbitrage opportunities based on order flow analysis
def predict_arbitrage_opportunities():
    main_history = order_flow_history['main_market']
    alt_history = order_flow_history['alt_market']
    
    # Analyze recent order flow trends (bullish/bearish imbalances)
    main_buy_pressure = sum(1 for item in main_history if item['pressure'] == 'Buy Pressure')
    alt_sell_pressure = sum(1 for item in alt_history if item['pressure'] == 'Sell Pressure')
    
    # If main has buy pressure and alt has sell pressure, predict arbitrage
    if main_buy_pressure > len(main_history) * 0.7 and alt_sell_pressure > len(alt_history) * 0.7:
        print("Predicting arbitrage opportunity: Buy on Main, Sell on Alt")
        return "BUY_MAIN_SELL_ALT"
    
    alt_buy_pressure = sum(1 for item in alt_history if item['pressure'] == 'Buy Pressure')
    main_sell_pressure = sum(1 for item in main_history if item['pressure'] == 'Sell Pressure')

    # If alt has buy pressure and main has sell pressure, predict arbitrage
    if alt_buy_pressure > len(alt_history) * 0.7 and main_sell_pressure > len(main_history) * 0.7:
        print("Predicting arbitrage opportunity: Buy on Alt, Sell on Main")
        return "SELL_MAIN_BUY_ALT"
    
    return None

# Main arbitrage function with integrated order flow and predictive model
async def arbitrage():
    retry_count = 0
    while True:
        try:
            # Fetch and analyze order flow data for both markets
            analyze_order_flow('CRZY_M')
            analyze_order_flow('CRZY_A')

            # Predict arbitrage opportunities based on order flow
            predicted_action = predict_arbitrage_opportunities()

            # Get bid/ask prices for both markets
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

            # Execute predicted arbitrage action if available
            if predicted_action == "BUY_MAIN_SELL_ALT":
                if main_ask < alt_bid:  # Buy on Main, Sell on Alternative
                    print(f"Arbitrage Opportunity: Buy CRZY_M at {main_ask}, Sell CRZY_A at {alt_bid}")
                    buy_params = {'action': 'BUY', 'ticker': 'CRZY_M', 'quantity': min(MAX_ORDER_SIZE, main_ask_size)}
                    sell_params = {'action': 'SELL', 'ticker': 'CRZY_A', 'quantity': min(MAX_ORDER_SIZE, alt_bid_size)}
                    place_buy_sell_in_parallel(buy_params, sell_params)

            elif predicted_action == "SELL_MAIN_BUY_ALT":
                if alt_ask < main_bid:  # Buy on Alternative, Sell on Main
                    print(f"Arbitrage Opportunity: Buy CRZY_A at {alt_ask}, Sell CRZY_M at {main_bid}")
                    buy_params = {'action': 'BUY', 'ticker': 'CRZY_A', 'quantity': min(MAX_ORDER_SIZE, alt_ask_size)}
                    sell_params = {'action': 'SELL', 'ticker': 'CRZY_M', 'quantity': min(MAX_ORDER_SIZE, main_bid_size)}
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
