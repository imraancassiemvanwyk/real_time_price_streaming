from confluent_kafka import Producer
import yfinance as yf
import time
import json

producer_conf = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(producer_conf)

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def fetch_stock_data(ticker):
    stock = yf.Ticker(ticker)
    data = stock.history(period='1d')
    if not data.empty:
        return {
            'ticker': ticker,
            'price': data['Close'].iloc[-1],
            'volume': int(data['Volume'].iloc[-1]),
            'timestamp': str(data.index[-1])
        }
    return None

if __name__ == '__main__':
    ticker = 'AAPL'
    try:
        while True:
            stock_data = fetch_stock_data(ticker)
            if stock_data:
                producer.produce(
                    'stock-data', key=ticker, value=json.dumps(stock_data), callback=delivery_report
                )
                producer.flush()
                print(f"Sent data: {stock_data}")
            time.sleep(5)
    except KeyboardInterrupt:
        print("Stopping data...")