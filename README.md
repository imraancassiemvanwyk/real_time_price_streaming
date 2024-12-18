# real_time_price_streaming
in main.py:

Calls are made to yfinance api to get stock prices then Kafka consumer is made to broadcast the stock prices 

in main2.py:

kafka producer gets the broadcasted prices, the moving average is calculated and put in mongoDB

Docker-compose:

Setting up Kafka and mongoDB
