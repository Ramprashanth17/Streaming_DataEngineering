import os
import json
import websocket
from kafka import KafkaProducer

API_KEY = os.environ["FINNHUB_API_KEY"]
TOPIC = "market.prices.raw"

producer = KafkaProducer(
    bootstrap_servers="localhost:9094",
    key_serializer=lambda k: k.encode("utf-8"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

def on_message(ws, message):
    data = json.loads(message)
    if data.get("type") == "trade":
        for trade in data["data"]:
            event = {"symbol": trade["s"], "price": trade["p"], "size": trade["v"], "timestamp": trade["t"]}
            producer.send(TOPIC, key=trade["s"], value=event)
            producer.flush()
            print(f"Produced: {event}")

def on_open(ws):
    print("WebSocket connection opened")
    for ticker in ["AAPL", "MSFT", "NVDA"]:
        ws.send(json.dumps({"type": "subscribe", "symbol": ticker}))
        print(f"Subscribed to {ticker}")
    ws.send(json.dumps({"type": "subscribe", "symbol": "BINANCE:BTCUSDT"}))
    print("Subscribed to BINANCE:BTCUSDT")

def on_error(ws, error):
    print(f"WebSocket error: {error}")

def on_close(ws, close_status_code, close_msg):
    print(f"WebSocket closed: {close_status_code} {close_msg}")

ws = websocket.WebSocketApp(
    f"wss://ws.finnhub.io?token={API_KEY}",
    on_message=on_message,
    on_open=on_open,
    on_error=on_error,
    on_close=on_close,
)
ws.run_forever()