import grpc
from concurrent import futures
from collections import deque

from routeguide import route_guide_pb2
from routeguide import route_guide_pb2_grpc

class TradePredictorServicer(route_guide_pb2_grpc.RouteGuideServicer):
    
    def __init__(self):
        # We use a 14-period RSI, which requires 15 prices to calculate 14 price changes.
        self.period = 14
        self.price_window = deque(maxlen=self.period + 1)

    def calculate_sma(self):
        """Calculates the Simple Moving Average of the current window."""
        prices = list(self.price_window)[-self.period:]
        return sum(prices) / self.period

    def calculate_rsi(self):
        """Calculates the Relative Strength Index (RSI)."""
        prices = list(self.price_window)
        gains = []
        losses = []
        
        # Calculate price changes tick-by-tick
        for i in range(1, len(prices)):
            change = prices[i] - prices[i-1]
            if change > 0:
                gains.append(change)
                losses.append(0)
            else:
                gains.append(0)
                losses.append(abs(change))
                
        avg_gain = sum(gains) / self.period
        avg_loss = sum(losses) / self.period
        
        if avg_loss == 0:
            return 100.0 # Prevent division by zero if price only goes up
            
        rs = avg_gain / avg_loss
        rsi = 100.0 - (100.0 / (1.0 + rs))
        return rsi

    def SendData(self, request_iterator, context):
        for tick in request_iterator:
            # 1. Catch the string from Go and cast it to a float
            price = float(tick.Price)
            self.price_window.append(price)
            
            # 2. Buffer phase: Do nothing until we have exactly 15 ticks of data
            if len(self.price_window) < self.period + 1:
                yield route_guide_pb2.outputData(action=3) # Send 3 (HOLD)
                continue
                
            # 3. Extract Features (Your ML Pipeline goes here)
            sma = self.calculate_sma()
            rsi = self.calculate_rsi()
            
            # 4. Basic Algorithmic Logic (Temporary ML Substitute)
            action_code = 3 # Default to HOLD
            
            if rsi < 30:
                action_code = 1 # BUY (Oversold)
                print(f"[BUY SIGNAL] RSI: {rsi:.2f} | SMA: {sma:.2f} | Price: {price}")
            elif rsi > 70:
                action_code = 2 # SELL (Overbought)
                print(f"[SELL SIGNAL] RSI: {rsi:.2f} | SMA: {sma:.2f} | Price: {price}")
            else:
                print(f"[HOLD] RSI: {rsi:.2f} | SMA: {sma:.2f} | Price: {price}")

            # 5. Fire prediction back to Go
            yield route_guide_pb2.outputData(action=action_code)

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    route_guide_pb2_grpc.add_RouteGuideServicer_to_server(TradePredictorServicer(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    print("Python AI Server running. Waiting for Go ingestion engine...")
    
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        server.stop(0)
        print("\nServer shut down successfully.")

if __name__ == '__main__':
    serve()