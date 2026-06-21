import grpc
from concurrent import futures

from routeguide import route_guide_pb2
from routeguide import route_guide_pb2_grpc

class TradePredictorServicer(route_guide_pb2_grpc.RouteGuideServicer):
    
    def SendData(self, request_iterator, context):
        """
        Because this is a Bidirectional Stream, request_iterator acts like an infinite loop.
        It catches incoming ticks from Go as fast as they arrive.
        """
        for tick in request_iterator:
            print(f"Received Tick -> ID: {tick.ID}, Price: {tick.Price}, Qty: {tick.Quantity}")
            
            yield route_guide_pb2.outputData(action=1)

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    
    route_guide_pb2_grpc.add_RouteGuideServicer_to_server(TradePredictorServicer(), server)
    
    server.add_insecure_port('[::]:50051')
    server.start()
    print("Python gRPC Machine Learning Server running on port 50051...")
    
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        server.stop(0)
        print("\nServer shut down successfully.")

if __name__ == '__main__':
    serve()