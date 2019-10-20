import cv2
import numpy as np
from gabriel_protocol import gabriel_pb2
from gabriel_client.server_comm import WebsocketClient
import logging

logger = logging.getLogger(__name__)


class OpencvClient(WebsocketClient):
    @abstractmethod
    def preprocess(self, frame):
        pass

    @abstractmethod
    def produce_engine_fields(self):
        pass

    @abstractmethod
    def consume_frame(self, frame, engine_fields):
        pass

    def __init__(self, server_ip, port, video_capture, engine_name):
        super().__init__(server_ip, port)
        self.video_capture = video_capture
        self.style_string = style_string
        self.engine_name = engine_name

    def producer(self):
        _, frame = self.video_capture.read()
        if frame is None:
            return None

        frame = self.preprocess(frame)
        _, jpeg_frame=cv2.imencode('.jpg', frame)

        from_client = gabriel_pb2.FromClient()
        from_client.payload_type = gabriel_pb2.PayloadType.IMAGE
        from_client.engine_name = self.engine_name
        from_client.payload = jpeg_frame.tostring()

        engine_fields = self.produce_engine_fields()
        if engine_fields is not None:
            from_client.engine_fields.Pack(engine_fields)

        return from_client

    def consumer(self, result_wrapper):
        if len(result_wrapper.results) == 1:
            result = result_wrapper.results[0]
            if result.payload_type == gabriel_pb2.PayloadType.IMAGE:
                if result.engine_name == self.engine_name:
                    np_data = np.fromstring(result.payload, dtype=np.uint8)
                    frame = cv2.imdecode(np_data, cv2.IMREAD_COLOR)

                    self.consume_frame(frame, result_wrapper.engine_fields)
                else:
                    logger.error('Got result from engine %s',
                                 result.engine_name)
            else:
                logger.error('Got result of type %s', result.payload_type.name)
        else:
            logger.error('Got %d results in output',
                         len(result_wrapper.results))
