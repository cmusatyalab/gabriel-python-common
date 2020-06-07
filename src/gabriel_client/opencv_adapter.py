import cv2
import numpy as np
from gabriel_protocol import gabriel_pb2
from gabriel_client.server_comm import WebsocketClient
import logging

logger = logging.getLogger(__name__)


class OpencvAdapter:
    def __init__(self, preprocess, produce_extras, consume_frame,
                 video_capture, filter_passed):
        '''
        preprocess should take a one frame parameter
        produce_engine_fields should take no parameters
        consume_frame should take one frame parameter and one engine_fields
        parameter
        '''

        self._preprocess = preprocess
        self._produce_extras = produce_extras
        self._consume_frame = consume_frame
        self._video_capture = video_capture
        self._filter_passed = filter_passed

    def producer(self):
        _, frame = self._video_capture.read()
        if frame is None:
            return None

        frame = self._preprocess(frame)
        _, jpeg_frame=cv2.imencode('.jpg', frame)

        from_client = gabriel_pb2.FromClient()
        from_client.payload_type = gabriel_pb2.PayloadType.IMAGE
        from_client.filter_passed = self._filter_passed
        from_client.payloads_for_frame.append(jpeg_frame.tostring())

        extras = self._produce_extras()
        if extras is not None:
            from_client.extras.Pack(extras)

        return from_client

    def consumer(self, result_wrapper):
        if result_wrapper.filter_passed != self._filter_passed:
            logger.error('Got result that passed filter %s',
                         result_wrapper.filter_passed)
            return

        if len(result_wrapper.results) != 1:
            logger.error('Got %d results in output',
                         len(result_wrapper.results))
            return

        result = result_wrapper.results[0]
        if result.payload_type != gabriel_pb2.PayloadType.IMAGE:
            logger.error('Got result of type %s', result.payload_type.name)
            return

        np_data = np.fromstring(result.payload, dtype=np.uint8)
        frame = cv2.imdecode(np_data, cv2.IMREAD_COLOR)

        self._consume_frame(frame, result_wrapper.extras)
