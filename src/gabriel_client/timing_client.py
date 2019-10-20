import time
import logging

logger = logging.getLogger(__name__)


def factory(super_class):
    class TimingClient(super_class):
        def __init__(self, host, port, output_freq=10):
            super().__init__(host, port)
            self._output_freq = output_freq
            self._send_timestamps = {}
            self._recv_timestamps = {}

            self._count = 0
            self._interval_count = 0
            self._start_time = time.time()
            self._interval_start_time = time.time()

        async def _send_helper(self, from_client):
            self._send_timestamps[self._frame_id] = time.time()
            await super()._send_helper(from_client)

        def consumer(self, result_wrapper):
            await super().consumer(result_wrapper)

            timestamp = time.time()
            self._recv_timestamps[reasult_wrapper.frame_id] = timestamp
            self._count += 1
            self._interval_count += 1

            if self._count % self._output_freq == 0:
                overall_fps = self._count / (timestamp - self._start_time)
                print('Overall FPS:', overall_fps)
                interval_fps = (self._interval_count /
                                (timestamp - self._interval_start_time))
                print('Interval FPS:', interval_fps)

                self._interval_count = 0
                self._interval_start_time = time.time()

        def compute_avg_rtt(self):
            count = 0
            total_rtt = 0

            for frame_id, sent in self._send_timestamps:
                received = self._recv_timestamps.get(frame_id)
                if received is None:
                    logger.error('Frame with ID %d never received')
                else:
                    count += 1
                    total_rtt += (received - sent)

            print('Average RTT', total_rtt / count)

        def clear_timestamps(self):
            self._send_timestamps.clear()
            self._recv_timestamps.clear()
