import asyncio
import os
from gabriel_protocol import gabriel_pb2
from gabriel_client.server_comm import ProducerWrapper


_NUM_BYTES_FOR_SIZE = 4
_BYTEORDER = 'big'


def consumer():
    pass


class Filter:
    def __init__(self, filter_name):
        self._filter_name = filter_name
        self._frame_available = asyncio.Event()
        self._latest_item = None

        self._constructor_pid = os.getpid()
        self._read, self._write = os.pipe()
        self._started_receiver = False

    def get_producer_wrapper(self):
        async def receiver():
            loop = asyncio.get_event_loop()
            stream_reader = asyncio.StreamReader(loop=loop)
            def protocol_factory():
                return asyncio.StreamReaderProtocol(stream_reader)
            transport = await loop.connect_read_pipe(
                protocol_factory, os.fdopen(self._read, mode='r'))

            while True:
                size_bytes = await stream_reader.readexactly(
                    _NUM_BYTES_FOR_SIZE)
                size_of_message = int.from_bytes(size_bytes, _BYTEORDER)

                from_client = gabriel_pb2.FromClient()
                from_client.ParseFromString(
                    await stream_reader.readexactly(size_of_message))
                self._latest_item = from_client
                self._frame_available.set()

        async def producer():
            if not self._started_receiver:
                self._started_receiver = True
                assert os.getpid() == self._constructor_pid
                asyncio.ensure_future(receiver())

            await self._frame_available.wait()
            self._frame_available.clear()
            return self._latest_item

        return ProducerWrapper(producer=producer, filter_name=self._filter_name)

    def send(self, from_client):
        from_client.filter_passed = self._filter_name
        serialized_message = from_client.SerializeToString()

        size_of_message = len(serialized_message)
        size_bytes = size_of_message.to_bytes(_NUM_BYTES_FOR_SIZE, _BYTEORDER)

        num_bytes_written = os.write(self._write, size_bytes)
        assert num_bytes_written == _NUM_BYTES_FOR_SIZE, 'Write incomplete'

        num_bytes_written = os.write(self._write, serialized_message)
        assert num_bytes_written == size_of_message, 'Write incomplete'
