import asyncio
import logging
import websockets
from gabriel_protocol import gabriel_pb2


URI_FORMAT = 'ws://{host}:{port}'


logger = logging.getLogger(__name__)
websockets_logger = logging.getLogger(websockets.__name__)

# The entire payload will be printed if this is allowed to be DEBUG
websockets_logger.setLevel(logging.INFO)


class WebsocketClient:
    def __init__(self, host, port, producers, consumer):
        '''
        producer should take no arguments.
        consumer should take one gabriel_pb2.ResultWrapper argument.
        '''

        self._welcome_event = asyncio.Event()
        self._num_tokens = {}
        self._token_update_events = {}
        self._frame_id = 0
        self._running = True
        self._uri = URI_FORMAT.format(host=host, port=port)
        self._event_loop = asyncio.get_event_loop()
        self.producers = producers
        self.consumer = consumer

    def launch(self):

        # TODO remove this line once we stop supporting Python 3.5
        asyncio.set_event_loop(self._event_loop)

        self._websocket = self._event_loop.run_until_complete(
            await websockets.connect(self._uri))

        consumer_task = asyncio.ensure_future(self._consumer_handler())
        tasks = [
            asyncio.ensure_future(self._producer_handler(producer))
            for producer in self.producers
        ]
        tasks.append(consumer_task)

        _, pending = self._event_loop.run_until_complete(asyncio.wait(
                tasks, return_when=asyncio.FIRST_COMPLETED))
        for task in pending:
            task.cancel()
        logger.info('Server Disconnected')

    def get_frame_id(self):
        return self._frame_id

    def get_filters(self):
        return self._num_tokens.keys()

    def stop(self):
        self._running = False
        logger.info('stopping server')

    async def _consumer_handler(self):
        while self._running:
            try:
                raw_input = await self._websocket.recv()
            except websockets.exceptions.ConnectionClosed:
                return  # stop the handler
            logger.debug('Recieved input from server')

            to_client = gabriel_pb2.ToClient()
            to_client.ParseFromString(raw_input)

            if to_client.HasField('result_wrapper'):
                result_wrapper = to_client.result_wrapper
                if (result_wrapper.status == gabriel_pb2.ResultWrapper.SUCCESS):
                    self.consumer(result_wrapper)
                else:
                    logger.error('Output status was: %d', result_wrapper.status)

                if to_client.return_token:
                    filter_passed = result_wrapper.filter_passed
                    self._return_token(filter_passed)
            elif to_client.HasField('welcome_message'):
                assert not to_client.return_token
                old_token_update_events = self._token_update_events

                filter_names = to_client.welcome_message.filters_consumed
                num_tokens = to_client.welcome_message.num_tokens_per_filter
                self._num_tokens = {
                    filter_name: num_tokens for filter_name in filter_names
                }
                self._token_update_events = {
                    filter_name: asyncio.Event()
                    for filter_name in filter_names
                }

                self._welcome_event.set()
                for event in old_token_update_events.values():
                    event.set()
            else:
                raise Exception('Empty to_client message')

    async def _get_token(self, filter_name):
        await self._welcome_event.wait()

        while self._num_tokens[filter_name] < 1:
            logger.debug('Too few tokens for filter %s. Waiting.', filter_name)
            event = self._token_update_events[filter_name]
            if event.is_set():
                event.clear()
            await event.wait()

        self._num_tokens[filter_name] -= 1

    async def _send_helper(self, from_client):
        from_client.frame_id = self._frame_id
        await self._websocket.send(from_client.SerializeToString())
        self._frame_id += 1

        filter_name = from_client.filter_passed
        logger.debug('num_tokens for %s is now %d', filter_name,
                    self._num_tokens[filter_name])

    def _return_token(self, filter_name):
        self._num_tokens[filter_name] += 1
        self._token_update_events[filter_name].set()

    async def _producer_handler(self, producer):
        '''
        Loop waiting until there is a token available. Then call supplier to get
        the partially built FromClient to send.
        '''
        while self._running:
            await self._get_token(from_client.filter_passed)
            from_client = await producer()
            if from_client is None:
                self._return_token(from_client.filter_passed)
                logger.info('Received None from producer')
            else:
                try:
                    await self._send_helper(from_client)
                except websockets.exceptions.ConnectionClosed:
                    return  # stop the handler
