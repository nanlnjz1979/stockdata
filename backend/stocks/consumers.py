from channels.generic.websocket import AsyncJsonWebsocketConsumer
import asyncio

class QuotesConsumer(AsyncJsonWebsocketConsumer):
    async def connect(self):
        await self.accept()
        # 占位：周期性推送模拟行情
        self.send_task = asyncio.create_task(self.push_mock())

    async def disconnect(self, close_code):
        if hasattr(self, 'send_task'):
            self.send_task.cancel()

    async def push_mock(self):
        i = 0
        while True:
            await self.send_json({
                'code': 'TEST',
                'timestamp': f'2024-01-01 09:{30+i:02d}',
                'price': 10.0 + (i % 5) * 0.1,
            })
            i += 1
            await asyncio.sleep(1)