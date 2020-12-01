#!/usr/bin/env python

import asyncio
import websockets


async def run():
    # uri = "ws://localhost:8000/ws/"
    uri = "wss://irrd.as213279.net/ws/"
    async with websockets.client.connect(uri, ping_interval=1000) as websocket:
        while True:
            greeting = await websocket.recv()
            print(f"< {greeting}")

asyncio.get_event_loop().run_until_complete(run())
