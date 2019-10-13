import websockets
import asyncio
import json
import async_timeout

API_ENDPOINT = 'wss://alpha.thundervigil.com/ws'
API_KEY = ''  # signup on thundervigil.com/signup to get API key


async def consumer_contract():
    async with websockets.connect(API_ENDPOINT) as ws:
        await ws.send(json.dumps({'command': 'register', 'key': API_KEY}))
        ack = await ws.recv()
        print(ack)
        ack = json.loads(ack)
        try:
            ack_cmd = ack['command']
        except KeyError:
            print('Bad response')
            await ws.close()
            return
        else:
            if ack_cmd != 'register:ack':
                print('Registration not acknowledged')
                await ws.close()
                return
        sessionID = ack['sessionID']
        # async for msg in ws:
        #    print(msg)
        count = 0
        while True:
            if count > 1000:
                await ws.send(json.dumps({'command': 'unregister', 'key': API_KEY}))
                ack = await ws.recv()
                print(ack)
                await ws.close()
                break
            try:
                async with async_timeout.timeout(10):
                    msg = await ws.recv()
                    try:
                        msg = json.loads(msg)
                    except:
                        print(msg)
                    else:
                        if msg['type'] != 'contractmon':
                            print(json.dumps(msg)+'\n\n')
                    count += 1
            except asyncio.TimeoutError:
                await ws.send(json.dumps({'command': 'heartbeat', 'sessionID': sessionID}))
                ack = await ws.recv()
                # print(ack)

try:
    asyncio.get_event_loop().run_until_complete(consumer_contract())
except KeyboardInterrupt:
    asyncio.get_event_loop().stop()
