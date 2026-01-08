import aio_pika
import asyncio
import json
import os

RABBIT_URL = os.getenv("RABBIT_URL")
EXCHANGE_NAME = "events_topic"

async def main():
    if not RABBIT_URL:
        raise RuntimeError("RABBIT_URL is not set. Did you load .env or set it in Docker Compose?")

    conn = await aio_pika.connect_robust(RABBIT_URL)
    ch = await conn.channel()

    ex = await ch.declare_exchange(EXCHANGE_NAME, aio_pika.ExchangeType.TOPIC)

    queue = await ch.declare_queue("repair_events_queue", durable=True)
    await queue.bind(ex, routing_key="repair.#")

    print("Listening for repair events (routing key: 'repair.#')...")

    async with queue.iterator() as q:
        async for msg in q:
            async with msg.process():
                data = json.loads(msg.body)
                print("Repair Event:", msg.routing_key, data)

if __name__ == "__main__":
    asyncio.run(main())
