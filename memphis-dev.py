import asyncio

from memphis import *

STATION_NAME = "raanana-west"


async def _get_memphis() -> Memphis:
    try:
        memphis_queue = Memphis()
        await memphis_queue.connect(
            host="localhost", username="conductor", connection_token="memphis"
        )
        return memphis_queue
    except (
        MemphisError,
        MemphisConnectError,
        MemphisHeaderError,
        MemphisSchemaError,
    ) as e:
        print(e)
        raise


async def enqueue():
    print("enqueue...")
    producer_name = "conductor"
    try:
        memphis_queue = await _get_memphis()
        producer = await memphis_queue.producer(
            station_name=STATION_NAME, producer_name=producer_name
        )
        headers = Headers()
        headers.add("producer", producer_name)
        while True:
            for i in range(5):
                await producer.produce(
                    bytearray(f"{i} passengers boarded the train", "utf-8"),
                    headers=headers,
                )
            asyncio.sleep(2)
    except (
        MemphisError,
        MemphisConnectError,
        MemphisHeaderError,
        MemphisSchemaError,
    ) as e:
        print(e)
        raise


async def dequeue():
    async def msg_handler(msgs, error):
        try:
            for msg in msgs:
                await msg.ack()
                headers = msg.get_headers()
                if error:
                    print(error)
                print(f'"message: {headers}: {msg.get_data()}')
        except (MemphisError, MemphisConnectError, MemphisHeaderError) as e:
            print(e)
            return

    print("dequeue...")
    try:
        memphis_queue = await _get_memphis()
        consumer = await memphis_queue.consumer(
            station_name=STATION_NAME,
            consumer_name="manager",
            consumer_group="station-1",
        )

        while True:
            consumer.consume(msg_handler)
            # Keep your main thread alive so the consumer will keep receiving data
            await asyncio.sleep(1)

    except (MemphisError, MemphisConnectError) as e:
        print(e)
        raise


async def main():
    asyncio.gather(enqueue(), dequeue())


if __name__ == "__main__":
    asyncio.run(main())
