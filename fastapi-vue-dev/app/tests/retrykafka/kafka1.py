import asyncio
from aiokafka import AIOKafkaConsumer


async def get_conn():
    consumer = AIOKafkaConsumer(
        "simulation_order-command-01",
        # bootstrap_servers="82.156.87.227:9092",
        retry_backoff_ms=1000,
        bootstrap_servers="192.168.101.213:9092",
        auto_offset_reset="earliest",
        group_id="command-test-01")
    return consumer


async def service():
    consumer = await get_conn()
    try:
        await consumer.start()
        try:
            # result = await consumer.getmany()
            # # print(result)
            # for tp, messages in result.items():
            #     if messages:
            #         for message in messages:
            #             print(message)
            async for request in consumer:
                print(request)
        except Exception as e:
            print(e)
        finally:
            await consumer.stop()
            return
    except Exception as e:
        print(e)
        await consumer.stop()
        await asyncio.sleep(3)
        await service()


if __name__ == "__main__":
    asyncio.run(service())
