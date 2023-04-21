from loguru import logger

logger.add("log/fastapi-async_kafka_order_resp_server_v1.1.log", rotation="100MB", encoding="utf-8", enqueue=True,
           format="{time:YYYY-MM-DD HH:mm:ss} |  {name} | {line} | {message}", retention="10 days")
