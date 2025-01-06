from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError
from fastapi import HTTPException
from config import KAFKA_BROKER
import logging

logger = logging.getLogger(__name__)
producer = None

async def get_producer():
    global producer
    if not producer:
        try:
            producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BROKER)
            await producer.start()
            logger.info("Kafka Producer started.")
        except KafkaError as e:
            logger.error(f"Failed to start Kafka Producer: {str(e)}")
            raise HTTPException(status_code=500, detail="Kafka Producer initialization failed.")
    return producer

async def produce_message(topic: str, message: str):
    try:
        producer = await get_producer()
        await producer.send_and_wait(topic, message.encode("utf-8"))
        logger.info(f"Message produced to topic '{topic}': {message}")
    except KafkaError as e:
        logger.error(f"Failed to produce message: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to produce message.")
