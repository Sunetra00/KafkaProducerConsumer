from fastapi import FastAPI, BackgroundTasks
from kafka.producer import produce_message
from kafka.consumer import consume_messages
from config import TOPIC_NAME
from logger import setup_logger



app = FastAPI()
logger = setup_logger()

@app.post("/produce")
async def produce_endpoint(message: str):
    """
    Produce a message to Kafka.
    """
    await produce_message(TOPIC_NAME, message)
    return {"status": "success", "message": message}

@app.get("/consume")
async def consume_endpoint(background_tasks: BackgroundTasks):
    """
    Start consuming messages from Kafka.
    """
    def process_message(msg):
        logger.info(f"Processed message: {msg}")

    background_tasks.add_task(consume_messages, process_message)
    return {"status": "success", "message": "Consuming messages in the background."}

@app.on_event("startup")
async def startup_event():
    logger.info("Application is starting.")

@app.on_event("shutdown")
async def shutdown_event():
    logger.info("Application is shutting down.")
