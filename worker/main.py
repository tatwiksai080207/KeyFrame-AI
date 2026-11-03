"""Worker daemon entrypoint initializing database, topologies, and signal handlers."""

import asyncio
import logging
import signal
import sys
from common.broker import get_connection
from common.db import init_db
from worker.consumer import StageConsumerDaemon

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s]: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("worker")


async def main() -> None:
    """Initializes infrastructure and runs the worker consumer loop until stopped."""
    logger.info("Initializing KeyFrame-AI worker process...")

    # Initialize database tables asynchronously if not yet present
    try:
        await init_db()
        logger.info("Database schema initialized and verified.")
    except Exception as e:
        logger.error(f"Failed to connect to database during startup: {e}")
        sys.exit(1)

    # Establish RabbitMQ connection
    try:
        connection = await get_connection()
        logger.info("Connected to RabbitMQ message broker.")
    except Exception as e:
        logger.error(f"Failed to connect to RabbitMQ broker: {e}")
        sys.exit(1)

    daemon = StageConsumerDaemon(connection=connection, prefetch_count=1)
    await daemon.start()
    logger.info("Worker consumer daemon is actively listening for stage tasks.")

    # Setup shutdown event
    stop_event = asyncio.Event()

    def _trigger_shutdown():
        logger.info("Shutdown signal received. Initiating graceful worker teardown...")
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _trigger_shutdown)
        except NotImplementedError:
            # Signal handling on Windows
            pass

    # Keep daemon alive until shutdown requested
    try:
        await stop_event.wait()
    except asyncio.CancelledError:
        pass
    finally:
        await daemon.stop()
        logger.info("Worker consumer daemon cleanly terminated.")


if __name__ == "__main__":
    asyncio.run(main())

