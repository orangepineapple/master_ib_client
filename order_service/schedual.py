from apscheduler.schedulers.blocking import BlockingScheduler
import logging
import signal
import sys

from config.constants import HOST, CLIENT_NUM
from main import start_order_service
from trading_util.alert_util import runner

import logging

logger = logging.getLogger(__name__)

# Create scheduler
scheduler = BlockingScheduler()

# Add jobs
scheduler.add_job(
    runner,
    'cron',
    day_of_week='mon-fri', 
    hour=12, 
    minute=0, 
    kwargs={"func" : start_order_service, 
            "name" : "IB Order Service Start",
            "host" : HOST,                  # Passing these because ping lives in shared util lib
            "client_num" : CLIENT_NUM},
    id="order service start"
)

# Graceful shutdown
def shutdown(signum, frame):
    logger.info("Shutting down...")
    scheduler.shutdown()
    sys.exit(0)

signal.signal(signal.SIGINT, shutdown)
signal.signal(signal.SIGTERM, shutdown)

if __name__ == "__main__":
    logger.info("Starting BlockingScheduler...")
    scheduler.start()