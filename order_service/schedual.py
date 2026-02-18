from apscheduler.schedulers.blocking import BlockingScheduler
import logging
import signal
import sys
from datetime import datetime, time

from main import start_order_service
from trading_util.alert_util import runner

import logging

logger = logging.getLogger(__name__)

def run_catchup():
    now = datetime.now()
    is_weekday = now.weekday() < 5  # 0=Mon, 4=Fri
    is_after_open = now.time() >= time(9, 30)
    
    # Optional: don't catch up if it's too late in the day
    is_before_close = now.time() < time(16, 0)
    
    if is_weekday and is_after_open and is_before_close:
        runner(func=start_order_service, name="IB Order Service")

# Create scheduler
scheduler = BlockingScheduler()

# Add jobs
scheduler.add_job(
    runner,
    'cron',
    day_of_week='mon-fri', 
    hour=9, 
    minute=0, 
    kwargs={"func" : start_order_service, 
            "name" : "IB Order Service"},
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
    run_catchup()
    scheduler.start()