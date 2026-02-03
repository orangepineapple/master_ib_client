from apscheduler.schedulers.blocking import BlockingScheduler
import logging
import signal
import sys

from config.constants import HOST, CLIENT_NUM
from trading_util.alert_util import send_notif
from trading_util.network import gateway_check

import logging

logger = logging.getLogger(__name__)

def runner(func, name, **kwargs):
    try :
        func(**kwargs)
    except:
        send_notif(f"@everyone task {name} failed to run")

# Create scheduler
scheduler = BlockingScheduler()

# Add jobs
scheduler.add_job(
    runner,
    'cron',
    day_of_week='mon-fri', 
    hour=12, 
    minute=0, 
    kwargs={"func" : gateway_check, 
            "name" : "IB GateWay Check",
            "host" : HOST,                  # Passing these because ping lives in shared util lib
            "client_num" : CLIENT_NUM},
    id="connection check"
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