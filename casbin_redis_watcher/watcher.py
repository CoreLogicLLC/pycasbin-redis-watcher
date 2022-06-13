import logging
import time
from multiprocessing import Process, Pipe

import redis
from redis import Redis

REDIS_CHANNEL_NAME = "casbin-role-watcher"
logging.basicConfig()
logger = logging.getLogger()


def get_redis(redis_url, redis_port):
    r = Redis(redis_url, redis_port)
    return r


def redis_casbin_subscription(redis_url, process_conn, redis_port=None, delay=0):
    # in case we want to delay connecting to redis (redis connection failure)
    time.sleep(delay)
    r = redis.Redis(redis_url, redis_port)
    p = r.pubsub()
    p.subscribe(REDIS_CHANNEL_NAME)
    logger.info("Waiting for casbin policy updates...")
    while True and r:
        # wait 20 seconds to see if there is a casbin update
        try:
            message = p.get_message(timeout=20)
        except Exception as e:
            logger.error(
                "Casbin watcher failed to get message from redis due to: %s", repr(e)
            )
            p.close()
            r = None
            break

        if message and message.get("type") == "message":
            logger.info("Casbin policy update identified.." " Message was: %s", message)
            try:
                process_conn.send(message)
            except Exception as e:
                logger.error(
                    "Casbin watcher failed sending update to piped"
                    " process due to: %s",
                    repr(e),
                )
                p.close()
                r = None
                break


class RedisWatcher(object):
    def __init__(self, redis_host, redis_port=None, start_process=True):
        self.redis_url = redis_host
        self.redis_port = redis_port
        self.subscribed_process, self.parent_conn = self.create_subscriber_process(
            start_process
        )

        self.pool = redis.ConnectionPool(host=redis_host, port=redis_port)

    def create_subscriber_process(self, start_process=True, delay=0):
        parent_conn, child_conn = Pipe()
        p = Process(
            target=redis_casbin_subscription,
            args=(self.redis_url, child_conn, self.redis_port, delay),
            daemon=True,
        )
        if start_process:
            p.start()
        return p, parent_conn

    def set_update_callback(self, fn):
        self.update_callback = fn

    def update_callback(self):
        logger.info("callback called because casbin role updated")

    def update(self):
        r = redis.Redis(connection_pool=self.pool)
        res = r.publish(REDIS_CHANNEL_NAME, f"casbin policy updated at {time.time()}")
        logger.info("Message received by %s clients", res)

    def should_reload(self):
        try:
            if self.parent_conn.poll():
                message = self.parent_conn.recv()
                return True
        except EOFError:
            logger.error(
                "Child casbin-watcher subscribe process has stopped, "
                "attempting to recreate the process in 10 seconds..."
            )
            self.subscribed_process, self.parent_conn = self.create_subscriber_process(
                delay=10
            )
            return False
