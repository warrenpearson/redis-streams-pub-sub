import json
import time

import redis

from logger import logging
from yaml_config import config


class Publisher:
    def run(self):
        redis_host = config["redis"]["host"]
        redis_port = config["redis"]["port"]
        redis_db = config["redis"]["db"]
        msg_queue = config["queue"]["name"]
        write_delay = config["queue"]["write_delay"]

        red = redis.Redis(host=redis_host, port=redis_port, db=redis_db)

        while True:
            payload = {"msg_time": time.time(), "msg_text": "hi"}
            payload = json.dumps(payload)
            red.xadd(msg_queue, {"type": "insert", "message": payload}, maxlen=200_000)
            logging.info("pushing")
            time.sleep(write_delay)


if __name__ == "__main__":
    Publisher().run()
