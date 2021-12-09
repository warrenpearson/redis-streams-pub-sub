import json
import time

import redis

from logger import logging
from yaml_config import config


class Subscriber:
    def run(self):
        redis_host = config["redis"]["host"]
        redis_port = config["redis"]["port"]
        redis_db = config["redis"]["db"]
        msg_queue = config["queue"]["name"]
        wait_delay = config["queue"]["wait_delay"]
        read_delay = config["queue"]["read_delay"]

        red = redis.Redis(host=redis_host, port=redis_port, db=redis_db)
        read_from = "0-0"

        while True:
            queue_length = red.xlen(msg_queue)
            logging.info(
                f"reading {msg_queue}, length {queue_length}, from {read_from}"
            )
            streams = {msg_queue: read_from}
            item = red.xread(streams, count=1)
            if item:
                next_key = self.handle_message(item)
                if not next_key:
                    logging.info(f"something went wrong with {item}")
                    read_from = "0-0"

                read_from = next_key
                red.xdel(msg_queue, next_key)
                time.sleep(read_delay)
            else:
                time.sleep(wait_delay)

    def handle_message(self, item):
        item = item[0][1][0]
        key = item[0].decode("utf-8")
        value = item[1]

        msg_type = value[b"type"].decode("utf-8")
        msg_payload = value[b"message"].decode("utf-8")
        msg_obj = json.loads(msg_payload)
        logging.info(f"{msg_type}: {msg_obj}")
        return key


if __name__ == "__main__":
    Subscriber().run()
