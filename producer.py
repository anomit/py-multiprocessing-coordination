import tornado.httpserver
import tornado.ioloop
import tornado.options
import tornado.web
import tornado.httpclient
import tornado.escape
from tornado.options import define, options

from eth_utils import keccak
import random
import json
import time
import logging
import sys

formatter = logging.Formatter('%(levelname)-8s %(name)-4s %(asctime)s,%(msecs)d %(module)s-%(funcName)s: %(message)s')
producer_logger = logging.getLogger('ProducerLogger')
producer_logger.propagate = False
producer_logger.setLevel(logging.DEBUG)

stdout_handler = logging.StreamHandler(sys.stdout)
stdout_handler.setLevel(logging.DEBUG)
stdout_handler.setFormatter(formatter)

stderr_handler = logging.StreamHandler(sys.stderr)
stderr_handler.setLevel(logging.ERROR)
stderr_handler.setFormatter(formatter)

producer_logger.addHandler(stdout_handler)
producer_logger.addHandler(stderr_handler)

hn = logging.NullHandler()
hn.setLevel(logging.DEBUG)
logging.getLogger("tornado.access").addHandler(hn)
logging.getLogger("tornado.access").propagate = False

define("port", default=5990, help="run on the given port", type=int)


class MainHandler(tornado.web.RequestHandler):
    async def get(self, idx):
        # generate a fake block of data
        data = {
            'hash': keccak(text=str(int(time.time())+int(idx))).hex(),
            'metaData': 'meta' + str(random.choice(range(1, 10000000)))
        }
        self.set_status(status_code=202)
        self.write(data)


def main():
    tornado.options.parse_command_line()
    application = tornado.web.Application([
        (r"/([0-9]+)", MainHandler),
    ])
    http_server = tornado.httpserver.HTTPServer(application)
    http_server.listen(options.port)
    try:
        tornado.ioloop.IOLoop.current().start()
    except KeyboardInterrupt:
        tornado.ioloop.IOLoop.current().stop()


if __name__ == '__main__':
    main()
