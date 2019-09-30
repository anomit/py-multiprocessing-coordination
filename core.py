import pickle
import multiprocessing as mp
from multiprocessing.managers import SyncManager
import psutil
import signal
import logging
import sys
import time
import queue
import tornado
import requests
import tornado.httpserver
import tornado.ioloop
import tornado.options
import tornado.web
import tornado.httpclient
import tornado.escape
from tornado.options import define, options

import json

formatter = logging.Formatter('%(levelname)-8s %(name)-4s %(asctime)s,%(msecs)d %(module)s-%(funcName)s: %(message)s')
pm_logger = logging.getLogger('PMCLogger')
pm_logger.propagate = False
pm_logger.setLevel(logging.DEBUG)

stdout_handler = logging.StreamHandler(sys.stdout)
stdout_handler.setLevel(logging.DEBUG)
stdout_handler.setFormatter(formatter)

stderr_handler = logging.StreamHandler(sys.stderr)
stderr_handler.setLevel(logging.ERROR)
stderr_handler.setFormatter(formatter)

pm_logger.addHandler(stdout_handler)
pm_logger.addHandler(stderr_handler)


RESPAWN_THRESHOLD = 15  # IN SECONDS
FETCHER_CRASH_SIMULATOR_COUNT = 20


class GracefulSIGTERMExit(Exception):
    def __str__(self):
        return "SIGTERMReceivedException"


class SNAFUException(Exception):
    def __str__(self):
        return "SNAFUException"


class FetcherConnectionError(Exception):
    def __str__(self):
        return "FetcherConnectionError"


def sigterm_handler(signum, frame):
    raise GracefulSIGTERMExit


class MainHandler(tornado.web.RequestHandler):
    def initialize(self, sync_q, identifier):
        self._q = sync_q
        self._name_id = identifier

    async def post(self):
        request_json = tornado.escape.json_decode(self.request.body)
        self.set_status(status_code=202)
        self.write({'success': True})
        if request_json['message'] == 'SNAFU':
            self._q.put(json.dumps({'message': 'SNAFU', 'sender': self._name_id, 'rollback_index': request_json['rollbackIndex']}))
            return


class MonitorHandler(tornado.web.RequestHandler):
    def initialize(self, sync_map, identifier):
        self._map = sync_map
        self._name_id = identifier

    async def get(self):
        self.set_status(status_code=202)
        self.write(self._map._getvalue())


class FetcherProcess(mp.Process):
    def __init__(self, name, sync_q, sync_map, begin_idx, simulate_crash_count):
        mp.Process.__init__(self, name=name)
        self._name_id = name
        self._map = sync_map
        self._simulate_crash_count = simulate_crash_count
        self._q = sync_q
        self._idx = begin_idx

    def run(self):
        pm_logger.debug('ProcessRun:%s' % self._name_id)
        pm_logger.debug('Begin Index: %d' % self._idx)
        try:
            simulate_conn_err_counter = 0
            while True:
                # pm_logger.debug('%s : Crash simulator count value: %d' % (self._name_id, simulate_conn_err_counter))
                if simulate_conn_err_counter == self._simulate_crash_count:
                    pm_logger.debug('ProcessCrash:%s' % self._name_id)
                    self._q.put(json.dumps({'process': self._name_id, 'message': 'CRASH', 'time': int(time.time()), 'index': self._idx}))
                    raise FetcherConnectionError
                # make a call to producer
                pm_logger.debug('FetchBlock')
                pm_logger.debug(self._idx)
                r = requests.get(url='http://localhost:5990/'+str(self._idx))
                r = r.json()
                self._map.update({self._idx: r})
                simulate_conn_err_counter += 1
                self._idx += 1
                time.sleep(1)
                # fetch some data

        except KeyboardInterrupt:
            pm_logger.debug('%s Received SIGINT. Going down...' % self._name_id)
        except GracefulSIGTERMExit:
            pm_logger.debug('%s Received SIGTERM. Going down...' % self._name_id)
        except FetcherConnectionError:
            pm_logger.debug('%s encountered connection error. Going down...' % self._name_id)


class PanicLookoutProcess(mp.Process):
    def __init__(self, name, sync_q, sync_map):
        mp.Process.__init__(self, name=name)
        self._name_id = name
        self._map = sync_map
        self._q = sync_q
        tornado.options.parse_command_line()
        self.application = tornado.web.Application([
            (r"/", MainHandler, dict(sync_q=self._q, identifier=self._name_id)),
        ])
        self._http_server = None

    def tornado_sigterm_handler(self, sig, frame):
        tornado.ioloop.IOLoop.current().add_callback_from_signal(self.shutdown)

    def shutdown(self):
        pm_logger.debug('Shutting down tornado')
        self._http_server.stop()
        tornado.ioloop.IOLoop.current().stop()

    def run(self):
        pm_logger.debug('Starting process %s' % self._name_id)
        hn = logging.NullHandler()
        hn.setLevel(logging.DEBUG)
        logging.getLogger("tornado.access").addHandler(hn)
        logging.getLogger("tornado.access").propagate = False
        self._http_server = tornado.httpserver.HTTPServer(self.application)
        self._http_server.listen(options.panic_port)
        signal.signal(signal.SIGTERM, self.tornado_sigterm_handler)
        signal.signal(signal.SIGINT, self.tornado_sigterm_handler)
        tornado.ioloop.IOLoop.current().start()


class ProcessMonitorCoordinator(mp.Process):
    def __init__(self, name, sync_q, sync_map):
        mp.Process.__init__(self, name=name)
        self._name = name
        self._q = sync_q
        self._map = sync_map
        self._process_directory = dict()
        self._crash_timeseries = dict()
        self._to_be_respawned = list()
        self._process_name_mapping = {
            'Fetcher': FetcherProcess,
            'Lookout': PanicLookoutProcess,
            'Monitor': MonitorRPCEndpointProcess
        }
        self._process_reinit_args = {
            'Fetcher': dict(
                name='Fetcher1',
                sync_q=self._q,
                sync_map=self._map,
                simulate_crash_count=FETCHER_CRASH_SIMULATOR_COUNT  # begin index will be added during run
            ),
            'Lookout': dict(
                name='Lookout',
                sync_q=self._q,
                sync_map=self._map
            ),
            'Monitor': dict(
                name='Monitor',
                sync_q=self._q,
                sync_map=self._map
            ),
        }
        signal.signal(signal.SIGTERM, sigterm_handler)

    def _extract_process_key(self, process_name):
        for key in self._process_name_mapping:
            if key in process_name:  # the substring check is a crude way to namespace out the process classes
                return key

    def _respawn(self, process_name):
        process_class_key = self._extract_process_key(process_name)
        pm_logger.debug('Respawning %s process with args:' % process_class_key)
        pm_logger.debug(self._process_reinit_args[process_class_key])
        c = self._process_name_mapping[process_class_key]
        j = c(**self._process_reinit_args[process_class_key])
        j.start()
        self._process_directory[process_name] = {'pid': j.pid}

    @staticmethod
    def _reap_children(timeout=3):
        def on_terminate(proc):
            pm_logger.debug("process {} terminated with exit code {}".format(proc, proc.returncode))

        procs = psutil.Process().children()
        # send SIGTERM
        for p in procs:
            p.terminate()
        gone, alive = psutil.wait_procs(procs, timeout=timeout, callback=on_terminate)
        if alive:
            # send SIGKILL
            for p in alive:
                pm_logger.debug("process {} survived SIGTERM; trying SIGKILL".format(p))
                p.kill()
            gone, alive = psutil.wait_procs(alive, timeout=timeout, callback=on_terminate)
            if alive:
                # give up
                for p in alive:
                    pm_logger.debug("process {} survived SIGKILL; giving up".format(p))

    def run(self) -> None:
        # find if cache already has indexes stored
        idxs = list(self._map._getvalue().keys())
        if idxs:
            begin_idx = max(idxs)
            pm_logger.debug('Got begin index for fetcher')
            pm_logger.debug(begin_idx)
        else:
            begin_idx = 0
        _fetcher = FetcherProcess(
            name='Fetcher1',
            sync_map=self._map,
            sync_q=self._q,
            begin_idx=begin_idx,
            simulate_crash_count=FETCHER_CRASH_SIMULATOR_COUNT
        )
        _fetcher.start()
        self._process_directory['Fetcher1'] = {'pid': _fetcher.pid}

        _lookout_guy = PanicLookoutProcess(name='Lookout', sync_q=self._q, sync_map=self._map)
        _lookout_guy.start()
        self._process_directory['Lookout'] = {'pid': _lookout_guy.pid}

        # TODO: ensure monitor process is set up as Fetcher and Lookout processes to be respawned
        _monitor = MonitorRPCEndpointProcess(
            name='Monitor',
            sync_map=self._map,
            sync_q=self._q
        )
        _monitor.start()
        self._process_directory['Monitor'] = {'pid': _monitor.pid}
        try:
            while True:
                # pm_logger.debug('Child process directory')
                # pm_logger.debug(self._process_directory)
                self._map.update({'processDir': self._process_directory})
                cur_ts = int(time.time())
                _respawned = list()
                for to_be_respawned in self._to_be_respawned:
                    time_diff = cur_ts - self._crash_timeseries[to_be_respawned]
                    if time_diff >= RESPAWN_THRESHOLD:
                        pm_logger.debug('Respawning %s after %d seconds' % (to_be_respawned, time_diff))
                        self._respawn(to_be_respawned)
                        _respawned.append(to_be_respawned)
                self._to_be_respawned = [t for t in self._to_be_respawned if t not in _respawned]
                try:
                    message = self._q.get(block=True, timeout=5)
                except queue.Empty:
                    continue
                self._q.task_done()
                message = json.loads(message)
                pm_logger.debug(message)
                if message['message'] == 'SNAFU' and 'Lookout' in message['sender']:
                    pm_logger.debug('Received SNAFU. Time for everyone to go...')
                    self._reap_children()
                    del self._process_directory['Fetcher1']
                    del self._process_directory['Lookout']
                    del self._process_directory['Monitor']
                    # set them up to be respawned
                    for crashed_process_name in ['Fetcher1', 'Lookout', 'Monitor']:
                        self._crash_timeseries[crashed_process_name] = int(time.time())
                        self._to_be_respawned.append(crashed_process_name)
                    idx = message['rollback_index']
                    self._process_reinit_args['Fetcher'].update({'begin_idx': idx})
                elif message['message'] == 'CRASH':
                    crashed_process_name = message['process']
                    crashed_ts = message['time']
                    pm_logger.debug('Received crashed worker notification | %s' % crashed_process_name)
                    del self._process_directory[crashed_process_name]
                    if crashed_process_name in self._crash_timeseries:
                        pm_logger.debug('%s: Time since last crash: %d' % (crashed_process_name, int(time.time()) - self._crash_timeseries[crashed_process_name]))
                    self._crash_timeseries[crashed_process_name] = int(time.time())
                    self._to_be_respawned.append(crashed_process_name)
                    if crashed_process_name == 'Fetcher1':
                        crashed_index = message['index']
                        self._process_reinit_args['Fetcher'].update({'begin_idx': crashed_index})

        except KeyboardInterrupt:
            pm_logger.debug('Recieved SIGINT. Shutting down.')
        except GracefulSIGTERMExit:
            pm_logger.debug('Received SIGTERM. Shutting down.')
        finally:
            # for each in self._running_processes:
            #     pm_logger.debug('Waiting to join')
            #     pm_logger.debug(each.name)
            #     each.join()
            self._reap_children()


class MonitorRPCEndpointProcess(mp.Process):
    def __init__(self, name, sync_q, sync_map):
        mp.Process.__init__(self, name=name)
        self._name = name
        self._q = sync_q
        self._map = sync_map

        tornado.options.parse_command_line()
        self.application = tornado.web.Application([
            (r"/", MonitorHandler, dict(sync_map=self._map, identifier=self._name)),
        ])
        self._http_server = None

    def tornado_sigterm_handler(self, sig, frame):
        tornado.ioloop.IOLoop.current().add_callback_from_signal(self.shutdown)

    def shutdown(self):
        pm_logger.debug('Shutting down tornado')
        self._http_server.stop()
        tornado.ioloop.IOLoop.current().stop()

    def run(self):
        pm_logger.debug('Starting monitor server process %s' % self._name)
        hn = logging.NullHandler()
        hn.setLevel(logging.DEBUG)
        logging.getLogger("tornado.access").addHandler(hn)
        logging.getLogger("tornado.access").propagate = False
        self._http_server = tornado.httpserver.HTTPServer(self.application)
        self._http_server.listen(options.monitor_port)
        signal.signal(signal.SIGTERM, self.tornado_sigterm_handler)
        signal.signal(signal.SIGINT, self.tornado_sigterm_handler)
        tornado.ioloop.IOLoop.current().start()


def save_to_file(mapping):
    with open('cache', 'wb') as f:
        pickle.dump(mapping, f)


def load_from_file():
    try:
        with open('cache', 'rb') as f:
            return pickle.load(f)
    except (OSError, FileExistsError, FileNotFoundError):
        pm_logger.debug('Cache does not exist yet')
        return None


if __name__ == '__main__':
    define("monitor_port", default=5233, help="run monitor on the given port", type=int)
    define("panic_port", default=5232, help="run panic monitor on the given port", type=int)
    def super_manager_init():
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        pm_logger.debug(
            "{}: Initialized SyncManager for shared queue and mapping".format(psutil.Process().name()))
    try:
        _super_manager = SyncManager()
        _super_manager.start(super_manager_init)
        _talk_queue = _super_manager.Queue()
        _some_mapping = _super_manager.dict()
        cached_mapping = load_from_file()
        if cached_mapping:
            pm_logger.debug('CacheFileLoad')
            try:
                _some_mapping.update(cached_mapping)
            except Exception as e:
                pm_logger.debug(cached_mapping)
                pm_logger.error('Error loading cached mapping', exc_info=True)
                pass
        pmc = ProcessMonitorCoordinator(name='ProcessMonitor', sync_q=_talk_queue, sync_map=_some_mapping)
        pmc.start()
        pmc.join()
    except KeyboardInterrupt:
        pm_logger.error('Main received SIGINT. Going down.')
    finally:
        try:
            pmc.join()
            pm_logger.debug('Saving to cache file')
            mapping_by_val = _some_mapping._getvalue()
            mapping_by_val.pop('processDir', None)
            save_to_file(mapping_by_val)
            _super_manager.shutdown()
            pm_logger.debug('SyncManager shut down')
        except Exception as e:
            pm_logger.error('Residual shutdown errors...')
            pm_logger.error(e, exc_info=True)
