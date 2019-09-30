#!/usr/bin/env python

import re
import time
import argparse
import logging
import requests
import psutil
import statsd
import time


def find_process(pid, regex):
    if pid:
        try:
            return psutil.Process(pid)
        except psutil.NoSuchProcess:
            print('Target process not found')
    elif regex:
        current_pid = psutil.Process().pid
        for p in psutil.process_iter():
            if regex.search(' '.join(p.cmdline())) and p.pid != current_pid:
                return p
    else:
        raise ValueError('No pid or regex')


def main(host, prefix, interval, pid, regex):
    print(prefix)
    client = statsd.StatsClient(host=host, prefix='.'.join(filter(bool, ['procmon', prefix])))
    while True:
        try:
            proc = None
            if prefix == 'fetcher':
                print('Attempting to retrieve fetcher process ID from process monitor')
                print('Calling RPC endpoint')
                r = requests.get(url='http://localhost:5233')
                # print('Response: ', r.text)
                r = r.json()
                try:
                    pid_2 = r['processDir']['Fetcher1']['pid']
                except KeyError:
                    print('No Fetcher process info present yet. Sleeping for 5...')
                else:
                    print('Found fetcher PID: ', pid_2)
                    proc = psutil.Process(pid_2)
            else:
                proc = find_process(pid, regex)
            print(proc)
            if proc:
                logging.debug('Found process %s, collecting metrics', proc)
            else:
                logging.warning('No target process found')
                time.sleep(5)
                continue

            try:
                pct = proc.cpu_percent(interval) # the call is blocking
                mem = proc.memory_info().rss
            except psutil.NoSuchProcess:
                logging.warning('Target process was terminated during collection')
            else:
                client.gauge('cpu', pct)
                client.gauge('rss', mem)
                logging.debug('Sent CPU:%.0f%% RSS:%d', pct, mem)
        except KeyboardInterrupt:
            break


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Process monitor')
    parser.add_argument('-p', '--pid', help='Target process pid', default=0)
    parser.add_argument('-r', '--regex', help='Target process cmdline regex', default='')
    parser.add_argument('-f', '--prefix', help='Statsd prefix', default='')
    parser.add_argument('-s', '--host', help='Statsd host', default='localhost')
    parser.add_argument('-i', '--interval', help='Collection interval in seconds', default=1)
    parser.add_argument('-v', '--verbose', help='Be verbose', action='store_true', default=False)

    args = parser.parse_args()
    if not any([args.pid, args.regex]):
        parser.error('Either PID or cmdline regex must be provided')

    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(level=level, format='%(asctime)s %(levelname)s %(name)s %(message)s')

    main(args.host, args.prefix, float(args.interval), int(args.pid), re.compile(args.regex))