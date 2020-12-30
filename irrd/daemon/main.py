#!/usr/bin/env python
# flake8: noqa: E402
import argparse
import logging
import os
import signal
import sys
import time
from pathlib import Path

import daemon
import psutil
from pid import PidFile, PidFileError

logger = logging.getLogger(__name__)
sys.path.append(str(Path(__file__).resolve().parents[2]))

from irrd import __version__, ENV_MAIN_PROCESS_PID
from irrd.conf import config_init, CONFIG_PATH_DEFAULT, get_setting, get_configuration
from irrd.mirroring.scheduler import MirrorScheduler
from irrd.server.http.server import run_http_server
from irrd.server.whois.server import start_whois_server
from irrd.storage.preload import PreloadStoreManager
from irrd.utils.process_support import ExceptionLoggingProcess


# This file does not have a unit test, but is instead tested through
# the integration tests. Writing a unit test would be too complex.

def main():
    description = """IRRd main process"""
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('--config', dest='config_file_path', type=str,
                        help=f'use a different IRRd config file (default: {CONFIG_PATH_DEFAULT})')
    parser.add_argument('--foreground', dest='foreground', action='store_true',
                        help=f"run IRRd in the foreground, don't detach")
    args = parser.parse_args()

    mirror_frequency = int(os.environ.get('IRRD_SCHEDULER_TIMER_OVERRIDE', 15))

    daemon_kwargs = {
        'umask': 0o022,
    }
    if args.foreground:
        daemon_kwargs['detach_process'] = False
        daemon_kwargs['stdout'] = sys.stdout
        daemon_kwargs['stderr'] = sys.stderr

    # config_init w/ commit may only be called within DaemonContext
    config_init(args.config_file_path, commit=False)

    with daemon.DaemonContext(**daemon_kwargs):
        config_init(args.config_file_path)
        piddir = get_setting('piddir')
        logger.info('IRRd attempting to secure PID')
        try:
            with PidFile(pidname='irrd', piddir=piddir):
                logger.info(f'IRRd {__version__} starting, PID {os.getpid()}, PID file in {piddir}')
                run_irrd(mirror_frequency, args.config_file_path if args.config_file_path else CONFIG_PATH_DEFAULT)
        except PidFileError as pfe:
            logger.error(f'Failed to start IRRd, unable to lock PID file irrd.pid in {piddir}: {pfe}')
        except Exception as e:
            logger.error(f'Error occurred in main process, terminating. Error follows:')
            logger.exception(e)
            os.kill(os.getpid(), signal.SIGTERM)


def run_irrd(mirror_frequency: int, config_file_path: str):
    terminated = False
    os.environ[ENV_MAIN_PROCESS_PID] = str(os.getpid())

    mirror_scheduler = MirrorScheduler()
    whois_process = ExceptionLoggingProcess(target=start_whois_server, name='irrd-whois-server-listener')
    whois_process.start()
    preload_manager = PreloadStoreManager(name='irrd-preload-store-manager')
    preload_manager.start()
    uvicorn_process = ExceptionLoggingProcess(target=run_http_server, name='irrd-http-server-listener', args=(config_file_path, ))
    uvicorn_process.start()

    def sighup_handler(signum, frame):
        # On SIGHUP, check if the configuration is valid and reload in
        # this process, and if it is valid, signal SIGHUP to all
        # child processes.
        if get_configuration().reload():
            parent = psutil.Process(os.getpid())
            children = parent.children(recursive=True)
            for process in children:
                process.send_signal(signal.SIGHUP)
            if children:
                logging.info('Main process received SIGHUP with valid config, sent SIGHUP to '
                             f'child processes {[c.pid for c in children]}')
    signal.signal(signal.SIGHUP, sighup_handler)

    def sigterm_handler(signum, frame):
        mirror_scheduler.terminate_children()
        parent = psutil.Process(os.getpid())
        children = parent.children(recursive=True)
        for process in children:
            try:
                process.send_signal(signal.SIGTERM)
            except Exception:
                # If we can't SIGTERM some of the processes,
                # do the best we can.
                pass
        if children:
            logging.info('Main process received SIGTERM, sent SIGTERM to '
                         f'child processes {[c.pid for c in children]}')

        nonlocal terminated
        terminated = True
    signal.signal(signal.SIGTERM, sigterm_handler)

    sleeps = mirror_frequency
    while not terminated:
        # This loops every second to prevent long blocking on SIGTERM.
        mirror_scheduler.update_process_state()
        if sleeps >= mirror_frequency:
            mirror_scheduler.run()
            sleeps = 0
        time.sleep(1)
        sleeps += 1

    logging.debug(f'Main process waiting for child processes to terminate')
    for child_process in whois_process, uvicorn_process, preload_manager:
        child_process.join(timeout=3)

    parent = psutil.Process(os.getpid())
    children = parent.children(recursive=True)
    for process in children:
        try:
            process.send_signal(signal.SIGKILL)
        except Exception:
            pass
    if children:
        logging.info('Some processes left alive after SIGTERM, send SIGKILL to '
                     f'child processes {[c.pid for c in children]}')

    logging.info(f'Main process exiting')


if __name__ == '__main__':  # pragma: no cover
    main()
