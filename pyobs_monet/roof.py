import logging
import time
from enum import Enum
import requests
from requests import Timeout

from pyobs.events import RoofOpenedEvent, RoofClosingEvent
from pyobs.interfaces import IMotion
from pyobs.modules import timeout
from pyobs.modules.roof import BaseRoof

log = logging.getLogger(__name__)


class Roof(BaseRoof):
    class Status(Enum):
        Opened = 'opened'
        Closed = 'closed'
        Opening = 'opening'
        Closing = 'closing'
        Stopped = 'stopped'
        Unknown = 'unknown'
        
    def __init__(self, url: str = '', username: str = None, password: str = None, interval: int = 10,
                 auto_reset: bool = False, *args, **kwargs):
        """Creates a module for the Monet roofs.

        Args:
            url: URL of roof controller.
            username: Username for roof controller.
            password: Password for roof controller.
            interval: Interval in which to update status.
            reset: Whether to reset the roof automatically on error.
        """

        BaseRoof.__init__(self, *args, **kwargs)

        # add thread func
        self._add_thread_func(self._update_thread, True)

        # store
        self._url = url
        self._username = username
        self._password = password
        self._interval = interval
        self._auto_reset = auto_reset

        # unknown since and last reset
        self._unknown_since = None
        self._last_reset = time.time()

        # init status
        self._status = Roof.Status.Unknown
        self._roof1 = Roof.Status.Unknown
        self._roof2 = Roof.Status.Unknown
        self._mode = None

        # change logging level for urllib3
        logging.getLogger("requests.packages.urllib3.connectionpool").setLevel(logging.WARNING)

    def _update_thread(self):
        # open new session
        session = requests.Session()

        # was last request a success?
        last_was_success = True

        # run until closed
        errored = False
        while not self.closing.is_set():
            try:
                # do request
                try:
                    r = session.get(self._url + '?STATUS', auth=(self._username, self._password), timeout=5)
                except Timeout:
                    # no success
                    last_was_success = False

                    # log it, if it's new
                    if last_was_success:
                        log.error('Could not connect to roof.')
                        self.closing.wait(5)
                        continue

                # first successful request?
                if not last_was_success:
                    log.info('Connected to roof successfully.')
                last_was_success = True

                # get content
                content = r.content.decode('utf-8')

                # TODO: remove, this is for circumventing a domeweb bug...
                if '<' in content:
                    content = content[:content.find('<')]

                # string is a comma-separated list of key=value pairs
                status = {}
                for kv in content.split(','):
                    key, value = kv.split('=')
                    status[key] = value

                # we need at least STATE1 and STATE2
                if 'STATE1' not in status or 'STATE2' not in status:
                    # show error message only once
                    if not errored:
                        log.error('Invalid status format.')
                    errored = True
                    self._status = Roof.Status.Unknown

                else:
                    # parse status
                    status_left = Roof.Status(status['STATE1'].lower())
                    status_right = Roof.Status(status['STATE2'].lower())

                    # get some kind of combined status
                    if status_left == Roof.Status.Opening or status_right == Roof.Status.Opening:
                        # if at least one roof is opening/closing, that's our status
                        new_status = Roof.Status.Opening
                    elif status_left == Roof.Status.Closing or status_right == Roof.Status.Closing:
                        # if at least one roof is opening/closing, that's our status
                        new_status = Roof.Status.Closing
                    elif status_left == Roof.Status.Stopped or status_right == Roof.Status.Stopped:
                        # if at least one roof is stopped, that's our status
                        new_status = Roof.Status.Stopped
                    elif status_left == status_right == Roof.Status.Opened:
                        # if both roofs are open, combined status is also open
                        new_status = Roof.Status.Opened
                    elif status_left == status_right == Roof.Status.Closed:
                        # if both roofs are closed, combined status is also closed
                        new_status = Roof.Status.Closed
                    else:
                        # whatever
                        new_status = Roof.Status.Unknown
                        log.info('Unknown state: %s', content)

                        # if mode is not "local", we need to deal with this
                        if self._mode != 'local':
                            # is this the first time that we're in unknown state?
                            if self._unknown_since is None:
                                # just remember it
                                self._unknown_since = time.time()

                            else:
                                # okay, seems to be going on for longer, but wait at least 20 seconds
                                if time.time() - self._unknown_since > 20:
                                    # reset it only every 60 seconds
                                    if time.time() - self._last_reset > 60 and self._auto_reset:
                                        # reset roof
                                        log.info('Resetting roof...')
                                        session.get(self._url + '?RESET', auth=(self._username, self._password))
                                        self._last_reset = time.time()
                                        log.info('Done.')

                    # reset unknown
                    if new_status != Roof.Status.Unknown:
                        self._unknown_since = None

                    # changes?
                    if self._status != new_status:
                        log.info('Roof is now %s.', new_status.value)

                        # send events?
                        if self._status != Roof.Status.Unknown:
                            if new_status == Roof.Status.Opened:
                                # roof has opened
                                self.comm.send_event(RoofOpenedEvent())
                            elif new_status == Roof.Status.Closing:
                                # roof started to close
                                self.comm.send_event(RoofClosingEvent())

                        # set status
                        self._status = new_status

                        # and evaluate it
                        self._eval_status()

                # finally, set mode
                if self._mode != status['MODE'].lower():
                    if self._mode is not None:
                        log.info('Mode of roof changed from %s to %s.', self._mode.upper(), status['MODE'].upper())
                        log.info('Debug: %s', content)
                    self._mode = status['MODE'].lower()

            except Exception:
                log.exception('Something went wrong.')

            except requests.exceptions.ConnectionError:
                log.error('Could not connect to roof controller')

            finally:
                # wait a little until next update
                self.closing.wait(self._interval)

    @timeout(300000)
    def init(self, *args, **kwargs):
        """Open roof.

        Raises:
            ValueError: If device could not be initialized.
        """

        # do nothing in local mode
        if self._mode != 'auto':
            log.warning('Roof in local mode, cannot do anything...')
            return

        # only close, if not already closed
        if self._status != Roof.Status.Opened:
            # good weather?
            if not self.is_weather_good():
                log.warning('Weather module reports bad weather, will not open.')
                return

            # open roof
            log.info('Opening roof...')
            requests.get(self._url + '?OPENALL', auth=(self._username, self._password))

            # wait for closed status
            while self._status != Roof.Status.Opened:
                self.closing.wait(1)

            # finished
            log.info('Opened roof successfully.')

    @timeout(300000)
    def park(self, *args, **kwargs):
        """Close roof.

        Raises:
            ValueError: If device could not be parked.
        """

        # do nothing in local mode
        if self._mode != 'auto':
            log.warning('Roof in local mode, cannot do anything...')
            return

        # only close, if not already closed
        if self._status != Roof.Status.Closed:
            # close roof
            log.info('Closing roof...')
            requests.get(self._url + '?CLOSEALL', auth=(self._username, self._password))

            # wait for closed status
            while self._status != Roof.Status.Closed:
                self.closing.wait(1)

            # finished
            log.info('Closed roof successfully.')

    def stop_motion(self, device: str = None, *args, **kwargs):
        """Stop motion of roof.

        Args:
            device: Name of device to stop, or None for all.
        """

        # stop roof
        log.info('Stopping roof...')
        requests.get(self._url + '?STOPALL', auth=(self._username, self._password))

        # wait for non-moving status
        while self._status in [Roof.Status.Opening, Roof.Status.Closing]:
            self.closing.wait(1)

        # finished
        log.info('Stopped roof successfully.')

    def _eval_status(self):
        """Evaluate internal status."""
        if self._status == Roof.Status.Opened:
            self._change_motion_status(IMotion.Status.POSITIONED)
        elif self._status == Roof.Status.Closed:
            self._change_motion_status(IMotion.Status.PARKED)
        elif self._status == Roof.Status.Opening:
            self._change_motion_status(IMotion.Status.INITIALIZING)
        elif self._status == Roof.Status.Closing:
            self._change_motion_status(IMotion.Status.PARKING)
        elif self._status == Roof.Status.Stopped:
            self._change_motion_status(IMotion.Status.IDLE)
        else:
            self._change_motion_status( IMotion.Status.UNKNOWN)


__all__ = ['Roof']
