import logging
from enum import Enum
import requests

from pyobs import PyObsModule
from pyobs.events import RoofOpenedEvent, RoofClosingEvent, BadWeatherEvent
from pyobs.interfaces import IRoof, IWeather

log = logging.getLogger(__name__)


class Status(Enum):
    Opened = 'opened'
    Closed = 'closed'
    Opening = 'opening'
    Closing = 'closing'
    Stopped = 'stopped'
    Unknown = None


class Roof(PyObsModule, IRoof, IWeather):
    def __init__(self, url: str = '', username: str = None, password: str = None, interval: int = 30, *args, **kwargs):
        PyObsModule.__init__(self, thread_funcs=[self._status], *args, **kwargs)

        # store
        self._url = url
        self._username = username
        self._password = password
        self._interval = interval

        # init status
        self._status = Status.Unknown
        self._roof1 = Status.Unknown
        self._roof2 = Status.Unknown

        # change logging level for urllib3
        logging.getLogger("requests.packages.urllib3.connectionpool").setLevel(logging.WARNING)

    def _status(self):
        # open new session
        session = requests.Session()

        # run until closed
        errored = False
        while not self.closing.is_set():
            try:
                r = session.get(self._url + '?type=STATUS', auth=(self._username, self._password))

                # string is a comma-separated list of key=value pairs
                status = {}
                for kv in r.content.split(','):
                    key, value = kv.split('=')
                    status[key] = value

                # we need at least STATE1 and STATE2
                if 'STATE1' not in status or 'STATE2' not in status:
                    # show error message only once
                    if not errored:
                        log.error('Invalid status format.')
                    errored = True
                    self._status = Status.Unknown

                else:
                    # parse status
                    status_left = Status(status['STATE1'].lower())
                    status_right = Status(status['STATE2'].lower())

                    # get some kind of combined status
                    if status_left == Status.Opening or status_right == Status.Opening:
                        # if at least one roof is opening/closing, that's our status
                        new_status = Status.Opening
                    elif status_left == Status.Opening or status_right == Status.Closing:
                        # if at least one roof is opening/closing, that's our status
                        new_status = Status.Closing
                    elif status_left == status_right == Status.Opened:
                        # if both roofs are open, combined status is also open
                        new_status = Status.Opened
                    elif status_left == status_right == Status.Closed:
                        # if both roofs are closed, combined status is also closed
                        new_status = Status.Closed
                    elif status_left == status_right == Status.Stopped:
                        # if both roofs are stopped, combined status is also stopped
                        new_status = Status.Stopped
                    else:
                        # whatever
                        new_status = Status.Unknown

                    # changes?
                    if self._status != new_status:
                        log.info('Roof is now %s.', new_status.value)

                        # send events?
                        if self._status != Status.Unknown:
                            if new_status == Status.Opened:
                                # roof has opened
                                self.comm.send_event(RoofOpenedEvent())
                            elif new_status == Status.Closing:
                                # roof started to close
                                self.comm.send_event(RoofClosingEvent())
                                self.comm.send_event(BadWeatherEvent())

                        # set status
                        self._status = new_status

            except Exception:
                log.exception('Something went wrong.')

            except requests.exceptions.ConnectionError:
                log.error('Could not connect to roof controller')

            finally:
                # wait a little until next update
                self.closing.wait(self.config['interval'])

    def open_roof(self, *args, **kwargs):
        pass

    def close_roof(self, *args, **kwargs):
        pass

    def halt_roof(self, *args, **kwargs):
        pass

    def get_motion_status(self, device: str = None) -> Status:
        return self._status


__all__ = ['Roof']
