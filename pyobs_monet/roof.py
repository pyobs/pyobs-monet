import logging
from enum import Enum
import requests

from pyobs import PyObsModule
from pyobs.events import RoofOpenedEvent, RoofClosingEvent
from pyobs.interfaces import IRoof, IMotion

log = logging.getLogger(__name__)


class Roof(PyObsModule, IRoof):
    class Status(Enum):
        Opened = 'opened'
        Closed = 'closed'
        Opening = 'opening'
        Closing = 'closing'
        Stopped = 'stopped'
        Unknown = None
        
    def __init__(self, url: str = '', username: str = None, password: str = None, interval: int = 30, *args, **kwargs):
        PyObsModule.__init__(self, thread_funcs=[self._update_thread], *args, **kwargs)

        # store
        self._url = url
        self._username = username
        self._password = password
        self._interval = interval

        # init status
        self._status = Roof.Status.Unknown
        self._roof1 = Roof.Status.Unknown
        self._roof2 = Roof.Status.Unknown

        # change logging level for urllib3
        logging.getLogger("requests.packages.urllib3.connectionpool").setLevel(logging.WARNING)

    def _update_thread(self):
        # open new session
        session = requests.Session()

        # run until closed
        errored = False
        while not self.closing.is_set():
            try:
                # do request
                r = session.get(self._url + '?STATUS', auth=(self._username, self._password))

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
                    elif status_left == Roof.Status.Opening or status_right == Roof.Status.Closing:
                        # if at least one roof is opening/closing, that's our status
                        new_status = Roof.Status.Closing
                    elif status_left == status_right == Roof.Status.Opened:
                        # if both roofs are open, combined status is also open
                        new_status = Roof.Status.Opened
                    elif status_left == status_right == Roof.Status.Closed:
                        # if both roofs are closed, combined status is also closed
                        new_status = Roof.Status.Closed
                    elif status_left == status_right == Roof.Status.Stopped:
                        # if both roofs are stopped, combined status is also stopped
                        new_status = Roof.Status.Stopped
                    else:
                        # whatever
                        new_status = Roof.Status.Unknown

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

            except Exception:
                log.exception('Something went wrong.')

            except requests.exceptions.ConnectionError:
                log.error('Could not connect to roof controller')

            finally:
                # wait a little until next update
                self.closing.wait(self._interval)

    def open_roof(self, *args, **kwargs):
        """Open the roof."""
        pass

    def close_roof(self, *args, **kwargs):
        """Close the roof."""
        pass

    def halt_roof(self, *args, **kwargs):
        pass

    def get_motion_status(self, device: str = None, *args, **kwargs) -> IMotion.Status:
        """Returns current motion status.

        Args:
            device: Name of device to get status for, or None.

        Returns:
            A string from the Status enumerator.
        """

        if self._status == Roof.Status.Opened:
            return IMotion.Status.POSITIONED
        elif self._status == Roof.Status.Closed:
            return IMotion.Status.PARKED
        elif self._status == Roof.Status.Opening:
            return IMotion.Status.INITIALIZING
        elif self._status == Roof.Status.Closing:
            return IMotion.Status.PARKING
        elif self._status == Roof.Status.Stopped:
            return IMotion.Status.IDLE
        else:
            return IMotion.Status.UNKNOWN


__all__ = ['Roof']
