import logging
from enum import Enum
import requests
from sqlalchemy import create_engine

from pyobs import PyObsModule
from pyobs.events import RoofOpenedEvent, RoofClosingEvent, BadWeatherEvent
from pyobs.interfaces import IWeather

log = logging.getLogger(__name__)


class Weather(PyObsModule, IWeather):
    def __init__(self, connect: str = '', interval: int = 30, *args, **kwargs):
        PyObsModule.__init__(self, thread_funcs=[self._update_thread], *args, **kwargs)

        # store
        self._connect = connect

    def _update_thread(self):
        # connect db
        engine = create_engine(self._connect)

        # run until closed
        while not self.closing.is_set():
            try:
                # do request
                with engine.begin() as conn:
                    r = conn.execute('select * from current_weather')
                    print(r)
                    return

            except Exception:
                log.exception('Something went wrong.')

            finally:
                # wait a little until next update
                self.closing.wait(self._interval)


__all__ = ['Weather']
