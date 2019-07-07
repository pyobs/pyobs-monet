import logging
import threading
from sqlalchemy import create_engine

from pyobs import PyObsModule
from pyobs.events import BadWeatherEvent
from pyobs.events.goodweather import GoodWeatherEvent
from pyobs.interfaces import IWeather
from pyobs.utils.time import Time

log = logging.getLogger(__name__)


class SuthWeather(PyObsModule, IWeather):
    def __init__(self, connect: str = '', interval: int = 30, *args, **kwargs):
        PyObsModule.__init__(self, thread_funcs=[self._update_thread], *args, **kwargs)

        # store
        self._connect = connect
        self._interval = interval
        self._data = {}
        self._data_lock = threading.RLock()
        self._weather_good = False

    def _update_thread(self):
        # connect db
        engine = create_engine(self._connect)

        # run until closed
        while not self.closing.is_set():
            try:
                # do request
                with engine.begin() as conn:
                    # do query
                    row = conn.execute('select * from current_weather').fetchone()

                    # get data, available columns are
                    # 'datetime', 'avg_t_min_tdew', 'avg_hum', 'avg_cloud', 'avg_wind', 'avg_temp', 't_min_tdew_warn',
                    # 'hum_warn', 'rain_warn', 'cloud_warn', 'wind_warn', 'temp_warn'
                    with self._data_lock:
                        self._data = {
                            IWeather.Sensors.TIME: row['datetime'],
                            IWeather.Sensors.HUMIDITY: row['avg_hum'],
                            IWeather.Sensors.WINDSPEED: row['avg_wind'],
                            IWeather.Sensors.TEMPERATURE: row['avg_temp']
                        }

                    # get datetime object
                    time = Time(self._data[IWeather.Sensors.TIME])

                    # decide on whether the weather is good or not
                    good = (Time.now() - time).tai < 300 and \
                        self._data[IWeather.Sensors.HUMIDITY] < 85. and \
                        self._data[IWeather.Sensors.WINDSPEED] < 45.

                    # did it change?
                    if self._weather_good != good:
                        # send event
                        if good is True:
                            log.info('Weather is now good.')
                            self.comm.send_event(GoodWeatherEvent())
                        else:
                            log.info('Weather is now bad.')
                            self.comm.send_event(BadWeatherEvent())

                        # store new state
                        self._weather_good = good

            except Exception:
                log.exception('Something went wrong.')

            finally:
                # wait a little until next update
                self.closing.wait(self._interval)

    def get_weather_status(self, *args, **kwargs) -> dict:
        """Returns status of object in form of a dictionary. See other interfaces for details."""
        return {k.value: v for k, v in self._data.items()}

    def is_weather_good(self, *args, **kwargs) -> bool:
        """Whether the weather is good to observe."""
        return self._weather_good


__all__ = ['SuthWeather']
