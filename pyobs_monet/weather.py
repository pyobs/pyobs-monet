import logging
import threading
from sqlalchemy import create_engine

from pyobs import PyObsModule
from pyobs.interfaces import IWeather

log = logging.getLogger(__name__)


class Weather(PyObsModule, IWeather):
    def __init__(self, connect: str = '', interval: int = 30, *args, **kwargs):
        PyObsModule.__init__(self, thread_funcs=[self._update_thread], *args, **kwargs)

        # store
        self._connect = connect
        self._interval = interval
        self._data = {}
        self._data_lock = threading.RLock()

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

            except Exception:
                log.exception('Something went wrong.')

            finally:
                # wait a little until next update
                self.closing.wait(self._interval)

    def get_weather_status(self, *args, **kwargs) -> dict:
        """Returns status of object in form of a dictionary. See other interfaces for details."""
        return self._data


__all__ = ['Weather']
