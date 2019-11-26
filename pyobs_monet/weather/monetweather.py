import logging
import threading
import requests

from pyobs import PyObsModule
from pyobs.events import BadWeatherEvent, GoodWeatherEvent
from pyobs.interfaces import IWeather, IFitsHeaderProvider
from pyobs.utils.time import Time

log = logging.getLogger(__name__)


class MonetWeather(PyObsModule, IWeather, IFitsHeaderProvider):
    def __init__(self, url: str = 'http://weather.monetn:8888/', interval: int = 30,
                 *args, **kwargs):
        PyObsModule.__init__(self, *args, **kwargs)

        # add thread func
        self._add_thread_func(self._update_thread, True)

        # store
        self._url = url
        self._interval = interval
        self._data = {}
        self._data_lock = threading.RLock()
        self._weather_good = None

    def _update_thread(self):
        # create session
        session = requests.session()

        # run until closed
        while not self.closing.is_set():
            try:
                # do request and parse json
                resp = session.get(self._url, params={'type': '5min'})

                # check code
                if resp.status_code != 200:
                    raise ValueError('Could not connect to Monet weather station.')

                # to json
                data = resp.json()

                # set it
                with self._data_lock:
                    self._data = {
                        IWeather.Sensors.TIME: data['time'],
                        IWeather.Sensors.TEMPERATURE: data['temp']['avg'],
                        IWeather.Sensors.HUMIDITY: data['humid']['avg'],
                        IWeather.Sensors.WINDDIR: data['winddir']['avg'],
                        IWeather.Sensors.WINDSPEED: data['windspeed']['avg'] * 3.6,
                        IWeather.Sensors.RAIN: data['rain']['max']
                    }

                # parse time
                time = Time(data['time'])

                # decide on whether the weather is good or not
                e = IWeather.Sensors
                good = (Time.now() - time).tai < 300 and \
                       self._data[e.HUMIDITY] is not None and self._data[e.HUMIDITY] < 85. and \
                       self._data[e.WINDSPEED] is not None and self._data[e.WINDSPEED] < 45. and \
                       self._data[e.RAIN] is not None and self._data[e.RAIN] is False

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

            except ValueError as e:
                self._weather_good = False
                log.error(str(e))

            except:
                log.exception('Something went wrong.')
                self._weather_good = False

            finally:
                # wait a little until next update
                self.closing.wait(self._interval)

    def get_weather_status(self, *args, **kwargs) -> dict:
        """Returns status of object in form of a dictionary. See other interfaces for details."""
        return {k.value: v for k, v in self._data.items()}

    def is_weather_good(self, *args, **kwargs) -> bool:
        """Whether the weather is good to observe."""
        return self._weather_good is True

    def get_fits_headers(self, namespaces: list = None, *args, **kwargs) -> dict:
        """Returns FITS header for the current status of this module.

        Args:
            namespaces: If given, only return FITS headers for the given namespaces.

        Returns:
            Dictionary containing FITS headers.
        """

        # get data
        hdr = {}
        with self._data_lock:
            try:
                hdr['WS-TEMP'] = (self._data[IWeather.Sensors.TEMPERATURE],
                                  'Ambient temperature average during exposure, C')
            except:
                log.error('Could not get temperature for FITS headers.')
            try:
                hdr['WS-HUMID'] = (self._data[IWeather.Sensors.HUMIDITY], 'Ambient rel. humidity average, %')
            except:
                log.error('Could not get humidity for FITS headers.')
            try:
                hdr['WS-WIND'] = (self._data[IWeather.Sensors.WINDSPEED], 'Ambient average wind speed, m/s')
            except:
                log.error('Could not get wind speed for FITS headers.')
            try:
                hdr['WS-AZ'] = (self._data[IWeather.Sensors.WINDDIR], 'Average wind direction, deg')
            except:
                log.error('Could not get wind direction for FITS header.')
            try:
                hdr['WS-PREC'] = (1 if self._data[IWeather.Sensors.RAIN] else 0, 'Ambient precipitation [0/1]')
            except:
                log.error('Could not get rain status for FITS header.')

        # return it
        return hdr


__all__ = ['MonetWeather']
