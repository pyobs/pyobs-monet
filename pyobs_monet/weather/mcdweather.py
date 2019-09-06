import logging
import threading
from datetime import datetime
import requests

from pyobs import PyObsModule
from pyobs.events import BadWeatherEvent, GoodWeatherEvent
from pyobs.interfaces import IWeather
from pyobs.utils.time import Time

log = logging.getLogger(__name__)


class McDonaldWeather(PyObsModule, IWeather):
    def __init__(self, url: str = 'http://weather.as.utexas.edu/latest_5min.dat', interval: int = 30, *args, **kwargs):
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
                # do request
                r = session.get(self._url)

                # check code
                if r.status_code != 200:
                    raise ValueError('Could not connect to McDonald weather station.')

                # split lines
                lines = [l.strip() for l in r.text.split('\n')]

                # do checks
                # 1st line is title
                if 'Mt. LOCKE LOCAL WEATHER' not in lines[0]:
                    raise ValueError('First line does not contain "Mt. LOCKE LOCAL WEATHER')

                # 2nd line is date
                if 'Current Date GMT' not in lines[1]:
                    raise ValueError('No date given in response from server.')
                # strip line and capitalize first letter of month
                date = lines[1][18:21] + lines[1][21].upper() + lines[1][22:]
                # compare with today
                now = datetime.utcnow()
                if now.strftime('%d-%b-%Y') != date:
                    raise ValueError('Weather data from server not for today.')

                # 3rd, 4th, and 5th lines
                if lines[2] != '|TEMP.|HUMID|DEW PT.| BAROM | WIND_DIR | WIND SPEED  | PARTICLE |  RAIN':
                    raise ValueError('Column headers do not match.')
                if lines[3] != 'TIME  | avg | avg |  avg  | PRESS | avg stdev| avg max min |  COUNT   |  Y/N':
                    raise ValueError('Column headers do not match.')
                if lines[4] != '-[GMT]-|-[F]-|-[%]-|--[F]--|[In.Hg]|---[Az]---|----[MPH]----|--[ppcf]--|[on/off]':
                    raise ValueError('Units do not match.')

                # finally, 6th line is current weather
                s = [l.strip() for l in lines[5].split('|')]

                # time is easy, just concatenate date and time
                time = Time(now.strftime('%Y-%m-%d') + ' ' + s[0] + ':00')

                # convert temp from °F to °C
                try:
                    temp = (float(s[1]) - 32) / 1.8
                except ValueError:
                    temp = None

                # humidity
                try:
                    humid = float(s[2])
                except ValueError:
                    humid = None

                # dew point, given in °F, convert to °C
                try:
                    dew_pt = (float(s[3]) - 32) / 1.8
                except ValueError:
                    dew_pt = None

                # pressure, convert from inHg to hPa
                try:
                    press = float(s[4]) * 33.86389
                except ValueError:
                    press = None

                # wind dir, given as "avg ~ std"
                try:
                    tmp = s[5].split('~')
                    wind_dir = float(tmp[0])
                except ValueError:
                    wind_dir = None

                # wind speed, given as "avg max min", convert from mph to kmh
                try:
                    tmp = s[6].split()
                    wind_speed = float(tmp[1]) / 1.609344
                except ValueError:
                    wind_speed = None

                # particle count, convert from particles per ft³ to particles per m³
                try:
                    particles = float(s[7]) * 3.2808**3
                except ValueError:
                    particles = None

                # rain
                rain = 1 if s[8] == 'Y' else 0

                # set it
                with self._data_lock:
                    self._data = {
                        IWeather.Sensors.TIME: time.iso,
                        IWeather.Sensors.TEMPERATURE: temp,
                        IWeather.Sensors.HUMIDITY: humid,
                        IWeather.Sensors.DEWPOINT: dew_pt,
                        IWeather.Sensors.WINDDIR: wind_dir,
                        IWeather.Sensors.WINDSPEED: wind_speed,
                        IWeather.Sensors.PARTICLES: particles,
                        IWeather.Sensors.RAIN: rain
                    }

                # decide on whether the weather is good or not
                e = IWeather.Sensors
                good = (Time.now() - time).tai < 300 and \
                       self._data[e.HUMIDITY] is not None and self._data[e.HUMIDITY] < 85. and \
                       self._data[e.DEWPOINT] is not None and self._data[e.DEWPOINT] > 2. and \
                       self._data[e.WINDSPEED] is not None and self._data[e.WINDSPEED] < 45. and \
                       self._data[e.PARTICLES] is not None and self._data[e.PARTICLES] < 3.5e6 and \
                       self._data[e.RAIN] is not None and self._data[e.RAIN] == 0.

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


__all__ = ['McDonaldWeather']