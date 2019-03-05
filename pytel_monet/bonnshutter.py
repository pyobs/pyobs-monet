import logging
import serial

from pytel import PytelModule

log = logging.getLogger(__name__)


class BonnShutter(PytelModule):
    def __init__(self, device='/dev/ttyUSB0', baud_rate=19200, timeout=1, interval=60, *args, **kwargs):
        PytelModule.__init__(self, thread_funcs=self._mechanic, *args, **kwargs)

        # store
        self._device = device
        self._baud_rate = baud_rate
        self._timeout = timeout
        self._interval = interval

    def _get_status(self):
        # init
        status = {}

        # open connection
        with serial.Serial(self._device, self._baud_rate, timeout=self._timeout) as ser:
            # write new line in order to get status
            ser.write(b'\r\n')
            ser.flush()

            # analyse response
            section = ''
            for bline in ser:
                # decode
                line = bline.decode('utf-8').strip()

                # ignore too short lines
                if len(line) < 5:
                    continue

                # if line starts with Blade, what follows now is the individual blade statuses
                if line.startswith('Blade '):
                    section = line.replace(' ', '') + '.'
                    continue

                # store
                key = line[:28].strip()
                val = line[28:]
                status[section + key] = val

        # finished
        return status

    def _reset_shutter(self):
        # open connection
        with serial.Serial(self._device, self._baud_rate, timeout=self._timeout) as ser:
            # reset
            log.info('Sending reset command...')
            ser.write(b'rs\r\n')
            ser.flush()

            # check response
            log.info('Checking response...')
            line = ser.readline()
            if b'Bonn Shutter' in line:
                log.info('Bonn shutter reset successfully.')
            else:
                log.error('Could not reset Bonn shutter.')

    def _mechanic(self):
        # loop until finished
        while not self.closing.is_set():
            # sleep a little
            self.closing.wait(self._interval)

            # get status
            status = self._get_status()

            # check required fields
            check = ['S_CAN_comm_error', 'S_blade_A_offline', 'S_blade_B_offline', 'S_error_interlock',
                     'BladeA.S_error_LED', 'BladeA.S_error_LED', 'BladeB.S_error_LED', 'BladeB.S_error_LED']
            fields_exist = True
            for c in check:
                if c not in status:
                    log.error('Field %s not in status from shutter, aborting...', c)
                    fields_exist = False
                    break

            # check, whether any of them is ON
            if fields_exist:
                for c in check:
                    if status[c] == 'ON':
                        log.warning('Found error condition in field %s.', c)
                        self._reset_shutter()
                        break


__all__ = ['BonnShutter']
