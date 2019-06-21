import logging
import os
import re
import math
from astropy.io import fits
import pymysql

from pyobs.interfaces import IImageDB
from pyobs.modules.imagedb import NewImageWatcher
from pyobs.utils.time import Time

log = logging.getLogger(__name__)


"""Mapping of IImageDB.ImageType to database values"""
IMAGE_TYPES_DICT = {
    IImageDB.ImageType.OBJECT: 0,
    IImageDB.ImageType.BIAS: 1,
    IImageDB.ImageType.DARK: 2,
    IImageDB.ImageType.FLAT: 3
}
IMAGE_TYPES_VALUES_DICT = {k.value: v for k, v in IMAGE_TYPES_DICT.items()}

"""Mapping of IImageDB.QueryDict to database fields."""
QUERY_DICT = {
    IImageDB.QueryDict.BINNING: 'binning',
    IImageDB.QueryDict.FILTER: 'filter',
    IImageDB.QueryDict.TARGET: 'objname',
    IImageDB.QueryDict.PROGRAM: 'object',
    IImageDB.QueryDict.USER: 'user'
}

"""Mapping of telescopes."""
TELESCOPES_DICT = {'MONETN': 0, 'MONETS': 1}

"""Mapping of instruments."""
INSTRUMENTS_DICT = {
    'Apogee ALTA E47+': 0,
    'SBIG STF-8300M': 1,
    'Spectral Instruments 1100': 2,
    'FLI ProLine PL230': 3
}

"""Mapping of everything else."""
MAPPING_DICT = {
    'object': 'STELLAID',
    'objname': 'OBJECT',
    'user': 'USER',
    'exptime': 'EXPTIME',
    'filter': 'FILTER',
    'datamean': 'DATAMEAN',
    'telaz': 'TEL-AZ',
    'telalt': 'TEL-ALT',
    'telt1': 'TEL-T1',
    'telt2': 'TEL-T2',
    'telfocus': 'TEL-FOCU',
    # 'teldoff': 'TEL-DOFF',
    'telrot': 'TEL-ROT',
    # 'telpara': 'TEL-PARA',
    'moonalt': 'MOON-ALT',
    'moonill': 'MOON-ILL',
    # 'moonlig': 'MOON-LIG',
    'moonsep': 'MOON-SEP',
    'solalt': 'SOL-ALT',
    # 'domestat': '',
    'dettemp': 'DET-TEMP',
    'ambtemp': 'WS-TEMP',
    'relhum': 'WS-HUMID',
    'dewpoint': 'WS-TDEW',
    'avrgwind': 'WS-WIND',
    'winddir': 'WS-AZ',
    'atmpress': 'WS-PRESS',
    # 'skytemp': 'WS-SKY',
    'rain': 'WS-PREC'
}


class DB(object):
    def __init__(self, *args, **kwargs):
        self.db = pymysql.connect(*args, **kwargs)

    def __enter__(self):
        return self.db

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.db.close()


class MonetImageWatcher(NewImageWatcher):
    def __init__(self, db: str = None, table: str = 'obs', *args, **kwargs):
        """Create new image watcher.

        Args:
            stella_db: Connect string for stella database.
            stella_table: Table in stella DB.
        """
        NewImageWatcher.__init__(self, *args, **kwargs)

        # store
        self._db_connect = db
        self._table = table
        self._db_connect = None

    def open(self):
        """Open module."""
        NewImageWatcher.open(self)

        # parse database connection
        db = re.match('(\w*)://((\w*):(\w*)@)?([^/]*)/(.*)', self._db_connect)
        if not db:
            log.error('Invalid DB connect string: ' + self._db_connect)
            return False
        self._db_connect = (db.group(5), db.group(3), db.group(4), db.group(6))

    def before_add(self, filename: str, hdus) -> bool:
        """Hook for derived classed that is called directly before sending the file to the ImageDB.

        If False is returned, the file will not be added to the database.

        Args:
            filename: Name of file to add.
            fits_file: Opened FITS file.

        Returns:
            Whether or not to continue with this file.
        """

        # add fits headers
        hdus[0].header['OBS'] = hdus[0].header['OBJID']
        hdus[0].header['TASK'] = 'stella.' + hdus[0].header['STELLAID']

        # add to stella db
        if not self._add_to_stella_db(filename):
            return False

        # success
        return True

    def _add_to_stella_db(self, filename: str) -> bool:
        """Add image to stella DB.

        Args:
            filename: Name of file to add.

        Returns:
            Success or not.
        """

        # init
        basename = os.path.basename(filename)
        log.info('Processing %s...', basename)

        # does it still exist?
        if not os.path.exists(filename):
            log.error('File %s does not exist, skipping...')
            return False

        # get fits header and init db object
        log.info('Extracting FITS header information...')
        header = fits.getheader(filename)
        data = self._from_fits_header(header)

        # create path in archive
        night_obs = Time.night_obs(self.observer)
        tel = header['TELESCOP'].lower()

        # set path
        data['path'] = os.path.join(self.config['archive_path'], tel, night_obs.strftime("%Y%m%d"), 'raw')
        data['filename'] = basename[:basename.index('.')]

        # objid and expid
        m = re.search(r'[a-z]+([0-9]{8}[N|S]-[0-9]{4})EXP([0-9]{4})', basename)
        if m:
            data['objid'] = m.group(1)
            data['expid'] = m.group(2)
            log.info('Found objid %s with expid %s.', data['objid'], data['expid'])
        else:
            log.error('Could not extract objid and expid from filename.')
            return False

        # copy to archive
        data['transfered'] = 1

        # open DB
        with DB(*self._db_connect, use_unicode=True, charset="utf8") as db:
            with db as cur:
                # check db
                sql = 'SELECT COUNT(objid) FROM ' + self.config['stella_table'] + ' WHERE objid=%s AND expid=%s'
                cur.execute(sql, (data['objid'], data['expid']))
                if cur.fetchone()[0] > 0:
                    log.warning('Found existing STELLA DB entry for this image. Deleting...')
                    sql = 'DELETE FROM ' + self.config['stella_table'] + ' WHERE objid=%s AND expid=%s'
                    cur.execute(sql, (data['objid'], data['expid']))

                # build sql and add to database
                log.info('Inserting image into STELLA database...')
                sql = 'INSERT INTO ' + self.config['stella_table'] + ' SET ' + ', '.join([c + '=%s' for c in data.keys()])
                values = list(data.values())
                cur.execute(sql, values)
                db.commit()

        # finished
        log.info('Finished processing image.')
        return True

    @staticmethod
    def _from_fits_header(header: fits.Header) -> dict:
        """Get data from FITS header.

        Args:
            header: Header to get data from.

        Returns:
            Data from header.
        """

        # create new data dict
        data = {}

        # dates
        if 'DATE-OBS' in header:
            data['dateobs'] = Time(header['DATE-OBS']).iso
        else:
            raise ValueError('Could not find DATE-OBS in FITS header.')
        data['date'] = Time.now().iso

        # telescope and instrument
        if 'TELESCOP' in header and header['TELESCOP'] in TELESCOPES_DICT:
            data['telescop'] = TELESCOPES_DICT[header['TELESCOP']]
        else:
            log.warning('Missing or invalid TELESCOP in FITS header.')
        if 'INSTRUME' in header and header['INSTRUME'] in INSTRUMENTS_DICT:
            data['instrument'] = INSTRUMENTS_DICT[header['INSTRUME']]
        else:
            log.warning('Missing or invalid INSTRUME in FITS header.')
        if 'IMAGETYP' in header and header['IMAGETYP'] in IMAGE_TYPES_VALUES_DICT:
            data['imagetyp'] = IMAGE_TYPES_VALUES_DICT[header['IMAGETYP']]
        else:
            log.warning('Missing or invalid IMAGETYP in FITS header.')

        # binning
        if 'XBINNING' in header and 'YBINNING' in header:
            data['binning'] = '%dx%d' % (header['XBINNING'], header['YBINNING'])
        else:
            log.warning('Missing or invalid XBINNING and/or YBINNING in FITS header.')

        # position
        if 'OBJRA' in header and 'OBJDEC' in header:
            data['ra'] = header['OBJRA']
            data['decl'] = header['OBJDEC']
            ra = math.radians(data['ra'])
            dec = math.radians(data['decl'])
            data['targetx'] = math.cos(dec) * math.cos(ra)
            data['targety'] = math.cos(dec) * math.sin(ra)
            data['targetz'] = math.sin(dec)

        # other stuff
        data['observer'] = 0
        for column, key in MAPPING_DICT.items():
            if key in header:
                data[column] = header[key]
            else:
                log.info('Missing or invalid %s in FITS header.', key)

        # finished
        return data


__all__ = ['MonetImageWatcher']
