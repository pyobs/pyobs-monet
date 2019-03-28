import logging

from pyobs import PyObsModule
from pyobs.interfaces import IFitsHeaderProvider, ICamera

log = logging.getLogger(__name__)


class MasterMind(PyObsModule, IFitsHeaderProvider):
    def __init__(self, *args, **kwargs):
        PyObsModule.__init__(self, *args, **kwargs)
        self._objid = 1000
        self._expid = 0
        self._object = None

    def run(self):
        # get camera
        camera = self.comm['camera']    # type: ICamera, Proxy

        # take 10 bias images
        self._object = 'bias'
        for self._expid in range(10):
            # take image
            log.info('Take image %d...', self._expid)
            camera.expose(0, ICamera.ImageType.BIAS)

    def get_fits_headers(self, *args, **kwargs) -> dict:
        # build obj id
        objid = self.environment.night_obs().strftime('%Y%m%d') + 'S-%04d' % self._objid
        # return it
        return {
            'OBJID': objid,
            'EXPID': self._expid,
            'OBJECT': self._object
        }
