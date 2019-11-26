import logging

from pyobs import PyObsModule
from pyobs.interfaces import IFitsHeaderProvider, ICamera
from pyobs.utils.time import Time

log = logging.getLogger(__name__)


class MasterMind(PyObsModule, IFitsHeaderProvider):
    def __init__(self, *args, **kwargs):
        PyObsModule.__init__(self, *args, **kwargs)
        self._objid = 1000
        self._expid = 0
        self._object = None

    def run(self):
        # get camera
        camera = self.comm['camera']    # type: (ICamera, Proxy)

        # take 10 bias images
        self._object = 'bias'
        for self._expid in range(10):
            # take image
            log.info('Take image %d...', self._expid)
            camera.expose(0, ICamera.ImageType.BIAS)

    def get_fits_headers(self, namespaces: list = None, *args, **kwargs) -> dict:
        """Returns FITS header for the current status of this module.

        Args:
            namespaces: If given, only return FITS headers for the given namespaces.

        Returns:
            Dictionary containing FITS headers.
        """

        # build obj id
        objid = Time.now().night_obs(self.observer).strftime('%Y%m%d') + 'S-%04d' % self._objid

        # return it
        return {
            'OBJID': objid,
            'EXPID': self._expid,
            'OBJECT': self._object
        }
