import logging
import os
import urllib.request
from tqdm import tqdm

logging.basicConfig(level=logging.DEBUG,
                    format="%(levelname)-9s %(name)s.%(funcName)s:%(lineno)d > %(message)s")
logger = logging.getLogger(__name__)

# list of files to download
bag_files = [
    "H13071_MB_VR_Ellipsoid.bag",
    "H13071_MB_VR_MLLW.bag",
]

server_url = r"https://bitbucket.org/ccomjhc/openns/downloads/"


class DownloadProgressBar(tqdm):
    def update_to(self, b=1, bsize=1, tsize=None):
        if tsize is not None:
            self.total = tsize
        self.update(b * bsize - self.n)


def download_url(url, output_path):
    with DownloadProgressBar(unit='B', unit_scale=True,
                             miniters=1, desc=url.split('/')[-1]) as t:
        urllib.request.urlretrieve(url, filename=output_path, reporthook=t.update_to)


# create/retrieve the local test/output folder (for outputs)

test_data_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), "data"))
if not os.path.exists(test_data_folder):
    os.mkdir(test_data_folder)
logger.info("test output folder: %s" % test_data_folder)

# for each BAG file, first remove it (if present) then download from the server

for bag_file in bag_files:
    data_src = os.path.join(server_url, bag_file)
    data_dst = os.path.abspath(os.path.join(test_data_folder, bag_file))
    logger.info("downloading %s to %s" % (data_src, data_dst))

    if os.path.exists(data_dst):
        try:
            os.remove(data_dst)
        except Exception as e:
            logger.error('while removing %s: %s' % (data_dst, e))

    try:
        download_url(data_src, data_dst)
    except Exception as e:
        logger.error('while downloading %s: %s' % (data_src, e))
