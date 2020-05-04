#!/usr/bin/env python
import os, sys, re, glob, logging, json, traceback
import pandas as pd
from datetime import datetime

log_format = "[%(asctime)s: %(levelname)s/%(funcName)s] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)
logger = logging.getLogger('parse_logs')


# compiled regexes
ALKS_RE = re.compile(r'^geocode.Azimuth\s+looks\s+=\s+(\d+)', re.M)
RLKS_RE = re.compile(r'^geocode.Range\s+looks\s+=\s+(\d+)', re.M)
EAST_RE = re.compile(r'^geocode.East\s+=\s+([-+]?\d*\.\d+|\d+)', re.M)
WEST_RE = re.compile(r'^geocode.West\s+=\s+([-+]?\d*\.\d+|\d+)', re.M)
NORTH_RE = re.compile(r'^geocode.North\s+=\s+([-+]?\d*\.\d+|\d+)', re.M)
SOUTH_RE = re.compile(r'^geocode.South\s+=\s+([-+]?\d*\.\d+|\d+)', re.M)
LENGTH_RE = re.compile(r'^geocode.Length\s+=\s+(\d+)', re.M)
WIDTH_RE = re.compile(r'^geocode.Width\s+=\s+(\d+)', re.M)
FILTER_TIME_RE = re.compile(r'^(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2},\d{3}) - isce.mroipac.filter - INFO - Filtering interferogram', re.M)
GEOCODING_TIME_RE = re.compile(r'^(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2},\d{3}) - isce.topsinsar.runGeocode - INFO - Geocoding Image', re.M)
ASC_NODE_TIME_MASTER_RE = re.compile(r'^master.sensor.ascendingnodetime\s+=\s+(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}\.\d+)', re.M)
ASC_NODE_TIME_SLAVE_RE = re.compile(r'^slave.sensor.ascendingnodetime\s+=\s+(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}\.\d+)', re.M)


def parse(isce_log_file):
    """Parse log file and return dict."""

    # get dataset id
    id = os.path.basename(os.path.dirname(isce_log_file))

    # read in isce log
    with open(isce_log_file) as f:
        isce_log = f.read()

    # parse out info
    alks = int(ALKS_RE.search(isce_log).group(1))
    logger.debug("alks: {}".format(alks))
    rlks = int(RLKS_RE.search(isce_log).group(1))
    logger.debug("rlks: {}".format(rlks))
    east = float(EAST_RE.search(isce_log).group(1))
    logger.debug("east: {}".format(east))
    west = float(WEST_RE.search(isce_log).group(1))
    logger.debug("west: {}".format(west))
    north = float(NORTH_RE.search(isce_log).group(1))
    logger.debug("north: {}".format(north))
    south = float(SOUTH_RE.search(isce_log).group(1))
    logger.debug("south: {}".format(south))
    length = int(LENGTH_RE.search(isce_log).group(1))
    logger.debug("length: {}".format(length))
    width = int(WIDTH_RE.search(isce_log).group(1))
    logger.debug("width: {}".format(width))
    filt_start_dt = datetime.strptime(FILTER_TIME_RE.search(isce_log).group(1), '%Y-%m-%d %H:%M:%S,%f')
    logger.debug("filt_start_dt: {}".format(filt_start_dt))
    geocoding_start_dt = datetime.strptime(GEOCODING_TIME_RE.search(isce_log).group(1), '%Y-%m-%d %H:%M:%S,%f')
    logger.debug("geocoding_start_dt: {}".format(geocoding_start_dt))
    filter_geo_delta_secs = (geocoding_start_dt - filt_start_dt).seconds
    logger.debug("filter_geo_delta_secs: {}".format(filter_geo_delta_secs))
    master_asc_node_time = datetime.strptime(ASC_NODE_TIME_MASTER_RE.search(isce_log).group(1), '%Y-%m-%d %H:%M:%S.%f')
    logger.debug("master_asc_node_time: {}".format(master_asc_node_time))
    slave_asc_node_time = datetime.strptime(ASC_NODE_TIME_SLAVE_RE.search(isce_log).group(1), '%Y-%m-%d %H:%M:%S.%f')
    logger.debug("slave_asc_node_time: {}".format(slave_asc_node_time))
    return {
        "id": id,
        "master_asc_node_time": master_asc_node_time.isoformat(),
        "slave_asc_node_time": slave_asc_node_time.isoformat(),
        "filt_start_dt": filt_start_dt.isoformat(),
        "geocoding_start_dt": geocoding_start_dt.isoformat(),
        "filter_geo_delta_secs": filter_geo_delta_secs,
        "length": length,
        "width": width,
        "east": east,
        "west": west,
        "south": south,
        "north": north,
        "alks": alks,
        "rlks": rlks,
        "lat": south + abs((north-south)/2),
        "lon": west + abs((east-west)/2),
    }


def crawl(path):
    """Crawl and look for isce.log files."""

    for root, dirs, files in os.walk(path, followlinks=True):
        files.sort()
        dirs.sort()
        for file in files:
            if file == "isce.log":
                yield os.path.join(root, file)
def main(path):
    """Main."""

    all_data = []
    for i in crawl(path):
        try: all_data.append(parse(i))
        except Exception as e:
            logger.error("Got error parsing {}:".format(i))
            logger.error(traceback.format_exc())
    df = pd.DataFrame(all_data)
    df.set_index('id', inplace=True)
    print(df)
    df.to_csv('isce_log.csv')


if __name__ == "__main__":
    isce_log_file = sys.argv[1]
    main(isce_log_file)
