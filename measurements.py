
from loop_data import parse5MinFile
from detector import parseVdsConfig
def getMeasurements():
    """yields dicts with all the attributes from the vds and from
    the sensor data file"""
    vds = parseVdsConfig("vds_config.xml")
    for sens in parse5MinFile("5minagg_latest.txt.gz"):
        try:
            sens.update(vds[sens['vds_id']])
            yield sens
        except KeyError:
            print "no vds data for vds_id %s" % sens['vds_id']
