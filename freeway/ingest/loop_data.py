import gzip, time

def parse5MinFile(filename):
    """    
    (1852985928, # timestamp from file
    [{'delay': '0', 'pct_observed': '100', 'vht': '2.2', 'flow': '227',
     'occupancy': '.0358', 'q': '69.3', 'travel_time': '',
     'vds_id': '401186', 'num_samples': '50', 'vmt': '150.955',
     'speed': '69.3'}])

    docs at:
    http://pems.dot.ca.gov/?dnode=Help&content=help_var&tab=var_fmt

    Column	Description
    VDS_ID	Unique station identifier
    FLOW	Flow (vehicles/5-minutes)
    OCCUPANCY	Average occupancy as a percentage (0 - 1)
    SPEED	Flow-weighted average of lane speeds
    VMT	Total vehicle miles traveled over this section of freeway
    VHT	Total vehicle hours traveled over this section of freeway
    Q	Measure of freeway quality (VMT/VHT)
    TRAVEL_TIME	Not in use
    DELAY	Vehicle hours of delay
    NUM_SAMPLES	# of samples received in the 5-minute period
    PCT_OBSERVED	Percentage of individual lane points from working detectors that were rolled into the station\'s 5-minute values.
    
    """
    txt = gzip.open(filename).read()
    lines = txt.splitlines()
    timestamp = int(time.mktime(time.strptime(lines[0], "%m/%d/%Y %H:%M:%S")))
    
    fields = ['vds_id', 'flow', 'occupancy', 'speed', 'vmt', 'vht',
              'q', 'travel_time', 'delay', 'num_samples', 'pct_observed']
    result = []
    for line in lines[1:]:
        fields = line.split(',')
        d = {'vds_id'      : fields[0],
             'num_samples' : int(fields[9]),
             'pct_observed' : float(fields[10]),
             }
        if fields[1]: d['flow'] = float(fields[1])
        if fields[2]: d['occupancy'] = float(fields[2])
        if fields[3]: d['speed'] = float(fields[3])
        if fields[4]: d['vmt'] = float(fields[4])
        if fields[5]: d['vht'] = float(fields[5])
        if fields[6]: d['q'] = float(fields[6])
        if fields[8]: d['delay'] = float(fields[8])

        result.append(d)

    return timestamp, result


if __name__ == '__main__':
    import pprint
    pprint.pprint(parse5MinFile("5minagg_latest.txt.gz")[:3])
