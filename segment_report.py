"""
diagrams for a particular segment of freeway
"""


from __future__ import division
from nevow import flat, tags as T

def table(updatingMeasurements):
    meas = []
    for m in updatingMeasurements.measurements:
        # (but consider beyond my strip, to see approaching traffic)
        if m['freeway_id'] == '101' and 408 < float(m['abs_pm']) < 412.5:
            meas.append((float(m['abs_pm']), m))

    meas.sort()

    rows = [T.tr[T.th(colspan=4, class_="dir-N")['North'],
                 T.th(colspan=4, class_="dir-S")['South']],
            T.tr[T.th['fwy'], T.th['postmile'], T.th['name'], T.th['speed'],
                 T.th['fwy'], T.th['postmile'], T.th['name'], T.th['speed']]]
    for _, m in meas:
        attr = dict(class_="dir-%s" % m['freeway_dir'])
        chunk = [T.td(**attr)[m['freeway_id'] + m['freeway_dir']],
                 T.td(**attr)[m['abs_pm']],
                 T.td(**attr)[m['name']],
                 T.td(**attr)[m['speed']],
                 ]
        if m['freeway_dir'] == 'N':
            tds = chunk + [T.td(colspan=4)]
        else:
            tds = [T.td(colspan=4)] + chunk
        rows.append(T.tr[tds])

    return T.table[rows]

if __name__ == '__main__':
    from measurements import getMeasurements
    open("segment.html", "w").write(flat.flatten(table(getMeasurements())))
