import matplotlib.pyplot as plt
import pandas as pd
import numpy as np


def main():
    # max distance 0.5 degrees
    dist = np.radians(1.0)
    print('dist:', dist)

    # read raw data
    raw = pd.read_csv('gaia_raw.csv', index_col=False, comment='#')

    # calculate X/Y/Z
    ra, dec = raw['ra'], raw['dec']
    raw['x'] = np.cos(dec) * np.cos(ra)
    raw['y'] = np.cos(dec) * np.sin(ra)
    raw['z'] = np.sin(dec)

    # get all stars fainter than 7.5
    targets = raw[raw['phot_g_mean_mag'] > 7.5]

    # init output file
    with open('pointing_cat.csv', 'w') as csv:
        csv.write('ra,dec,x,y,z,mag\n')

        # loop all rows
        idx = []
        for i, row in targets.iterrows():
            # get xyz and mag
            x, y, z, mag, source_id = row['x'], row['y'], row['z'], row['phot_g_mean_mag'], row['source_id']

            # calc distance
            raw['dist'] = np.sqrt((x - raw['x'])**2 + (y - raw['y'])**2 + (z - raw['z'])**2)

            # get all stars closer than dist and brighter than star in row + 0.1
            close = raw[(raw['dist'] < dist) & (raw['phot_g_mean_mag'] < mag + 0.1) & (raw['source_id'] != source_id)]
            if len(close) == 0:
                csv.write('%f,%f,%f,%f,%f,%f\n' % (row['ra'], row['dec'], x, y, z, mag))
            if len(close) > 0:
                print(i, close['dist'].values, mag, close['phot_g_mean_mag'].values)


if __name__ == '__main__':
    main()
