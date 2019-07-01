import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

# load data
data = pd.read_csv('pointing_cat.csv', index_col=False)

# plot it
fig = plt.figure(figsize=(16, 8))
ax = fig.add_subplot(111, projection="mollweide")
ax.scatter(np.radians(data['ra'] - 180), np.radians(data['dec']), c=data['mag'])
plt.show()
