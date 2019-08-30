import time

from pybloom import ScalableBloomFilter

import read_data
import numpy as np
from clustering import Emd

column1 = read_data.data['C_CustKey']
column2 = read_data.data['AC_CustKey']

start = time.time()
i = Emd.bloom_filter_intersection(column1, column2)
end = time.time()

print(end-start)
# 235.01 string
# 4.89 numeric

start = time.time()
ii = Emd.intersection_emd(column1, column2)
end = time.time()

print(end-start)
# 243.63 string
# 0.31 numeric
