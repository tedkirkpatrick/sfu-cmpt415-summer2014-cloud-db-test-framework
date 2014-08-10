#!/usr/bin/env python
import sys
import re
import os
sys.path.append("/srv/cal/lib")
from jepsen import Jepsen
overwrite = False
for arg in sys.argv:
	if arg == 'overwrite': overwrite = True

Jepsen("dynamodb",test="basic",props={'wait': 1000,'count': 40,'overwrite': overwrite},threads=5)
