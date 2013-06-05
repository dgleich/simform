""" Parse errors for simform. """
import sys
import json
import numpy
import pprint

output = sys.stdin
outmap = {}
for line in output:
    parts = line.split('\t')
    r = int(parts[0])
    t = int(parts[1])
    noderange = parts[2]
    for rankresults in parts[3:]:
        rankresults = json.loads(rankresults)
        rank = int(rankresults[0])
        errs = numpy.array(rankresults[1])
        if rank in outmap:
            outmap[rank] += errs
        else:
            outmap[rank] = errs

R = max(outmap.iterkeys())
npreds = outmap.values()[0].shape[0]

for j in xrange(npreds):
    for r in xrange(R):
        errs = outmap[r+1]
        print '%.18e'%(errs[j]),
    print '\n',
    

