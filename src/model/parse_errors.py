""" Parse errors for simform. """
import sys
import math
import json
import numpy
import pprint

output = sys.stdin
outmap = {}
outnorms = {}
for line in output:
    parts = line.split('\t')
    r = int(parts[0])
    t = int(parts[1])
    noderange = parts[2]
    for rankresults in parts[3:]:
        rankresults = json.loads(rankresults)
        rank = int(rankresults[0])
        errs = numpy.array(rankresults[1])
        norms = numpy.array(rankresults[2])
        
        if rank in outmap:
            outmap[rank] += errs
            outnorms[rank] += norms
        else:
            outmap[rank] = errs
            outnorms[rank] = norms

R = max(outmap.iterkeys())
npreds = outmap.values()[0].shape[0]

for j in xrange(npreds):
    for r in xrange(R):
        errs = outmap[r+1]
        norms = outnorms[r+1]
        print '%.18e'%(math.sqrt(errs[j])/math.sqrt(norms[j])),
    print '\n',
    

