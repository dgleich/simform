#!/usr/bin/env dumbo

"""
Full TSQR algorithm for MapReduce (part 1)

Austin R. Benson (arbenson@stanford.edu)
David F. Gleich
Copyright (c) 2012
"""

import mrmc
import dumbo
import util
import os
import full

# 2013-05-24: dgleich - Added subset option
# 2013-06-05: dgleich - Added saveA option

# create the global options structure
gopts = util.GlobalOptions()

def runner(job):
    subset = gopts.getstrkey('subset')
    if subset == 'all':
        subset = None
    else:
        subset = [int(_) for _ in subset.split(',')]
        
    saveA = gopts.getintkey('saveA')
    mapper = full.FullTSQRMap1(subset=subset,save_A=bool(saveA))
        
    reducer = mrmc.ID_REDUCER
    job.additer(mapper=mapper,reducer=reducer,opts=[('numreducetasks',str(0))])

def starter(prog):
    # set the global opts
    gopts.prog = prog

    mat = mrmc.starter_helper(prog, True)
    if not mat: return "'mat' not specified"
    
    subset = gopts.getstrkey('subset','all')
    if subset != 'all':
        # check parsing
        subset = [int(_) for _ in subset.split(',')]
        # then ignore

    save_A = gopts.getintkey('saveA',0)

    matname,matext = os.path.splitext(mat)
    output = prog.getopt('output')
    if not output:
        prog.addopt('output','%s-full-tsqr1%s'%(matname,matext))
    
    gopts.save_params()

if __name__ == '__main__':
    dumbo.main(runner, starter)
