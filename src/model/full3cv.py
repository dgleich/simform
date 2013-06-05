#!/usr/bin/env dumbo

"""
Full TSQR algorithm for MapReduce (part 2)

Austin R. Benson (arbenson@stanford.edu)
David F. Gleich
Copyright (c) 2012
"""

import mrmc
import dumbo
import util
import os
import full

# create the global options structure
gopts = util.GlobalOptions()

def runner(job):
    subset = gopts.getstrkey('subset')
    subset = [int(_) for _ in subset.split(',')]
        
    test_subset = gopts.getstrkey('test_subset')
    test_subset = [int(_) for _ in test_subset.split(',')]
    
    q2path = gopts.getstrkey('q2path')
    upath = gopts.getstrkey('upath')
    ppath = gopts.getstrkey('ppath')
    sigmapath = gopts.getstrkey('sigmapath')
    vtpath = gopts.getstrkey('vtpath')
            
    mapper = full.FullTSQRROMCV(q2path, upath, ppath, subset, test_subset, 
                        sigmapath, vtpath)
    reducer = mrmc.ID_REDUCER
    job.additer(mapper=mapper,reducer=reducer,opts=[('numreducetasks',str(0))])
    
def handle_file_opt(prog,fileopt):
    filepath = prog.delopt(fileopt)
    if not filepath:
        return "'%s' not specified"%(fileopt)
    prog.addopt('file', os.path.join(os.path.dirname(__file__), filepath))    
    gopts.getstrkey(fileopt, filepath)

def starter(prog):
    # set the global opts
    gopts.prog = prog

    mat = mrmc.starter_helper(prog, True)
    if not mat: return "'mat' not specified"

    matname,matext = os.path.splitext(mat)
    output = prog.getopt('output')
    if not output:
        prog.addopt('output','%s-full-tsqr-3%s'%(matname,matext))

    handle_file_opt(prog, 'q2path')
    handle_file_opt(prog, 'ppath')
    handle_file_opt(prog, 'sigmapath')
    handle_file_opt(prog, 'vtpath')
    handle_file_opt(prog, 'upath')
    
    gopts.getstrkey('subset','-1')
    gopts.getstrkey('test_subset','-1')
    
 
    gopts.save_params()

if __name__ == '__main__':
    dumbo.main(runner, starter)

