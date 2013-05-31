#!/usr/bin/env dumbo

"""
Dump mseq2 file to a matrix text file for Matlab

David F. Gleich
Copyright (c) 2013
"""

import sys
import mrmc
import dumbo
import os
import full
import numpy

class AssembleMatrixReducer(mrmc.MatrixHandler):
    """ This class outputs the matrix as a list of string values and no keys.
    It's useful for doing a text-file dump of the data."""
    
    def __init__(self):
        mrmc.MatrixHandler.__init__(self)
        self.keys = []
        self.values = []
        self.timesteps = set()
        self.realizations = set()
        self.nnodes = 0
                
    def multicollect(self,key,values):
        """ Collect multiple rows at once with a single key. 
        
        This code only works with key,values that have the form:
        
        (realization, timestep, (node start, node end + 1))
        
        where realization is 1-index based and timestep is zero-index based
        """
        
        r = key[0]
        t = key[1]
        nrange = key[2]
        
        self.nnodes = max(self.nnodes, nrange[1])
        self.timesteps.add(t)
        self.realizations.add(r)
        
        self.keys.append(key)
        self.values.append(values)
        
        for row in values:
            self.nrows += 1
            if self.nrows%50000 == 0:
                self.counters['rows processed'] += 50000
    
    def collect(self,key,value):
        assert false, "not supported by this simple code."

    def close(self):
        self.counters['rows processed'] += self.nrows%50000
        
        ntsteps = len(self.timesteps)
        nrealizations = len(self.realizations)
        
        assert(self.nrows == ntsteps*nrealizations*self.nnodes)
        print >>sys.stderr, "nrows = %i"%(self.nrows)
        print >>sys.stderr, "ncols = %i"%(self.ncols)

        mat = numpy.zeros((self.nrows, self.ncols))
        
        for vi, key in enumerate(self.keys):
            r = key[0]
            t = key[1]
            nrange = key[2]
            
            assert(t < ntsteps) # zero index based
            assert(r <= nrealizations) # one index based
            
            roffset = (r-1)*(ntsteps*self.nnodes)
            
            rowstart = roffset+t*self.nnodes + nrange[0]
            rowend = roffset+t*self.nnodes + nrange[1]
            
            mat[rowstart:rowend] = self.values[vi]
            
        for row in mat:
            yield (" ".join([ '%.18e'%(i) for i in row.tolist()])," ")
        

    def __call__(self,data):
        for key, values in data:
	    self.collect_data(values, key=key)
        for key,val in self.close():
            yield key, val

def runner(job):
    mapper = mrmc.ID_MAPPER
    reducer = AssembleMatrixReducer()
    job.additer(mapper=mapper, reducer=reducer,
                opts=[('numreducetasks', str(1))])

def starter(prog):
    prog.addopt('memlimit','20g') 
    prog.addopt('outputformat','text')
    mat = mrmc.starter_helper(prog, True)
    if not mat: return "'mat' not specified"

    matname,matext = os.path.splitext(mat)
    output = prog.getopt('output')
    if not output:
        prog.addopt('output','%s%s'%(matname,'.mtxt'))
    
    

if __name__ == '__main__':
    dumbo.main(runner, starter)
