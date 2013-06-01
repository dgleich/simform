"""
MapReduce job to compute 2norm errors 


Example:

python mr_error_hadoop.py \
hdfs://icme-hadoop1.localdomain/user/yangyang/dbsmall2/data.seq/random_media/random_media*.seq \
hdfs://icme-hadoop1.localdomain/user/yangyang/dbsmall2/predict/p* \
-r hadoop --no-output -o dbsmall2/error --numParas 64  --variable TEMP \
--subset=3,7,11,15,19,23,27,31,35,39,43,47,51,55,59
"""

__author__ = 'Yangyang Hou <hyy.sun@gmail.com>'

import sys
import os

from math import sqrt
from mrjob.job import MRJob
from numpy import *

class MRErrorNorm(MRJob):

    STREAMING_INTERFACE = MRJob.STREAMING_INTERFACE_TYPED_BYTES
    
    def configure_options(self):
        """Add command-line options specific to this script."""
        super(MRErrorNorm, self).configure_options()
        
        self.add_passthrough_option(
            '--variable', dest='variable',
            help='--variable VAR, the variable need to compute global variance'       
        )
        
        self.add_passthrough_option(
            '--numParas', dest='numParas',
            help='--numParas INT, the number of parameters'       
        )
        
        self.add_passthrough_option(
            '--subset', dest='subset',
            help='--subset, Get the subsets of parameters for error estimation'       
        )
        
       
    def load_options(self, args):
        super(MRErrorNorm, self).load_options(args)
        
        if self.options.variable is None:
            self.option_parser.error('You must specify the --variable VAR')
        else:
            self.variable = self.options.variable
            
        if self.options.numParas is None:
            self.option_parser.error('You must specify the --numParas INT')
        else:
            self.numParas = self.options.numParas
            
        if self.options.subset is None:
            self.option_parser.error('You must specify the --subset for error estimate')
        else:
            self.subset = self.options.subset
            
    def twonorm(self, vec):
        # return the square of 2-norm of the vector
        n = len(vec)
        norm = 0
        for i in xrange(n):
            tmp = vec[i]
            norm = tmp*tmp + norm
        return norm
    
    def mapper(self, key, value):
        """
        input: 
          key = -1 => xcoord data
          key = -2 => ycoord data
          key = -3 => zcoord data
          key = (fset, timestep) => value = array
          key = (r, t, p) => value = (valarray, errarray)
        output: (r, t, p), valuearray
        """
        subset = [int(_) for _ in self.subset.split(',')]
        #print >>sys.stderr, key
        if (key == -1) or (key == -2) or (key == -3) :
            pass # ignore coordinate (x,y,z) data
        elif len(key) == 2: # files form original sequence files
            fset = int(key[0])
            p = fset%int(self.numParas)
            if p == 0:
                p = int(self.numParas)
                r = fset/int(self.numParas)
            else:
                r = fset/int(self.numParas) + 1
            t = key[1][0]
            if p not in subset:
                pass
            else:
                outputkey = (r,t,p)
                for i, var in enumerate(value):
                    name = var[0]
                    if name == self.variable:
                        outputvalue = array(var[1])
                #print >>sys.stderr, 'key', outputkey
                #print >>sys.stderr, 'value', outputvalue
                yield outputkey, outputvalue     
        elif len(key) == 3: # files from prediction sequence files
            p = key[2]
            if p not in subset:
                pass
            else:
                yield key, value[0] # ignore err array
        else:
            pass
            
    def reducer(self, key, values): 
        """ 
        input: (r,t,p), [v1,v2]
        output: (r,t,p), errnorm
        """
        ind = 0
        for i, val in enumerate(values):
            if ind == 0:
                err = array(val)
            else:
                err = array(val) - err
            ind  = ind + 1
        
        errnorm = self.twonorm(err)
        yield key, errnorm
            
    def steps(self):
        return [self.mr(self.mapper, self.reducer),]

if __name__ == '__main__':
    MRErrorNorm.run()
