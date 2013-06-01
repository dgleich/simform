"""
MapReduce job 

python mr_predict2_hadoop.py \
hdfs://icme-hadoop1.localdomain/user/yangyang/dbsmall2/train_3/p* \
-r hadoop --no-output -o dbsmall2/predict --weights=weights.txt --file weights.txt \
--subset=3,7,11,15,19,23,27,31,35,39,43,47,51,55,59
"""

__author__ = 'Yangyang Hou <hyy.sun@gmail.com>'

import sys
import os
from mrjob.job import MRJob

from numpy import *

class MRPredictwithSVD(MRJob):

    STREAMING_INTERFACE = MRJob.STREAMING_INTERFACE_TYPED_BYTES
    
    def configure_options(self):
        """Add command-line options specific to this script."""
        super(MRPredictwithSVD, self).configure_options()
        
        self.add_passthrough_option(
            '--weights', dest='weights',
            help='--weights FILE, the file of interpolation weights with SVD'       
        )
        
        self.add_passthrough_option(
            '--subset', dest='subset',
            help='--subset, the test subset of parameters for prediction'       
        )
    
       
    def load_options(self, args):
        super(MRPredictwithSVD, self).load_options(args)
            
        if self.options.weights is None:
            self.option_parser.error('You must specify the --weights FILE')
        else:
            self.weights = self.options.weights
            
        if self.options.subset is None:
            self.option_parser.error('You must specify the --subset')
        else:
            self.subset = self.options.subset
    

    def mapper(self, key, value):     
        """
        input:  (r,t,noderange), value array
        output: (r,t,p),(node, val, err)
        """
       
        W=loadtxt(os.path.basename(self.weights)) # interpolation weights
        subset = [int(_) for _ in self.subset.split(',')]
        
        r = key[0]
        t = key[1]
        noderange = key[2]
        nbegin = noderange[0]
        nend = noderange[1]
        
        numNodes = nend - nbegin
        numParasubset = value.size / numNodes
        #print >>sys.stderr, numNodes, numParasubset #should be 33153, 16
        
        for i, weight in enumerate(W):
            K = weight[0] # number of left singular vectors to compute the interpolation
            val = 0
            err = 0
            for k in range(numNodes):
                for j in range(K):
                    val += value[k][j]*weight[j+1]
                    #print >>sys.stderr, j
                for j in range (K,numParasubset):
                    err += (value[k][j]*weight[j+1])*(value[k][j]*weight[j+1])
                    #print >>sys.stderr, j
                    
                yield (r,t,subset[i]), (k+nbegin, val, err)
                
        
    def reducer(self, key, values):
        """
        input: (r,t,p), (node, val, err)
        output:(r,t,p), (valarray, errarray)

        2013-04-29
        The reduce groups all values and variances from a single time-step of
        a simulation via their node id so that we don't have to store the
        node id explicitly anymore and can just store an array instead.

        """
        
        val_order = {}
        err_order = {}
        
        for i, value in enumerate(values):
            val_order[value[0]]=value[1]
            err_order[value[0]]=value[2]
            
        val = [ ]  
        for k,value in sorted(val_order.iteritems()):
            val.append(value)
        val = array(val)
        
        err = [ ]  
        for k,value in sorted(err_order.iteritems()):
            err.append(value)
        err = array(err)
        
        #print >>sys.stderr, key,val
        yield key, (val, err)

if __name__ == '__main__':
    MRPredictwithSVD.run()