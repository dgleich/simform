"""
MapReduce job to compute global variance of a series of simulation runs for all node space.

Example:

python mr_seq2mseq2_hadoop.py \
hdfs://icme-hadoop1.localdomain/user/yangyang/dbsmall/data.seq/random_media/random_media*.seq \
-r hadoop --no-output -o dbsmall/data.mseq4 --variable TEMP --numParas 64

"""

__author__ = 'Yangyang Hou <hyy.sun@gmail.com>'

import sys
import os

from mrjob.job import MRJob
from numpy import *

class MRSeq2Mseq(MRJob):

    STREAMING_INTERFACE = MRJob.STREAMING_INTERFACE_TYPED_BYTES
    
    def configure_options(self):
        """Add command-line options specific to this script."""
        super(MRSeq2Mseq, self).configure_options()
        
        self.add_passthrough_option(
            '--variable', dest='variable',
            help='--variable VAR, the variable need to compute global variance'       
        )
        
        self.add_passthrough_option(
            '--numParas', dest='numParas',
            help='--numParas INT, the number of parameters'       
        )
       
    def load_options(self, args):
        super(MRSeq2Mseq, self).load_options(args)
            
        if self.options.variable is None:
            self.option_parser.error('You must specify the --variable VAR')
        else:
            self.variable = self.options.variable
            
        if self.options.numParas is None:
            self.option_parser.error('You must specify the --numParas INT')
        else:
            self.numParas = self.options.numParas
    
    def mapper(self, key, value):
        """
        input: 
          key = -1 => xcoord data
          key = -2 => ycoord data
          key = -3 => zcoord data
          key = (fset, timestep) => value = array
        output: (r,timestep,node), (param,value)
        """
        # ignore coordinate (x,y,z) data
        if (key != -1) and (key != -2) and (key != -3) : 
            for i, var in enumerate(value):
                name = var[0]
                if name == self.variable:
                    fset = int(key[0])
                    p = fset%int(self.numParas)
                    if p == 0:
                        p = int(self.numParas)
                        r = fset/int(self.numParas)
                    else:
                        r = fset/int(self.numParas) + 1
                    p = (p-1)/4 + 1 # mseq5 convert to special form
                    t = key[1][0]
                    
                    n = 129
                    fdata = var[1] # extract the field data
                    fdata = array(fdata)
                    numNodes = len(fdata)
                    nvars = numNodes/n # there are nvars elements in each of the n groups
                    
                    ngroups = numNodes/nvars
                    if numNodes%nvars > 0:
                        ngroups += 1 # add one group if we don't evenly split everything
                    
                    for j in xrange(ngroups):
                        nodes = (j*nvars, min((j+1)*nvars,numNodes)) # note that the range is +1 upper range
                        yield (r, t, nodes), (p, fdata[nodes[0]:nodes[1]])
            

    def reducer(self, key, values): 
        """ 
        Each key is a (r, timestep, nodes) pair where nodes is a range.
        Each value is (param, values) pair where values is an array with the 
          value for each node in nodes.
          
        We want to make an array of all the values over the parameters. 
        """
        nodes = key[2] # extract the range of nodes
        
        # NOTE THE FOLLOWING LINE IS HARD-CODED FOR THIS SINGLE EXAMPLE
        submat = [ [0. for _ in xrange(int(16))] for _ in xrange(nodes[1]-nodes[0]) ]
        
        print >>sys.stderr, "reporter:status:loading values"
        for val in values:
            pi = val[0]
            vals = val[1]
            for j,v in enumerate(vals):
                submat[j][pi-1] = v
        
        print >>sys.stderr, "reporter:status:outputting rows"    
        yield key, array(submat)
        #for i,j in enumerate(xrange(nodes[0],nodes[1])):
            #yield (key[0], key[1], j), list(submat[i])
        
    def steps(self):
        return [self.mr(mapper = self.mapper, reducer = self.reducer)]

if __name__ == '__main__':
    MRSeq2Mseq.run()
