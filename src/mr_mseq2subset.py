"""
Extract a subset of rows from an mseq2 file. Write it out as a standard
mseq file.

Example:

python mr_mseq2subset_hadoop.py <input> \
-r hadoop --no-output -o dbsmall/data.mseq4 \

"""

__author__ = 'David F. Gleich'

import sys
import os

from mrjob.job import MRJob
import numpy

class MRMSeq2Subset(MRJob):

    STREAMING_INTERFACE = MRJob.STREAMING_INTERFACE_TYPED_BYTES
    
    def configure_options(self):
        """Add command-line options specific to this script."""
        super(MRMSeq2Subset, self).configure_options()
      
        self.add_file_option(
            '--subset', dest='subset_filename',
            help='--subset FILE, the file with a list of 0-indexed nodal variables to extract')
        
        self.add_passthrough_option(
            '--timestep', dest='timestep', type='int',
            help='--timestep INT, the 0-indexed time-step index')         
       
    def load_options(self, args):
        super(MRMSeq2Subset, self).load_options(args)

            
        if self.options.subset_filename is None:
            self.option_parser.error('You must specify --subset FILE')
        
        if self.options.timestep is None:
            self.option_parser.error('You must specify --timestep INT')
        
    def mapper_init(self):
        self.subset = numpy.loadtxt(self.options.subset_filename,dtype=int)
        def isNonDecreasing(l):
            """ from http://stackoverflow.com/questions/3755136/pythonic-way-to-check-if-a-list-is-sorted-or-not """
            for i, el in enumerate(l[1:]):
                if el < l[i]: # if the ith element is smaller than the previous element
                    return False
            return True
        assert isNonDecreasing(self.subset), \
            "the subset must be sorted in increasing order"
        
        self.subsetset = set(self.subset)
        
        print >>sys.stderr, "Looking for keys with timestep ", self.options.timestep
    
    def in_subset(self, nodestart, nodeend):
        for index in self.subset:
            if index >= nodestart and index < nodeend:
                return True
        return False
    
    def mapper(self, key, value):
        """
        input: (r,t,noderange), numpy-array(len(noderange)xncols)
        """
        
        r = key[0]
        t = key[1]
        
        if t != self.options.timestep:
            print >>sys.stderr, "Ignoring data with key (due to timestep) " , key
            return
        
        noderange = key[2]
        nodestart,nodeend = noderange[0], noderange[1]
        
        if not self.in_subset(nodestart, nodeend):
            print >>sys.stderr, "Ignoring data with key (due to range) ", key
            return
            
        
        for i,nodeid in enumerate(xrange(nodestart, nodeend)):
            if nodeid in self.subsetset:
                yield (r, t, nodeid), value[i,:].tolist()

        
    def steps(self):
        return [self.mr(mapper_init = self.mapper_init, 
            mapper = self.mapper)]

if __name__ == '__main__':
    MRMSeq2Subset.run()
