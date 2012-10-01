#!/usr/bin/env dumbo

"""
MapReduce matrix computations.  This contains the basic building blocks.

Austin R. Benson (arbenson@stanford.edu)
David F. Gleich
Copyright (c) 2012
"""

import sys
import os
import time
import random
import struct

import numpy
import numpy.linalg

import util
import dumbo
import dumbo.backends.common

from dumbo import opt

# some variables
ID_MAPPER = 'org.apache.hadoop.mapred.lib.IdentityMapper'
ID_REDUCER = 'org.apache.hadoop.mapred.lib.IdentityReducer'

class DataFormatException(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)


def starter_helper(prog, use_full=False):
    print 'running starter!'

    mypath = os.path.dirname(__file__)
    print 'my path: ' + mypath    

    prog.addopt('file', os.path.join(mypath, 'util.py'))
    prog.addopt('file', os.path.join(mypath, 'mrmc.py'))
    if use_full:
        prog.addopt('file', os.path.join(mypath, 'full.py'))

    splitsize = prog.delopt('split_size')
    if splitsize is not None:
        prog.addopt('jobconf',
            'mapreduce.input.fileinputformat.split.minsize=' + str(splitsize))

    prog.addopt('overwrite', 'yes')
    prog.addopt('jobconf', 'mapred.output.compress=true')
    prog.addopt('memlimit', '8g')

    mat = prog.delopt('mat')
    if mat:
        # add numreps copies of the input
        numreps = prog.delopt('repetition')
        if not numreps:
            numreps = 1
        for i in range(int(numreps)):
            prog.addopt('input',mat)
    
        return mat            
    else:
        return None


def add_splay_iteration(job, part):
    nreducers = int(part[1:])
    # these tasks should just spray data and compress
    opts = [('numreducetasks', str(nreducers))]
    job.additer(mapper=ID_MAPPER, reducer=ID_REDUCER, opts=opts)
    job.additer(mapper, reducer, opts=opts)


"""
MatrixHandler reads data and collects it
"""
class MatrixHandler(dumbo.backends.common.MapRedBase):
    def __init__(self):
        self.ncols = None
        self.unpacker = None
        self.nrows = 0
        self.deduced = False

    def collect(self, key, value):
        pass

    def collect_data_instance(self, key, value):
        if isinstance(value, str):
            if not self.deduced:
                self.deduced = self.deduce_string_type(value)
                # handle conversion from string
            if self.unpacker is not None:
                value = self.unpacker.unpack(value)
            else:
                value = [float(p) for p in value.split()]

        if self.ncols == None:
            self.ncols = len(value)
            print >>sys.stderr, 'Matrix size: %i columns' % (self.ncols)
        if len(value) != self.ncols:
            raise DataFormatException(
                'Length of value did not match number of columns')
        self.collect(key, value)        

    def collect_data(self, data, key=None):
        if key == None:
            for key, value in data:
                self.collect_data_instance(key, value)
        else:
            for value in data:
                self.collect_data_instance(key, value)

    def deduce_string_type(self, val):
        # first check for TypedBytes list/vector
        try:
            [float(p) for p in val.split()]
        except:
            if len(val) == 0:
                return False
            if len(val) % 8 == 0:
                ncols = len(val) / 8
                # check for TypedBytes string
                try:
                    val = list(struct.unpack('d' * ncols, val))
                    self.unpacker = struct.Struct('d' * ncols)
                    return True
                except struct.error, serror:
                    # no idea what type this is!
                    raise DataFormatException('Data format type is not supported.')
            else:
                raise DataFormatException('Number of data bytes (%d)' % len(val)
                                          + ' is not a multiple of 8.')
        return True


"""
Serial TSQR
"""
class SerialTSQR(MatrixHandler):
    def __init__(self, blocksize=3, isreducer=False, isfinal=False):
        MatrixHandler.__init__(self)
        self.blocksize = blocksize
        self.isreducer = isreducer
        self.data = []
        self.isfinal = isfinal
    
    def QR(self):
        A = numpy.array(self.data)
        return numpy.linalg.qr(A,'r')
    
    def compress(self):
        # Compute a QR factorization on the data accumulated so far.
        if self.ncols is None or len(self.data) < self.ncols:
            return

        t0 = time.time()
        R = self.QR()
        dt = time.time() - t0
        self.counters['numpy time (millisecs)'] += int(1000 * dt)

        # reset data and re-initialize to R
        self.data = []
        for row in R:
            self.data.append(util.array2list(row))
                        
    def collect(self, key, value):
        self.data.append(value)
        self.nrows += 1
        
        if len(self.data) > self.blocksize * self.ncols:
            self.counters['QR Compressions'] += 1
            # compress the data
            self.compress()
            
        # write status updates so Hadoop doesn't complain
        if self.nrows % 50000 == 0:
            self.counters['rows processed'] += 50000

    def close(self):
        self.counters['rows processed'] += self.nrows % 50000
        self.compress()
        for i,row in enumerate(self.data):
            key = numpy.random.randint(0, 4000000000)
            # If this is not the final output, we can use a TypedBytes String format
            if not self.isfinal:
                # If we already created the unpacker, then we can use it for efficiency
                if self.unpacker is not None:
                    yield key, self.unpacker.pack(*row)
                else:
                    yield key, struct.pack('d' * len(row), *row)
            else:
                yield key, row

    def __call__(self, data):
        if not self.isreducer:
            self.collect_data(data)
        else:
            for key, values in data:
                self.collect_data(values, key)

        # finally, output data
        for key, val in self.close():
            yield key, val



"""
Tall-and-skinny matrix multiplication
"""
class TSMatMul(MatrixHandler):
    def __init__(self, blocksize=3, mpath='m.txt'):
        MatrixHandler.__init__(self)        
        self.blocksize = blocksize
        self.row = None
        self.data = []
        self.keys = []
        self.parseM(mpath)

    def parseM(self, mpath):
        data = []
        for row in util.parse_matrix_txt(mpath):
            data.append(row)
        self.small = numpy.mat(data)

    def compress(self):        
        # Compute the matmul on the data accumulated so far
        if self.ncols is None or len(self.data) == 0:
            return

        self.counters['MatMul compression'] += 1

        t0 = time.time()
        A = numpy.mat(self.data)
        out_mat = A * self.small
        dt = time.time() - t0
        self.counters['numpy time (millisecs)'] += int(1000 * dt)

        # reset data and add flushed update to local copy
        self.data = []
        for i, row in enumerate(out_mat.getA()):
            yield self.keys[i], row

        # clear the keys
        self.keys = []
    
    def collect(self, key, value):
        self.keys.append(key)
        self.data.append(value)
        self.nrows += 1
        
        # write status updates so Hadoop doesn't complain
        if self.nrows%50000 == 0:
            self.counters['rows processed'] += 50000

    def __call__(self, data):
        for key,value in data:
            self.collect_data_instance(key, value)

            # if we accumulated enough rows, output some data
            if len(self.data) >= self.blocksize * self.ncols:
                for key, val in self.compress():
                    yield key, val
                    
        # output data the end of the data
        for key, val in self.compress():
            yield key, val


"""
ARInv is just a thin wrapper around TSMatMul
"""
class ARInv(TSMatMul):
    def __init__(self, blocksize=3, rpath='m.txt'):
        TSMatMul.__init__(self, blocksize=blocksize, mpath=rpath)
        # Computing ARInv is the same as TSMatMul, except that our multiplier is
        # the inverse of the parsed matrix.
        self.small = numpy.linalg.pinv(self.small)


class Cholesky(dumbo.backends.common.MapRedBase):
    def __init__(self, ncols=10):
        self.ncols = ncols
        self.data = [numpy.zeros((1, ncols)).tolist()[0] for x in range(ncols)]
    
    def close(self):
        L = numpy.linalg.cholesky(self.data)
        M = numpy.mat(L.T)
        for ind, row in enumerate(M.getA()):
            yield ind, row.tolist()

    def __call__(self, data):
        for key,values in data:
            for value in values:
                # TODO(arbenson): handle typedbytes string here
                self.data[key] += numpy.array(value)
                
        for key, val in self.close():
            yield key, val


class AtA(MatrixHandler):
    def __init__(self, blocksize=3):
        MatrixHandler.__init__(self)        
        self.blocksize=blocksize
        self.data = []
        self.A_curr = None
    
    def compress(self):
        # Compute AtA on the data accumulated so far
        if self.ncols is None or len(self.data) == 0:
            return

        t0 = time.time()
        A_mat = numpy.mat(self.data)
        A_flush = A_mat.T * A_mat
        dt = time.time() - t0
        self.counters['numpy time (millisecs)'] += int(1000 * dt)

        # Add flushed update to local copy
        if self.A_curr == None:
            self.A_curr = A_flush
        else:
            self.A_curr += A_flush
        self.data = []

    
    def collect(self, key, value):        
        self.data.append(value)
        self.nrows += 1
        
        if len(self.data) > self.blocksize * self.ncols:
            self.counters['AtA Compressions'] += 1
            # compress the data
            self.compress()
            
        # write status updates so Hadoop doesn't complain
        if self.nrows % 50000 == 0:
            self.counters['rows processed'] += 50000

    def close(self):
        self.counters['rows processed'] += self.nrows % 50000
        self.compress()
        if self.A_curr is not None:
            for ind, row in enumerate(self.A_curr.getA()):
                yield ind, util.array2list(row)

    def __call__(self, data):
        for key,value in data:
            self.collect_data_instance(key, value)
        for key,val in self.close():
            yield key, val


class BtAReducer(MatrixHandler):
    # Now that we have B and A stored together, combine them locally
    def __init__(self, blocksize=3):
        MatrixHandler.__init__(self)
        self.blocksize=blocksize    
        self.BtA = None
        self.dataB = []
        self.dataA = []
   
    def array2list(self,row):
        return [float(val) for val in row]

    def compress(self):
        # Compute BtA on the data accumulated so far
        if self.ncols is None or len(self.dataB) != len(self.dataA):
            return

        t0 = time.time()
        B_mat = numpy.mat(self.dataB)
        A_mat = numpy.mat(self.dataA)
        BtA_flush = B_mat.T * A_mat
        dt = time.time() - t0
        self.counters['numpy time (millisecs)'] += int(1000 * dt)
        self.counters['BtA Compressions'] += 1

        # reset data and add flushed update to local copy
        self.dataB = []
        self.dataA = []
        if self.BtA == None:
            self.BtA = BtA_flush
        else:
            self.BtA = self.BtA + BtA_flush

    def collect(self,key,value,subset):        
        subset.append(value)
        self.nrows += 1
        
        if len(subset) > self.blocksize * self.ncols:
            # compress the data
            self.compress()
            
        # write status updates so Hadoop doesn't complain
        if self.nrows % 50000 == 0:
            self.counters['rows processed'] += 50000

    def close(self):
        if len(self.dataA) != len(self.dataB):
            raise DataFormatException('A and B data lengths do not match!')
        self.counters['rows processed'] += self.nrows
        self.compress()
        for ind, row in enumerate(self.BtA.getA()):
            r = self.array2list(row)
            yield ind, r
        
    def __call__(self, data):
        # this is always a reducer
        for key,values in data:
            for val in values:
                if val[0] == 'B':
                    self.collect(key, val[1], self.dataB)
                elif val[0] == 'A':
                    self.collect(key, val[1], self.dataA)
                else:
                    raise DataFormatException('Do not recognize source of data')

        for key, val in self.close():
            yield key, val


class BtAMapper(dumbo.backends.common.MapRedBase):
    opts = [('addpath','yes')]
    def __init__(self, B_id):
        self.B_id = B_id

    def __call__(self, key_, value):
        path = key_[0]
        print >>sys.stderr, 'path is: ' + str(path)
        key = key_[1]
        if path.find(self.B_id) == -1:
            # this is A
            self.counters['A values'] += 1
            yield key, ('A', value)
        else:
            # this is B
            self.counters['B values'] += 1
            yield key, ('B', value)


class ArraySumReducer(MatrixHandler):
    def __init__(self):
        MatrixHandler.__init__(self)
        self.row_sums = {}

    def collect(self, key, value):
        if key not in self.row_sums:
            self.row_sums[key] = value
        else:
            if len(value) != len(self.row_sums[key]):
                print >>sys.stderr, 'value: ' + str(value)
                print >>sys.stderr, 'value: ' + str(self.row_sums[key])
                raise DataFormatException('Differing array lengths for summing')
            for k in xrange(len(self.row_sums[key])):
                self.row_sums[key][k] += value[k]
    
    def __call__(self, data):
        for key, values in data:
            self.collect_data(values, key)
        for key in self.row_sums:
            yield key, self.row_sums[key]

        

