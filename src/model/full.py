#!/usr/bin/env dumbo

"""
Full mrtsqr

Austin R. Benson (arbenson@stanford.edu)
David F. Gleich
Copyright (c) 2012
"""

import sys
import time
import struct
import uuid

import numpy
import numpy.linalg

import util
import mrmc

import dumbo
import dumbo.backends.common
from dumbo import opt

"""
FullTSQRMap1
--------------

Input: <key, value> pairs representing <row id, row> in the matrix A

Output:
  1. R matrix: <mapper id, row>
  2. Q matrix: <mapper id, row + [row_id]>
"""
@opt("getpath", "yes")
class FullTSQRMap1(mrmc.MatrixHandler):
    def __init__(self,subset=None):
        mrmc.MatrixHandler.__init__(self)
        self.keys = []
        self.data = []
        self.mapper_id = uuid.uuid1().hex
        self.subset = subset
        
    def add_row(self,row):
        self.data.append(row)
        self.nrows += 1
                
        if self.nrows%50000 == 0:
            self.counters['rows processed'] += 50000
        
    def multicollect(self,key,value):
        """ Collect multiple rows at once with a single key. """
        nkeys = len(value)
        newkey = ('multi',nkeys,key)
        
        self.keys.append(newkey)
        
        for row in value:
            self.add_row(row.tolist())
    
    def collect(self,key,value):
        self.keys.append(key)
        self.add_row(value)

    def close(self):
        self.counters['rows processed'] += self.nrows%50000

        # if no data was passed to this task, we just return
        if len(self.data) == 0:
            return
            
        # take the subset here so we can use numpy array indexing
        # semantics
        mat = numpy.array(self.data)
        if self.subset is not None:
            mat = mat[:,self.subset]

        QR = numpy.linalg.qr(mat)
        Q = QR[0].tolist()
        print >>sys.stderr, "yielding R"
        yield ("R_%s" % str(self.mapper_id), self.mapper_id), QR[1].tolist()

        flat_Q = [entry for row in Q for entry in row]
        val = (struct.pack('d'*len(flat_Q), *flat_Q), self.keys)
        print >>sys.stderr, "yielding Q"
        yield ("Q_%s" % str(self.mapper_id), self.mapper_id), val


    def __call__(self,data):
        self.collect_data(data)
        for key,val in self.close():
            yield key, val


"""
FullTSQRRed2
------------

Takes all of the intermediate Rs

Computes [R_1, ..., R_n] = Q2R_{final}

Output:
1. R_final: R in A = QR with key-value pairs <i, row>
2. Q2: <mapper_id, row>

where Q2 is a list of key value pairs.

Each key corresponds to a mapperid from stage 1 and that keys value is the
Q2 matrix corresponding to that mapper_id
"""
@opt("getpath", "yes")
class FullTSQRRed2(dumbo.backends.common.MapRedBase):
    def __init__(self, compute_svd=False):
        self.R_data = {}
        self.key_order = []
        self.Q2 = None
        self.compute_svd = compute_svd

    def collect(self, key, value):
        assert(key not in self.R_data)
        data = []
        for row in value:
            data.append([float(val) for val in row])
        self.R_data[key] = data

    def close_R(self):
        data = []
        for key in self.R_data:
            data += self.R_data[key]
            self.key_order.append(key)
        A = numpy.array(data)
        QR = numpy.linalg.qr(A)        
        self.Q2 = QR[0]
        self.R_final = QR[1].tolist()
        for i, row in enumerate(self.R_final):
            yield ("R_final", i), row
        if self.compute_svd:
            U, S, Vt = numpy.linalg.svd(self.R_final)
            S = numpy.diag(S)
            for i, row in enumerate(U):
                yield ("U", i), row
            for i, row in enumerate(S):
                yield ("Sigma", i), row
            for i, row in enumerate(Vt):
                yield ("Vt", i), row

    def close_Q(self):
        num_rows = len(self.Q2)
        rows_to_read = num_rows / len(self.key_order)

        ind = 0
        key_ind = 0
        local_Q = []
        for row in self.Q2:
            local_Q.append(row.tolist())
            ind += 1
            if (ind == rows_to_read):
               flat_Q = [entry for row in local_Q for entry in row]
               yield ("Q2", self.key_order[key_ind]), flat_Q
               key_ind += 1
               local_Q = []
               ind = 0

    def __call__(self,data):
        for key,values in data:
                for value in values:
                    self.collect(key, value)

        for key, val in self.close_R():
            yield key, val
        for key, val in self.close_Q():
            yield key, val


"""
FullTSQRMap3
------------

input: Q1 as <mapper_id, [row] + [row_id]>
input: Q2 comes attached as a text file, which is then parsed on the fly

output: Q as <row_id, row>
"""
class FullTSQRMap3(dumbo.backends.common.MapRedBase):
    def __init__(self,q2path='q2.txt',ncols=10,upath=None):
        # TODO implement this
        self.Q1_data = {}
        self.row_keys = {}
        self.Q2_data = {}
        self.Q_final_out = {}
        self.ncols = ncols
        self.q2path = q2path
        self.u_data = None
        if upath is not None:
          self.u_data = []
          for row in util.parse_matrix_txt(upath):
            self.u_data.append(row)
          self.u_data = numpy.mat(self.u_data)

    def parse_q2(self):
        try:
            f = open(self.q2path, 'r')
        except:
            # We may be expecting only the file to be distributed
            # with the script
            f = open(self.q2path.split('/')[-1], 'r')        
        for line in f:
            if len(line) > 5:
                ind1 = line.find('(')
                ind2 = line.rfind(')')
                key = line[ind1+1:ind2]
                # lazy parsing: we only need the keys that we have
                if key not in self.Q1_data:
                    continue
                line = line[ind2+3:]
                line = line.lstrip('[').rstrip().rstrip(']')
                line = line.split(',')
                line = [float(v) for v in line]
                line = numpy.array(line)
                mat = numpy.reshape(line, (self.ncols, self.ncols))
                self.Q2_data[key] = mat
        f.close()

        
    def collect(self, mapkey, rowkeys, mat):
        """
        @param mapkey the unique mapper id
        @param rowkeys a list of keys for all the rows
        @param mat a matrix representing a block of rows of Q1
        
        It is not possible to get multiple blocks from a unique mapper id
        """       
        
        assert( mapkey not in self.Q1_data )
        
        self.Q1_data[mapkey] = mat
        self.row_keys[mapkey] = rowkeys
            

    def close(self):
        # parse the q2 file we were given
        self.parse_q2()
        
        for mapkey in self.Q1_data:
            # for each little chunk output by a mapper
            assert(mapkey in self.row_keys)
            assert(mapkey in self.Q2_data)
            Q1 = self.Q1_data[mapkey]
            
            Q2 = numpy.mat(self.Q2_data[mapkey])
            if self.u_data is not None:
              Q2 = Q2 * self.u_data

            Q_out = (Q1*Q2).getA() # compute product and get array
            
            # decode the key output
            rowoff = 0
            for key in self.row_keys[mapkey]:
                if isinstance(key, tuple) and key[0] == 'multi':
                    # this is a multikey = ('multi',nkeys,origkey)
                    nkeys = key[1]
                    origkey = key[2]
                    block = Q_out[rowoff:rowoff+nkeys]
                    rowoff += nkeys
                    yield origkey, block
                else:
                    yield key, Q_out[rowoff].tolist()
                    rowoff += 1

    def __call__(self, data):
        for key, val in data:
            # key is a mapper id
            # value stores the local Q matrix 
            #   and the list of keys
            matrix, keys = val
            num_entries = len(matrix) / 8
            assert (num_entries % self.ncols == 0)
            mat = list(struct.unpack('d'*num_entries, matrix))
            mat = numpy.mat(mat)
            mat = numpy.reshape(mat, (num_entries / self.ncols , self.ncols))
            
            self.collect(key, keys, mat)
        

        for key, val in self.close():
            yield key, val
