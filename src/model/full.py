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
import dumbo.util

from dumbo import opt

def setstatus(msg):
    print >>sys.stderr, "Status:", msg
    dumbo.util.setstatus(msg)


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
    def __init__(self,subset=None,save_A=False):
        mrmc.MatrixHandler.__init__(self)
        self.keys = []
        self.data = []
        self.mapper_id = uuid.uuid1().hex
        self.subset = subset
        self.save_A = save_A
        
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
        Amat = numpy.array(self.data)
        mat = Amat
        if self.subset is not None:
            mat = Amat[:,self.subset]

        QR = numpy.linalg.qr(mat)
        Q = QR[0].tolist()
        setstatus("yielding R")
        yield ("R_%s" % str(self.mapper_id), self.mapper_id), QR[1].tolist()
        flat_Q = [entry for row in Q for entry in row]

        if self.save_A:
            setstatus("yielding Q, A")
            flat_A = [entry for row in Amat for entry in row]
            val = (len(Q[0]), 
                   struct.pack('d'*len(flat_Q), *flat_Q),
                   Amat.shape[1],
                   struct.pack('d'*len(flat_A), *flat_A), 
                   self.keys)
        else:
            setstatus("yielding Q")
            val = (struct.pack('d'*len(flat_Q), *flat_Q), self.keys)
        
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
                yield ("U", i), row.tolist()
            for i, row in enumerate(S):
                yield ("Sigma", i), row.tolist()
            for i, row in enumerate(Vt):
                yield ("Vt", i), row.tolist()

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



import numpy as np

class FullTSQRROMCV(dumbo.backends.common.MapRedBase):
    """ Cross-validate a SVD-based ROM for the SIAM SISC experiments.
    
    This code computes the matrix U on a subset of columns and then
    forms a large set of interpolants. The 2-norm error for each
    
    It roughly corresponds to the following Matlab code:
    
    E = zeros(Ntest,Nmodel); # we cross-validate on all other variables
                             
    for i=1:Ntest
        for j=1:Nmodel
            v = vtilde(V,smodel,stest(i))';
            %t = Nmodel-(j-1);
            %ftilde = U(:,1:t)*(sig(1:t).*v(1:t));
            ftilde = U(:,1:j)*(sig(1:j).*v(1:j));
            E(i,j) = norm(ftilde-Ftest(:,i))/norm(Ftest(:,i));
        end
    end

    In other words, we finish computing U, which is stored with A,
    then compute all of the interpolants we need right then, and just
    output error.
    
    """
   
    def __init__(self, q2path, upath, ppath, 
                  subset, test_subset, sigmapath, vtpath):
        # TODO implement this
        self.Q1_data = {}
        self.row_keys = {}
        self.Q2_data = {}
        self.Q_final_out = {}
        self.q2path = q2path
        self.subset = subset
        self.test_subset = test_subset
        self.A_data = {}
        
        self.u_data = self.parse_matrix(upath)
        assert self.u_data is not None, "U matrix required"
        self.v_data = self.parse_matrix(vtpath)
        assert self.v_data is not None, "Vt matrix required"
        self.v_data = self.v_data.transpose()
        self.sig_data = self.parse_matrix(sigmapath)
        assert self.sig_data is not None, "Sigma matrix required"
        self.sig_data = numpy.diag(self.sig_data)
        
        try:
            self.params = numpy.loadtxt(ppath)
        except:
            # handle the case where it comes with the script
            self.params = numpy.loadtxt(ppath.split('/')[-1])
        
            
        self.compute_interpweights()
          
    def parse_matrix(self, mpath):
        if mpath is None:
            return None
        
        data = []
        for row in util.parse_matrix_txt(mpath):
            data.append(row)
        return numpy.array(data)

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



        
    def compute_interpweights(self):
        
        assert(self.sig_data is not None)
        assert(self.subset is not None)
        assert(self.test_subset is not None)
        assert(self.v_data is not None)
        assert(self.params is not None)
        
        Sig = self.sig_data
        V = self.v_data
        
        design_points = self.params[self.subset]
        ndp = design_points.shape[0]
        interp_points = self.params[self.test_subset]
        nip = interp_points.shape[0]
        
        #print >>sys.stderr, "interp_points: ", interp_points
        #print >>sys.stderr, "design_points: ", design_points
        
        Vinterp = np.zeros((nip,ndp))
        for i in xrange(ndp):
            Vinterp[:,i] = np.interp(interp_points,design_points,V[:,i])
        
        Winterp = {}
        # for each possible rank, determine the inter
        for R in xrange(1,self.v_data.shape[1]+1):
            W = np.zeros((nip,ndp))
            for i,p in enumerate(interp_points):
                 W[i,0:R] = Vinterp[i,0:R]*Sig[0:R]
            Winterp[R] = W.T
            
        self.Winterp = Winterp

    def evalerror(self, key, block, Ablock):
        """ 
        This code is specific to the SISC study, it assumes that we want
        to compute 
        
        block = U in the SVD
        Ablock = original values which contain all the exact solutions
        
        Output:
        
        key = 
        origkey
        
        val can be decodes as follows
        for errs in val:
            R = errs[0]
            for j, err in enumerate(errs[1:]):
                # err = error in computing test_subset(j) with R terms
                
        
        where r, t, nodetrange are from the original
        ti is the index from the test subset
        and rank is the the rank of the prediction
        
        """
        
        assert(self.Winterp is not None)
        assert(self.test_subset is not None)
        
        
        errs = []
        for R in self.Winterp:
            interpblock = numpy.dot(block,self.Winterp[R])
            exactblock = Ablock[:,self.test_subset]
            err = numpy.sum(numpy.square(interpblock-exactblock),0).tolist()
            err.insert(0, R)
            errs.append(err)
        
        yield key, errs

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

            U = (Q1*Q2).getA() # compute product and get array
            
            Amat = self.A_data[mapkey]

            # decode the key output
            rowoff = 0
            for key in self.row_keys[mapkey]:
                if isinstance(key, tuple) and key[0] == 'multi':
                    # this is a multikey = ('multi',nkeys,origkey)
                    nkeys = key[1]
                    origkey = key[2]
                    block = U[rowoff:rowoff+nkeys]
                    Ablock = Amat[rowoff:rowoff+nkeys]
                    rowoff += nkeys
                    for outkey,outval in self.evalerror(origkey, block, Ablock):
                        yield outkey, outval
                else:
                    assert False, "Not implemented"
                    #arow
                    #for outkey,outval in self.evalerror(origkey, Q_out[rowoff], A):
                        #yield outkey, outval
                    #rowoff += 1

    def __call__(self, data):
        for key, val in data:
            # key is a mapper id
            
            ncolsQ,matQflat,ncolsA,matAflat,keys = val
            
            num_entries = len(matQflat) / 8
            assert (num_entries % ncolsQ == 0)
            mat = list(struct.unpack('d'*num_entries, matQflat))
            
            mat = numpy.mat(mat)
            Qmat = numpy.reshape(mat, (num_entries / ncolsQ , ncolsQ))
            self.ncols = ncolsQ
            
            num_entries = len(matAflat) / 8
            assert (num_entries % ncolsA == 0)
            mat = list(struct.unpack('d'*num_entries, matAflat))
            
            mat = numpy.mat(mat)
            Amat = numpy.reshape(mat, (num_entries / ncolsA , ncolsA))
            
            mapkey = key

            assert( mapkey not in self.Q1_data )

            self.Q1_data[mapkey] = Qmat
            self.row_keys[mapkey] = keys
            self.A_data[mapkey] = Amat
            


        for key, val in self.close():
            yield key, val


