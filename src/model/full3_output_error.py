#!/usr/bin/env dumbo

"""
Full TSQR algorithm for MapReduce (part 2)

Austin R. Benson (arbenson@stanford.edu)
David F. Gleich
Copyright (c) 2012
"""

import sys

import mrmc
import dumbo
import util
import os
import full

import struct

import numpy as np
import numpy

class FullTSSVDPredictAndComputeError(dumbo.backends.common.MapRedBase):
    """ Cross-validate a SVD-based ROM for the SIAM SISC experiments.
    
    This code computes the matrix U on a subset of columns and then
    forms a large set of interpolants. It selects a single realization
    and outputs data that can generate an exodus file with the two-norm 
    error of a single realization.
    
    In other words, we finish computing U, which is stored with A,
    then compute all of the interpolants we need right then, and just
    output data that corresponds to the error.
    
    """
   
    def __init__(self, q2path, upath, ppath, 
                  subset, predict_column, sigmapath, vtpath,
                  realization, taubar):
        # TODO implement this
        self.Q1_data = {}
        self.row_keys = {}
        self.Q2_data = {}
        self.Q_final_out = {}
        self.q2path = q2path
        self.subset = subset
        self.predict_column = predict_column
        self.A_data = {}
        self.realization = realization
        self.taubar = taubar
        
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
        
            
        self.compute_interpweights_tau()
          
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


    def compute_interpweights_tau(self):
        
        assert(self.sig_data is not None)
        assert(self.subset is not None)
        assert(self.predict_column is not None)
        assert(self.v_data is not None)
        assert(self.params is not None)
        
        Sig = self.sig_data
        V = self.v_data
        
        design_points = self.params[self.subset]
        ndp = design_points.shape[0]
        interp_points = self.params[self.predict_column]
        nip = interp_points.shape[0]
        
        ds = np.diff(design_points)
        dV = np.diff(V,axis=0)
        dVds = dV/np.tile(ds,(ndp,1)).transpose()
        
        Tau = np.cumsum(np.abs(dVds),axis=1)
        
        Vinterp = np.zeros((nip,ndp))
        for i in xrange(ndp):
            Vinterp[:,i] = np.interp(interp_points,design_points,V[:,i])
            
        assert len(interp_points) == 1
        W = np.zeros((1,ndp))
        
        taubar = self.taubar
        
        # customize on the singleton case for prediction
        p = interp_points[0]
        i = 0
        
        ind = np.minimum(np.sum(p>=design_points)-1,Tau.shape[0]-1)
        R = int(np.sum(Tau[ind,:]<taubar))
        W[i,0:R] = Vinterp[i,0:R]*Sig[0:R]
        W[i,R+1:] = 0.
        
        print >>sys.stderr, "(R,taubar) at point %f = (%i,%18g)"%(p, R, taubar)
        
        self.Winterp = (R,W.T) 

    def evalerror(self, key, block, Ablock):
        """ 
        This code is specific to the SISC study, it assumes that we want
        to compute 
        
        block = U in the SVD
        Ablock = original values which contain all the exact solutions
        
        Output:
        
        key = 
        origkey = (r,t,noderange)
        
        block = 
        block of U matrix
        
        val can be decodes as follows
        for errs in val:
            R = errs[0]
            for j, err in enumerate(errs[1:]):
                # err = error in computing predict_column(j) with R terms
                
        
        where r, t, nodetrange are from the original
        ti is the index from the test subset
        and rank is the the rank of the prediction
        
        """
        
        assert(self.Winterp is not None)
        assert(self.predict_column is not None)
        
        r = key[0]
        if r != self.realization:
            # we can skip this one!
            return
            
        t = key[1]
        noderange = key[2]
        
        R,W = self.Winterp
        
        interpblock = numpy.dot(block,W).squeeze()
        exactblock = numpy.array(Ablock[:,self.predict_column]).squeeze()
        varblock = numpy.sqrt(numpy.dot(numpy.array(block[:,R:])**2,(self.sig_data[R:])**2).squeeze())
        err = numpy.array(interpblock-exactblock)
        exact = exactblock
        
        c = self.predict_column[0]
        
        yield (c,t), (noderange, 
            interpblock, varblock, err, 
            exactblock.squeeze())

    def close(self):
        if len(self.Q1_data) == 0:
            return # nothing to do here!
            
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
            
            print >> sys.stderr, "realization = ", self.realization
            
            usekey = False
            for rowkey in keys:
                if isinstance(rowkey, tuple) and rowkey[0] == 'multi':
                    # we are going to optimize here
                    # origkey = (r,t,rowkeys)
                    
                    print >>sys.stderr, "key = ", rowkey
                    origkey = rowkey[2] # extract the original key
                    r = origkey[0] 
                    
                    if r==self.realization:
                        usekey = True
                else:
                    assert False, "not implemented"

            if not usekey:
                # we can skip this block
                continue
            
            mapkey = key

            assert( mapkey not in self.Q1_data )

            self.Q1_data[mapkey] = Qmat
            self.row_keys[mapkey] = keys
            self.A_data[mapkey] = Amat

        for key, val in self.close():
            yield key, val


def aggregate_simulation(key, values):
    """ key = (simno, time-step)
    values = [(noderange, values, var, errors, exactsol)]
    
            The reduce groups all values and variances from a single time-step of
        a simulation via their node id so that we don't have to store the
        node id explicitly anymore and can just store an array instead.

    """

    print >>sys.stderr, "reducer for key", key
    myvals = [ val for val in values ] # realize the values to cache them
    print >>sys.stderr, "reducer for key", key, "values = ", len(myvals)
    nnodes = max( val[0][1] for val in myvals ) # find the number of nodes
    print >>sys.stderr, "nnodes = %i"%(nnodes)
    for val in myvals:
        print >>sys.stderr, "noderange=", val[0]
        
    interpval = numpy.zeros ( nnodes )
    interpvar = numpy.zeros ( nnodes )
    interperr = numpy.zeros ( nnodes )
    interpsol = numpy.zeros ( nnodes )

    for val in myvals:
        # val = (noderange, interpval in range, interperr in range)
        noderange = val[0]
        interpval[noderange[0]:noderange[1]] = val[1]
        interpvar[noderange[0]:noderange[1]] = val[2]
        interperr[noderange[0]:noderange[1]] = val[3]
        interpsol[noderange[0]:noderange[1]] = val[4]
    
    yield key, (interpval, interpvar, interperr, interpsol)
                
    print >>sys.stderr, "done with key"


# create the global options structure
gopts = util.GlobalOptions()

def runner(job):
    subset = gopts.getstrkey('subset')
    subset = [int(_) for _ in subset.split(',')]
        
    predict_column = gopts.getstrkey('predict_column')
    predict_column = [int(_) for _ in predict_column.split(',')]
    
    q2path = gopts.getstrkey('q2path')
    upath = gopts.getstrkey('upath')
    ppath = gopts.getstrkey('ppath')
    sigmapath = gopts.getstrkey('sigmapath')
    vtpath = gopts.getstrkey('vtpath')
    realno = gopts.getintkey('realization')
    taubar = float(gopts.getstrkey('taubar'))
            
    mapper = FullTSSVDPredictAndComputeError(q2path, upath, ppath, subset, predict_column, 
                        sigmapath, vtpath, realno, taubar)
    reducer = aggregate_simulation
    job.additer(mapper=mapper,reducer=reducer,opts=[('numreducetasks',str(1))])
    
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
    gopts.getstrkey('predict_column','-1')
    gopts.getintkey('realization',1)
    gopts.getstrkey('taubar', '0.5')
 
    gopts.save_params()

if __name__ == '__main__':
    dumbo.main(runner, starter)

