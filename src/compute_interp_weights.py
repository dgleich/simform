import sys
import numpy as np
from StringIO import StringIO
import pdb
   

if __name__ == "__main__":
   
   Vlist = []
   Vfile = file(sys.argv[1])
   for line in Vfile:
      i0 = line.find('(')
      i1 = line.find(')')
      i = int(line[i0+1:i1])

      i0 = line.find('[')
      i1 = line.find(']')
      v = np.loadtxt(StringIO(line[i0+1:i1]))
      Vlist.append(v)
   V = np.asarray(Vlist).transpose()

   Siglist = []
   Sigfile = file(sys.argv[2])
   for line in Sigfile:
      i0 = line.find('(')
      i1 = line.find(')')
      i = int(line[i0+1:i1])

      i0 = line.find('[')
      i1 = line.find(']')
      sig = np.loadtxt(StringIO(line[i0+1:i1]))
      sigind = np.nonzero(sig>0)[0][0]
      Siglist.append(sig[sigind])
   Sig = np.asarray(Siglist)

   design_points = np.loadtxt(sys.argv[3])
   ndp = design_points.shape[0]
   interp_points = np.loadtxt(sys.argv[4])
   nip = interp_points.shape[0]

   ds = np.diff(design_points)
   dV = np.diff(V,axis=0)
   dVds = dV/np.tile(ds,(ndp,1)).transpose()
   
   Tau = np.cumsum(np.abs(dVds),axis=1)

   # this only works for 1d
   Vinterp = np.zeros((nip,ndp))
   for i in range(ndp):
      Vinterp[:,i] = np.interp(interp_points,design_points,V[:,i])

   W = np.zeros((nip,ndp+1))
   if sys.argv[5] == 'R':
      R = float(sys.argv[6])
      for i,p in enumerate(interp_points):
         W[i,0] = R
         W[i,1:R+1] = Vinterp[i,0:R]*Sig[0:R]
         W[i,R+1:] = Sig[R:]
   else:      
      taubar = float(sys.argv[6])
      for i,p in enumerate(interp_points):
         ind = np.minimum(np.sum(p>=design_points)-1,Tau.shape[0]-1)
         R = np.sum(Tau[ind,:]<taubar)
         W[i,0] = R
         W[i,1:R+1] = Vinterp[i,0:R]*Sig[0:R]
         W[i,R+1:] = Sig[R:]
      
   np.savetxt(sys.stdout,W)

