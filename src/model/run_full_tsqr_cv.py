#!/usr/bin/env python

"""
Do direct TSQR and cross-validation of error in one. This avoid extra data movement.
It's a really hacked up code.

Austin R. Benson     arbenson@stanford.edu
David F. Gleich
Copyright (c) 2013
"""

import os
import shutil
import subprocess
import sys
import time
import util
import numpy
from optparse import OptionParser

# Parse command-line options
#
# TODO(arbenson): use argparse instead of optparse when icme-hadoop1 defaults
# to python 2.7
parser = OptionParser()
parser.add_option('-i', '--input', dest='input', default='',
                  help='input matrix')
parser.add_option('-o', '--output', dest='out', default='',
                  help='base string for output of Hadoop jobs')
parser.add_option('-l', '--local_output', dest='local_out', default='full_out_tmp',
                  help='Base directory for placing local files')
parser.add_option('-t', '--times_output', dest='times_out', default=None,
                  help='Base directory for placing local files')
parser.add_option('-H', '--hadoop', dest='hadoop', default='',
                  help='name of hadoop for Dumbo')

parser.add_option('--javamem', default=None, help='memory used for java process')
parser.add_option('-r','--replication',default=None, dest="rep",
                  help='HDFS replication factor for output')

parser.add_option('-q', '--quiet', action='store_false', dest='verbose',
                  default=True, help='turn off some statement printing')

parser.add_option('--train-subset', dest='subset', default=None,
                  help='set of training columns in the matrix')
parser.add_option('--parameter-file', dest='params', default=None,
                  help='parameter for each column of the matrix (only used with cross-validate)');
parser.add_option('--test-subset', dest='test_subset', default=None,
                  help='set of testing columns in the matrix')
                  


(options, args) = parser.parse_args()
cm = util.CommandManager(verbose=options.verbose)

# Store options in the appropriate variables
in1 = options.input
if in1 == '':
  cm.error('no input matrix provided, use --input')

out = options.out
if out == '':
  # TODO(arbenson): make sure in1 is clean
  out = in1 + '_FULL'

local_out = options.local_out
out_file = lambda f: local_out + '/' + f
if os.path.exists(local_out):
  shutil.rmtree(local_out)
os.mkdir(local_out)

def copy_and_parse_locally(hdfspath, localpath):
  parsedext='.out' # TODO dgleich make this an option, but it's baked into
                   # cm.parse_seq_file right now
  parsedfile = localpath + parsedext
  if os.path.exists(localpath):
    os.remove(localpath)
  if os.path.exists(parsedfile):
    os.remove(parsedfile)
  cm.copy_from_hdfs(hdfspath, localpath)
  cm.parse_seq_file(localpath)

times_out = options.times_out
if times_out is None:
    times_out = out_file('timing.txt')
    
hadoop = options.hadoop

# validate all the subset arguments

if options.subset is None:
    cm.error('no training subset provided')
else:    
    try:
        subset = [int(s) for s in options.subset.split(',')]
        assert( min(subset) >= 0 )
    except:
        cm.error('invalid training subset provided')

if options.test_subset is None:
    cm.error('no testing subset provided')
else:
    try:
        tsubset = [int(s) for s in options.test_subset.split(',')]
        assert( min(tsubset) >= 0 )
    except:
        cm.error('invalid test subset provided')
    
if options.params is None:
    cm.error('no parameter for column file provided')
else:
    params = numpy.loadtxt(options.params)
    if params.shape[0] < max(subset):
        cm.error('not enough parameters for the training subset')
    if params.shape[0] < max(tsubset):
        cm.error('not enough parameters for the testing subset')

subsetopt = '-subset %s'%(options.subset)  
tsubsetopt = '-test_subset %s'%(options.test_subset)
pfileopt = '-ppath %s'%(options.params)



if options.javamem is None:
    memopt = ''
else:
    memopt = '-jobconf mapred.child.java.opts="-Xmx'+options.javamem+'"'

if options.rep is None:
    repopt = ''
else:
    repopt = '-jobconf dfs.replication='+str(options.rep)

# Now run the MapReduce jobs
out1 = out + '_1'
cm.run_dumbo('full1.py', hadoop, ['-mat ' + in1, '-output ' + out1,
                                  subsetopt, '-saveA 1',
                                  '-libjar feathers.jar',
                                  memopt, repopt, 
                                  '-jobconf mapred.map.max.attempts=10',
                                  '-jobconf mapred.task.timeout=1200000'])

out2 = out + '_2'
cm.run_dumbo('full2.py', hadoop, ['-mat ' + out1 + '/R_*', '-output ' + out2,
                                  '-svd 3',
                                  '-libjar feathers.jar', 
                                  memopt,
                                  '-jobconf mapred.map.max.attempts=10',
                                  '-jobconf mapred.task.timeout=1200000'])


# Q2 file needs parsing before being distributed to phase 3
Q2_file = out_file('Q2.txt')
copy_and_parse_locally(out2+'/Q2', Q2_file)

small_U_file = out_file('U.txt')
small_Sigma_file = out_file('Sigma.txt')
small_Vt_file = out_file('Vt.txt')
copy_and_parse_locally(out2+'/U', small_U_file)
copy_and_parse_locally(out2+'/Sigma', small_Sigma_file)
copy_and_parse_locally(out2+'/Vt', small_Vt_file)

uopt = '-upath '+small_U_file+'.out'
sigmaopt = '-sigmapath '+small_Sigma_file+'.out'
vtopt = '-vtpath '+small_Vt_file+'.out'

in3 = out1 + '/Q_*'
cm.run_dumbo('full3cv.py', hadoop, ['-mat ' + in3, '-output ' + out + '_3',
                                  '-q2path ' + Q2_file + '.out',
                                  '-libjar feathers.jar', 
                                  subsetopt, tsubsetopt, pfileopt,
                                  uopt, sigmaopt, vtopt,
                                  memopt, repopt,
                                  '-jobconf mapred.map.max.attempts=10',
                                  '-jobconf mapred.task.timeout=1200000'])
                                  
                                  
cm.exec_cmd('dumbo cat ' + out + '_3/part-* -hadoop ' + hadoop + ' | python parse_errors.py > '+out_file('errs.txt'))

try:
  f = open(times_out, 'w')
  f.write('times: ' + str(cm.times))
  f.close
except:
  pass
