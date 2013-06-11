"""
MapReduce job 

python mr_outputexoduswithSVD_hadoop.py \
hdfs://icme-hadoop1.localdomain/user/yangyang/simform/predict/p* \
-r hadoop --no-output -o  --variable TEMP \
--outputname = thermal_maze_interpolation
--outdir = hdfs://icme-hadoop1.localdomain/user/yangyang/simform/exodus
--indir = hdfs://icme-hadoop1.localdomain/user/yangyang/simform/

"""

__author__ = 'Yangyang Hou <hyy.sun@gmail.com>'

import sys
import os
from mrjob.job import MRJob
from mrjob.compat import get_jobconf_value

import exopy as ep
from numpy import array, loadtxt
from subprocess import call, check_call, Popen, PIPE, STDOUT

class MROutputExoduswithSVD(MRJob):

    STREAMING_INTERFACE = MRJob.STREAMING_INTERFACE_TYPED_BYTES
    JOBCONF = {'mapred.child.java.opts':'-Xmx7g'}
    
    def configure_options(self):
        """Add command-line options specific to this script."""
        super(MROutputExoduswithSVD, self).configure_options()
        
        self.add_passthrough_option(
            '--variable', dest='variable',
            help='--variable VAR, the variable need to be inserted in the exodus file'       
        )
        self.add_passthrough_option(
            '--outputname', dest='outputname',
            help='--outputname NAME, the name of created new interpolation exodus file'       
        )
        self.add_passthrough_option(
            '--outdir', dest='outdir',
            help='--outdir DIR, Write the output to the directory DIR'       
        )
        self.add_passthrough_option(
            '--indir', dest='indir',
            help='--indir DIR, The HDFS directory you can get the template exodus file '       
        )
    
       
    def load_options(self, args):
        super(MROutputExoduswithSVD, self).load_options(args)
            
        if self.options.variable is None:
            self.option_parser.error('You must specify the --variable VAR')
        else:
            self.variable = self.options.variable
        
        if self.options.outputname is None:
            self.option_parser.error('You must specify the --outputname NAME')
        else:
            self.outputname = self.options.outputname
            
        if self.options.outdir is None:
            self.option_parser.error('You must specify the --outdir DIR')
        else:
            self.outdir = self.options.outdir
            
        if self.options.indir is None:
            self.option_parser.error('You must specify the --indir DIR')
        else:
            self.indir = self.options.indir
    

    def mapper(self, key, value):     
        fnum = key[0]
        time = key[1]
        print >>sys.stderr, value
        yield fnum, (time, value)
                
        
    def reducer(self, key, values):
        """
        input: ith new exodus file, (time, (interp, var, err, exact))
        output:ith new exodus file
        """

        print >>sys.stderr, "reducer:key = ", key
        
        myvals = dict( values ) # realize the values to cache them by
            # time-step

        print >>sys.stderr, "reducer:timesteps = ", myvals.keys()        

        print >>sys.stderr, "reducer:values = ", myvals             

        sortedtime = sorted(myvals.keys())
        
        interp = array([ myvals[ti][0] for ti in sortedtime ] )
        var = array([ myvals[ti][1] for ti in sortedtime ] )
        err = array([ myvals[ti][2] for ti in sortedtime ] )
        exact = array([ myvals[ti][3] for ti in sortedtime ] )
        
        # grab template exodus file from HDFS        
        tmpstr = self.indir[7:]
        index = tmpstr.find('/')
        prefix = 'hdfs://'+tmpstr[0:index]
        
        cmd = 'hadoop fs -ls '+ self.indir
        p = Popen(cmd, shell=True, stdin=PIPE, stdout=PIPE, stderr=STDOUT, close_fds=True)
        content = p.stdout.read()
        files = content.split('\n')
        
        flag = True

        for file in files:
            file = file.split(' ')
            fname = file[len(file)-1]
            if fname.endswith('.e'):
                fname = prefix + fname
                if flag:
                    check_call(['hadoop', 'fs', '-copyToLocal', fname, 'template.e'])
                    flag = False
                    break
        
        template = 'template.e'
        
        # create new interpolation exodus file
        
        if call(['test', '-e', template]) != 0:
            print >>sys.stderr, "The template file doesnot exist!"
            yield key,1
        else:
            print >>sys.stderr, "Reading templatefile %s"%(template)
            templatefile = ep.ExoFile(template,'r')
            
            outfile = self.outputname+str(key)+'.e'
            print >>sys.stderr,  "Writing outputfile %s"%(os.path.join(outfile))
            newfile = ep.ExoFile(os.path.join(outfile),'w')  
            
            interp_name = self.variable+'_INTERP'
            var_name = self.variable+'_SQRTVAR'
            err_name = self.variable+'_ERR'
            templatefile.change_nodal_vars2(newfile, array(sortedtime), 
                (interp_name, var_name, err_name, self.variable), 
                (interp,var,err,exact), ('d','d','d','d'))
            
            newfile.src.sync() 
            newfile.close()
            print >>sys.stderr, "Finished writing data, copying to Hadoop"
            
            user = get_jobconf_value('mapreduce.job.user.name')
            call(['hadoop', 'fs', '-copyFromLocal', outfile, os.path.join(self.outdir,outfile)])
            call(['hadoop', 'fs', '-chown', '-R', user, os.path.join(self.outdir)])
            
            print >>sys.stderr, "Copied to Hadoop, removing ..."
            
            call(['rm', template])
            call(['rm', outfile])
            yield key,0
            
            print >>sys.stderr, "Done"
        

if __name__ == '__main__':
    MROutputExoduswithSVD.run()
