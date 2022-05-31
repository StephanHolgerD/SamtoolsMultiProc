from glob import glob
import time
import os
import subprocess
import sys
import multiprocessing as mp
from threading import Thread
import socket

cores = int(sys.argv[2])
path = sys.argv[1]
out=sys.argv[3]
outF=out.split('.')[0]
if not os.path.exists(outF):
    os.makedirs(outF)
#cores = 1
#path = '/home/stephan/Documents/ITD_693_bam2Cram/00_bams/00_softLinkRaid/'
#out='test'



AllFiles=[[x] for x in glob(path+'/*bam')]

stop_threads=False


def GetIOusage(outFolder):
    while True:
        global stop_threads
        hostname=socket.gethostname()    
        logF=time.strftime("%D_%Y_%H_%M_%S", time.localtime()).replace('/','_')
        logF=f'{outFolder}/{hostname}_{logF}_iostat.log'
        bashCommand=f"iostat -x "
        process = subprocess.Popen(bashCommand.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        output, error = process.communicate()
        error=error.decode("utf-8")
        output=output.decode("utf-8")
        with open(logF,'w') as o:
            o.write(output)
        time.sleep(10)
        if stop_threads:
            break

def ViewBam(file=None, threads=1):
    st=time.time()
    fileSize=os.path.getsize(file)*(9.31*(10**-10))
    start=time.strftime("%D-%H:%M:%S", time.localtime())
    bashCommand=f"samtools view -@ {threads} {file}"
    #bashCommand=f"ls {file}"
    process = subprocess.Popen(bashCommand.split(), stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    output, error = process.communicate()
    error=error.decode("utf-8")
    end=time.strftime("%D-%H:%M:%S", time.localtime())
    et=time.time()-st
    return [start,end,et,file,fileSize]



t = Thread(target = GetIOusage, args=(outF,))

t.start()
with mp.Pool(cores) as pool:
    x= pool.starmap(ViewBam,AllFiles)


    
with open(out,'w') as o:
    o.write(f'start\tend\tduration\tfile\tfilesize\n')
    for xx in x:
        o.write(f'{xx[0]}\t{xx[1]}\t{xx[2]}\t{xx[3]}\t{xx[4]}\n')
        
stop_threads=True

t.join()

    
