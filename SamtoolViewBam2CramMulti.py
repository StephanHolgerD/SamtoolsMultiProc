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
ref=sys.argv[4]
outF='/'.join(out.split('/')[:-1])
AllFiles=[[x] for x in glob(path+'/*bam')]
stop_threads=False





def GetIOusage(outFolder):
    if not os.path.exists(outFolder):
        os.makedirs(outFolder)
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

def BamCram(file=None, outFile=None, threads=1, ref=None):
    if ref==None or outFile==None or file==None:
        return ['','','','','missing input']
    st=time.time()
    fileSizeBam=os.path.getsize(file)*(9.31*(10**-10))
    start=time.strftime("%D-%H:%M:%S", time.localtime())
    #bashCommand=f"samtools view -@ {threads} {file}"
    #bashCommand=f"ls {file}"
    bashCommand=f"samtools view -@ {threads} -C -o {outFile} -T {ref} {file}"
    
    process = subprocess.Popen(bashCommand.split(), stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    output, error = process.communicate()
    error=error.decode("utf-8")
    fileSizeCram=os.path.getsize(outFile)*(9.31*(10**-10))
    
    end=time.strftime("%D-%H:%M:%S", time.localtime())
    et=time.time()-st
    return [start,end,et,file,fileSizeBam, fileSizeCram]



if not os.path.exists(outF):
    os.makedirs(outF)

t = Thread(target = GetIOusage, args=(f'{outF}/IostatLog',))

t.start()
with mp.Pool(cores) as pool:
    x= pool.starmap(BamCram,AllFiles)

    
with open(out,'w') as o:
    o.write(f'start\tend\tduration\tfile\tfilesizeBam\tfilesizeCram\n')
    for xx in x:
        o.write(f'{xx[0]}\t{xx[1]}\t{xx[2]}\t{xx[3]}\t{xx[4]}\n')
        
stop_threads=True

t.join()

    
