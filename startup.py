import subprocess
import os, sys
 pot

def execute (row):
    count += 1

    pfoldername = "logfile" + 'count'
    sfoldername = "logfile" + 'count'
    os.mkdir(foldername)

    loadgen = subprocess.Popen("./generateWorkLoad.py", row, stdout=logfile)
    movefile = subprocess.Popen("mv logfile pfoldername")

    loadgen = subprocess.Popen("dstat -tcmsn -N eth0", stdout=dstatlog)
    movefile = subprocess.Popen("mv dstatlog sfoldername")

   #Show information about top cpu, top latency and top memory
    if row >= 100000:
        loadgen = subprocess.Popen("dstat --top-cpu-adv --top-latency --top-mem", stdout=logfile)



if __name__ == '__main__':
    count = 0
    execute (2000, count):
    execute (40000, count):
    execute (50000, count):
    execute (80000, count):
    execute (100000, count):
    #execute (200000, count):
    #execute (300000, count):
    
