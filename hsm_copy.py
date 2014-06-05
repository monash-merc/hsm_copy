#!/usr/bin/python
# This script takes a list of files on stdin, and reads them back from tape minimising tape loads and unloads
# It assumes that the primary copy works. If the HSM has to move onto a secondary copy, thats fine, but in that case,
# the system might unload primary,load backup,read, unload backup, load previous tape.
# Hopefully this doesn't happen often.

#import dmf_wrapper as hsm
import dummy_hsm_wrapper as hsm
import dummy_copy as dest
import logging

class hsmfile():

    def __init__(self,filename,tape=None,sequence=None):
        self.filename=filename
        self.tape=tape
        self.sequence=sequence


    def __str__(self):
        return "file %s on tape %s at %s"%(self.filename,self.tape,self.sequence)

class errorObject():

    def __init__(self,hsmfile,exception=None):
        self.hsmfile=hsmfile
        self.exception=exception
    
    def __str__(self):
        return "error processing %s. Exeption was %s"%(self.hsmfile,self.exception)

def getInfo(hsmfile):
    success=False
    try:
        hsmfile.tape=hsm.get_tape(hsmfile.filename)
        hsmfile.sequence=hsm.get_sequence(hsmfile.filename)
        success=True
    except: 
        pass
    if success:
        return hsmfile
    else:
        raise Exception("exception in getInfo for file %s"%hsmfile)

def getFile(hsmfile):
    success=False
    try:
        hsm.get_file(hsmfile.filename)
        success=True
    except:
        pass
    if success:
        return hsmfile
    else:
        raise Exception("exception in getFile for file %s"%hsmfile)

def waitFile(hsmfile):
    success=False
    try:
        hsm.wait_file(hsmfile.filename)
        success=True
    except:
        pass
    if success:
        return hsmfile
    else:
        raise Exception("exception in waitFile for file %s"%hsmfile)

def releaseFile(hsmfile):
    success=False
    try:
        hsm.release_file(hsmfile.filename)
        success=True
    except:
        pass
    if success:
        return hsmfile
    else:
        raise Exception("exception in releaseFile for file %s"%hsmfile)

def copyFile(hsmfile):
    success=False
    try:
        dest.copy(hsmfile.filename)
        success=True
    except:
        pass
    if success:
        return hsmfile
    else:
        raise Exception("exception in copyFile for file %s"%hsmfile)

def verifyFile(hsmfile):
    success=False
    try:
        dest.verify(hsmfile.filename)
        success=True
    except:
        pass
    if success:
        return hsmfile
    else:
        raise Exception("exception in verifyFile for file %s"%hsmfile)



def threadQueueWrap(func,inq,outq,errq,stop,debug=False):
    import Queue
    import time
    while not (stop.isSet()) or (not inq.empty()):
        try:
            o=inq.get(False)
            outq.put(func(o))
        except Queue.Empty as e:
            if stop.isSet():
                return
            else:
                time.sleep(0.001)
        except Exception as e:
            errq.put(errorObject(o,e))

def progress(queue,maxfiles,stop):
    import Queue
    import time
    count=0
    while not stop.isSet():
        try: 
            queue.get(False)
            count=count+1
            if (count%10)==0:
                logger.debug("sync'd %s of %s files on this tape"%(count,maxfiles))
        except Queue.Empty as e:
            time.sleep(1)


def main():
    import fileinput
    import logging
    import threading
    import Queue
    import datetime
    import time
    logger.debug("Starting HSM Sync")
    filelist=Queue.Queue()
    fileinfo=Queue.Queue()
    errorQueue=Queue.Queue()
    nthreads=10 # Execute as many threads as you want here. 
               # These are quering the HSM database, not accessing tape drives
               # So pick a number that won't kill the server, but will be fast
    stop=threading.Event() # We use "stopObject" to indicate to a thread that it can stop processing. 
    threads=[]
    for i in range(nthreads): # This will look at our list of files that we need to move, and gather info on which tape and sector it is on. 
                              # Its threaded for parallel access to the HSM database
        thread=threading.Thread(target=threadQueueWrap,args=[getInfo,filelist,fileinfo,errorQueue,stop])
        thread.start()
        threads.append(thread)
    for line in fileinput.input():
        f=hsmfile(line.strip())
        filelist.put(f)
    logger.debug("enqueued all files for getInfo")
    stop.set() # Tell the threads that we have queued up everything we are going to do.
    for t in threads:
        t.join()

    tapes={}
    logger.debug("sorting files into their tapes")
    while not fileinfo.empty(): # Having obtained info in all of our files, sort them into lists on each tape
        f=fileinfo.get()
        if tapes.has_key(f.tape):
            tapes[f.tape].append(f)
        else:
            tapes[f.tape]=[f]

    for t in tapes.keys(): # Next for each tape, we sort its list of files from the begining of the tape to the end
        logger.debug("sorting tape %s into a sequence of files"%t)
        tapes[t].sort(key=lambda hsmfile: hsmfile.sequence)


    logger.debug("Begining HSM operations")
    maxHSMRequests=10
    maxCopyRequests=20
    tapecount=0
    maxtapes=len(tapes.keys())
    for tapeid in tapes.keys(): # Processing one tape at a time. We could process each tape sequentially without waiting, but this way, if we have errors, we might want to retry before we unload
        tapecount=tapecount+1
        maxfiles=len(tapes[tapeid])
        logger.debug("Processing files on tape %s of %s"%(tapecount,maxtapes))
        logger.debug("There are %s files on this tape"%(maxfiles))
        getQueue=Queue.Queue()
        getStop=threading.Event()
        waitQueue=Queue.Queue(maxsize=1)
        waitStop=threading.Event()
        copyQueue=Queue.Queue()  
        copyStop=threading.Event()
        verifyQueue=Queue.Queue()
        verifyStop=threading.Event()
        releaseQueue=Queue.Queue()
        releaseStop=threading.Event()
        completeQueue=Queue.Queue()
        getThreads=[]
        waitThreads=[]
        copyThreads=[]
        verifyThreads=[]
        releaseThreads=[]
        for i in range(1): # Get file should asynchnosly request files to be on disk. Only 1 thread should be necessary
            t=threading.Thread(target=threadQueueWrap,args=[getFile,getQueue,waitQueue,errorQueue,getStop])
            t.start()
            getThreads.append(t)
        for i in range(maxHSMRequests): # number of HSM requests = number of threads + size of queue (1)
            t=threading.Thread(target=threadQueueWrap,args=[waitFile,waitQueue,copyQueue,errorQueue,waitStop])
            t.start()
            waitThreads.append(t)
        for i in range(maxCopyRequests):
            t=threading.Thread(target=threadQueueWrap,args=[copyFile,copyQueue,verifyQueue,errorQueue,copyStop])
            t.start()
            copyThreads.append(t)
        for i in range(1):
            t=threading.Thread(target=threadQueueWrap,args=[verifyFile,verifyQueue,releaseQueue,errorQueue,verifyStop])
            t.start()
            verifyThreads.append(t)
        for i in range(1):
            t=threading.Thread(target=threadQueueWrap,args=[releaseFile,releaseQueue,completeQueue,errorQueue,releaseStop])
            t.start()
            releaseThreads.append(t)

        progressStop=threading.Event()
        progressThread=threading.Thread(target=progress,args=[completeQueue,maxfiles,progressStop])
        progressThread.start()

        for f in tapes[tapeid]:
            getQueue.put(f)
        getStop.set()
        for t in getThreads:
            t.join()
        waitStop.set()
        for t in waitThreads:
            t.join()
        copyStop.set()
        for t in copyThreads:
            t.join()
        verifyStop.set()
        for t in verifyThreads:
            t.join()
        releaseStop.set()
        for t in releaseThreads:
            t.join()

        while not completeQueue.empty():
            time.sleep(0.001)
        progressStop.set()
        progressThread.join()

        while not errorQueue.empty():
            logger.critical("%s"%errorQueue.get())

        while not completeQueue.empty():
            f=completeQueue.get()
            if isinstance(f,hsmfile): # There will be stopObjects in the queue, so we will skip them
                logger.info("synced file %s"%f)



    

logging.basicConfig(filename="hsm_copy.log")
logger=logging.getLogger("hsm_copy")
logger.setLevel(logging.DEBUG)
main()
