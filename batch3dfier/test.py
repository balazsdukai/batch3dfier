'''
Created on 16 Jul 2017

@author: bdukai
'''
import threading
import logging
import queue
import time

# logging.basicConfig(level=logging.DEBUG,
#                     format='(%(threadName)-10s) %(message)s',
#                     )
# 
# class MyThread(threading.Thread):
#     
#     def __init__(self, stuff):
#         threading.Thread.__init__(self)
#         self.stuff=stuff
# 
#     def run(self):
#         fun(self.stuff)
#         return
#     
# def fun(a):
#     print(a.__len__())
# 
# def run():
#     a = [1, 2, 3]
#     for i in range(5):
#         t = MyThread(a)
#         t.start()
# 
# run()


class myThread (threading.Thread):

    def __init__(self, threadID, name, q, exitFlag, queueLock):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.q = q
        self.exitFlag = exitFlag
        self.queueLock = queueLock

    def run(self):
        print ("Starting " + self.name)
        tile_skipped = process_data(self.name, self.q, self.exitFlag, self.queueLock)
        return(tile_skipped)
        print ("Exiting " + self.name)


def process_data(threadName, q, exitFlag, queueLock):
    while not exitFlag:
        queueLock.acquire()
        if not q.empty():
            tile_dict = q.get()
            queueLock.release()
            print ("%s processing %s" % (threadName, tile_dict['tile']))
            t = worker_fun(tile_dict['tile'], tile_dict['ctr'])
            if t[0] is not None:
                return(t[0])
        else:
            queueLock.release()
            time.sleep(1)
            

def worker_fun(tile, ctr):
    ctr += 1
    if ctr % 2 == 0:
        return([tile, ctr])
    else:
        return([None, ctr])
        print("processing", tile)
    time.sleep(1)

def run(thread_list, union_view, tiles_clipped, tile_views):
    
    exitFlag = 0
    tiles_skipped = []

    threadList = ["Thread-" + str(t+1) for t in range(thread_list)]
    queueLock = threading.Lock()
    workQueue = queue.Queue(0)
    threads = []
    threadID = 1
    
    # Create new threads
    for tName in threadList:
        thread = myThread(threadID, tName, workQueue, exitFlag, queueLock)
        thread.start()
        threads.append(thread)
        threadID += 1
    
    # Fill the queue
    queueLock.acquire()
    if union_view:
        workQueue.put(union_view)
    elif tiles_clipped:
        for tile in tiles_clipped:
            tile_dict = {'tile': tile, 'ctr': CTR}
            workQueue.put(tile_dict)
    else:
        for tile in tile_views:
            workQueue.put(tile)
    queueLock.release()
    
    # Wait for queue to empty
    while not workQueue.empty():
        pass
    
    # Notify threads it's time to exit
    exitFlag = 1
    
    # Wait for all threads to complete
    for t in threads:
        t.join()
    print ("Exiting Main Thread")
    
    return([tiles_skipped, threadList])

CTR = 0

run(3, union_view=None, tiles_clipped=None, tile_views=['a', 'b', 'c'])