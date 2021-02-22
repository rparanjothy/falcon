import threading
import time
from flask import Flask,jsonify

class Load():
    # This is where we define the work?

    def __init__(self, payload, compute=max, maxPerWorker=10):
        self.payload = self.toGen(payload)
        self.compute = compute
        self.maxPerWorker = maxPerWorker

    def toGen(self, payload):
        """
            make this a gen
        """
        o = ()
        for i in payload:
            o += (i,)
            if ((len(o) == self.maxPerWorker) or (len(o) == len(payload))):
                yield o
                o = ()
        else:
            if len(o):
                yield o


class Cluster(Flask):
    def __init__(self, nProcessors=10, load=None):
        self.nProcessors = nProcessors
        self.incomingCompute = load.compute
        self.badWorkers = set()
        self.freeWorkers = set()
        self.busyWorkers = set()
        self.bundles = load.payload  # this will be gen
        self.bundleResult = []
        super().__init__(__name__)

        # Start one Leader, rest are workers
        # self.leader = Processor(-1)
        # This leader should keep tabs but for now he is also worker

        self.freeWorkers = {Processor(x, isLeader=False, buffer=self.bundleResult) for x in range(0, self.nProcessors)}

        # Main work aka process
        if not self.bundles:
            raise Exception("Load not split")

        print("=" * 40)
        print(self.incomingCompute.__name__)
        print("=" * 40)

   

    def distributeAndCollect(self):
        '''
            We have the bundle, we have the computefn, we also have the free workers..
            so load them up..
        '''

        def _assignTaskAndLoad(y):

            x = self.fetchNextAvailable()
            self.addToBusy(x)
            
            x.assignTask(self.incomingCompute)

            try:
                x.assignLoadAndRun(y)
                self.addToFree(x)
            except Exception:
                print("adding to bad")
                self.addToBad(x)

        # map
        # Avoid this list just loop
        #  This is probably submittin serially
        
        for x in self.bundles:
            threading.Thread(target=_assignTaskAndLoad,args=[x],daemon=True).start()
        

        # reduce
        # apply the compute func to collected results

        # return self.incomingCompute(list(map(lambda x: x, self.bundleResult)))
        return self.bundleResult

    def getInventory(self):
        return (self.badWorkers, self.freeWorkers, self.busyWorkers)

    def getFreeInventory(self):
        return self.freeWorkers

    def fetchNextAvailable(self):
        # Chances are first set is still clocking.. 
        # so we need to wait until the first set of workers finish

        while not self.freeWorkers:
            print("waiting.. 2 sec")
            time.sleep(2)
            
        if len(self.freeWorkers)>0:
            p=self.freeWorkers.pop() 
            print(">>>","F",len(self.freeWorkers))
            return p    


    def addToFree(self, worker):
        # Take from Busy when done and then add to free

        self.busyWorkers.remove(worker)
        self.freeWorkers.add(worker)

    def addToBusy(self, worker):
        # Take from Free and then add to busy
        # Taking from free is via fetchNext
        # self.freeWorkers.remove(worker)

        self.busyWorkers.add(worker)

    def addToBad(self, worker):
        # if dead, Take from busy and put to bad
        if worker.isDead():
            self.busyWorkers.remove(worker)
            self.badWorkers.add(worker)


class Processor():
    def __init__(self, id, name=None, isLeader=True, buffer=None):
        # sets
        self.id = id
        self.isLeader = isLeader
        self.name = name
        self.setName()
        self.dead = False
        self.task = None
        self.buffer = buffer
        self.info()

    def assignTask(self, task):
        self.task = task

    def assignLoadAndRun(self, payload):
        self.payload = payload
        if not self.task:
            raise Exception("Task not assigned")

        def cc(x, v):
            try:            
                
                res = x(v)
                self.buffer.append(res)

                print(self.name,self.buffer,v,res)
                # with open(f"./out/{self.name}.out", "a") as g:
                #     g.write(f"{self.buffer}: {str(res)}: {v}")
                #     g.write("\n")
                    # raise Exception("I am dead")
            except Exception:
                self.dead = True
                print(self.name, "dead")

        cc(self.task, self.payload)

    def isDead(self):
        return self.dead

    def info(self):
        print(self.id, self.name)

    def setName(self):
        '''
            Set the name for this processor based on isLeader - debating on the significance
        '''
        self.name = f"leader-{self.id}" if self.isLeader else f"worker-{self.id}" if not self.name else self.name

    def becomeLeader(self):
        '''
            become leader
        '''
        self.isLeader = True
        self.setName()


# ----------
mxPerProc = 10

# Define the Work to be distributed here
def ccc(x):
    time.sleep(10)
    return sum(x)

# mainLoad = Load(range(100_000), compute=sum, maxPerWorker=mxPerProc)
mainLoad = Load(range(200), compute=ccc, maxPerWorker=mxPerProc)

# Start the cluster with the payload definition and process
nProc = 4
system = Cluster(nProc, load=mainLoad)
system.distributeAndCollect()


@system.route("/")
def x():
    return jsonify({"msg":"ok","buff":system.bundleResult,"status":[ {"bad": [i.name for i in system.getInventory()[0]]},{"free": [i.name for i in system.getInventory()[1]]},{"busy": [i.name for i in system.getInventory()[2]]}]})
    

if __name__ == "__main__":    
    system.run("0.0.0.0",5000,debug=True)
  
