import threading


class Load():
    # This is where we define the work?

    def __init__(self, payload, compute=max):
        self.payload = payload
        self.compute = compute


class Cluster():
    def __init__(self, nProcessors=10, load=None, maxPerWorker=4):
        self.nProcessors = nProcessors

        self.incomingPayload = load.payload
        self.incomingCompute = load.compute
        self.badWorkers = set()
        self.freeWorkers = set()
        self.busyWorkers = set()
        self.bundles = []
        self.bundleResult = []
        self.maxPerWorker = maxPerWorker

        # Start one Leader, rest are workers
        self.leader = Processor(0)
        self.freeWorkers = {Processor(x, isLeader=False, buffer=self.bundleResult) for x in range(1, self.nProcessors)}
        self.splitWork()

        # Main work aka process
        if self.bundles == []:
            raise Exception("Load not split")

        print("=" * 40)
        print(self.incomingCompute.__name__)
        print(f"{self.maxPerWorker} items each")
        print("=" * 40)

    def splitWork(self):
        '''
            Split the work based on no of workers.. exclude leader
            and get your bundles
        '''
        if not self.incomingPayload:
            raise Exception("Empty load")

        def _split(w, n):
            '''
                n should be Len of payload/nProcessors
            '''
            if w == []:
                return w
            else:
                self.bundles.append(w[:n])
                return _split(w[n:], n)

        _split(self.incomingPayload, self.maxPerWorker)

    def distributeAndCollect(self):
        '''
            We have the bundle, we have the computefn, we also have the free workers..
            so load them up..
        '''

        def _assignTaskAndLoad(y):

            x = self.fetchNextAvailable()
            self.addToBusy(x)
            x.assignTask(self.incomingCompute)
            x.assignLoadAndRun(y)
            self.addToFree(x)

        # return list(map(_assignTaskAndLoad, self.bundles))
        print(f"Total Chunks - {len(self.bundles)}")
        
        # map
        list(map(_assignTaskAndLoad, self.bundles))
        
        # reduce
        # apply the compute func to collected results
        return self.incomingCompute(list(map(lambda x: x, self.bundleResult)))

    def getInventory(self):
        return (self.badWorkers, self.freeWorkers, self.busyWorkers)

    def getFreeInventory(self):
        return self.freeWorkers

    def fetchNextAvailable(self):
        return self.freeWorkers.pop()

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
            self.buffer.append(x(v))

        threading.Thread(target=cc, args=[self.task, self.payload], daemon=True).start()
        # return out
        # return 1

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


def main():
    print("Hello")

    # Define the Work to be distributed here
    mainLoad = Load([i for i in range(1_000_000_000)], compute=sum)

    # Start the cluster with the payload definition and process
    nProc=8
    mxPerProc=100_000_000
    system = Cluster(nProc, load=mainLoad,maxPerWorker=mxPerProc)
    clusterOut = system.distributeAndCollect()
    print(clusterOut)


if __name__ == "__main__":
    main()
