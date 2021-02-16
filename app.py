class Controller():
    def __init__(self,nProcessors=10):
        self.nProcessors = nProcessors
        # self.allWorkers = set()
        self.badWorkers = set()
        self.freeWorkers = set()
        self.busyWorkers = set()

        # Start one Leader, rest are workers
        self.leader= Processor(0)
        self.freeWorkers = { Processor(x,isLeader=False) for x in range(1,self.nProcessors)}

    def getInventory(self):
        return (self.badWorkers,self.freeWorkers,self.busyWorkers)

    def fetchNextAvailable(self):
        return self.freeWorkers.pop()

    def addToFree(self,worker):
        # Take from Busy when done and then add to free
        self.busyWorkers.remove(worker)
        self.freeWorkers.add(worker)


    def addToBusy(self,worker):
        # Take from Free and then add to busy
        self.freeWorkers.remove(worker)
        self.busyWorkers.add(worker)


    def addToBad(self,worker):
        # if dead, Take from busy and put to bad
        if worker.isDead():
            self.busyWorkers.remove(worker)
            self.badWorkers.add(worker)

class Processor():
    def __init__(self, id, name=None, isLeader=True):
        # sets
        self.id = id
        self.isLeader = isLeader
        self.name = name
    
        # actions
        self.setName()
        self.info()
        self.dead=False

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
    system=Controller(4)

if __name__ == "__main__":
    main()


 # Not needed - Maintained at Controller level which will be shared
    # def fetchWorkerInventory(self, controller):
    #     '''

    #         fetchAllWorkers
    #         if you become a w>l then you need to know who all are your workers
    #     '''
    #     newLeader.allWorkers, newLeader.badWorker, newLeader.freeWorkers, newLeader.busyWorkers = (
    #         self.allWorkers, self.badWorker, self.freeWorkers, self.busyWorkers)

    # def showWorkerInventory(self):
    #     """
    #         Show the worker Inventory under this leaders ownership
    #     """
    #     print(self.allWorkers,self.badWorker,self.freeWorkers,self.busyWorkers)
