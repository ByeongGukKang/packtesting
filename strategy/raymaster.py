from os import cpu_count
from asyncio import Event
from typing import Tuple

import ray
from ray.actor import ActorHandle
import numpy as np
from tqdm import tqdm

class RayMaster:

    def __init__(self, desc, num_cpu, data, axis, function, *args):
        # tqdm description
        self.desc = desc

        # Create Batch
        y = [i.shape[axis] for i in np.array_split(data[0], num_cpu, axis=axis)]
        x = [0] + y[:-1]
        xx = [np.sum(x[:i]) for i in range(1, len(x)+1)]
        yy = [np.sum(y[:i]) for i in range(1, len(y)+1)]
        self.batch = zip(xx, yy)

        self.shape = data[0].shape
        self.data = ray.put(data)
        self.function = ray.remote(function)
        self.args = args
    
    def run(self):
        pb = ProgressBar(self.shape[0], self.desc)
        actor = pb.actor

        results = [self.function.remote(actor, start, end, self.data, self.args) for start,end in self.batch]
        pb.print_until_done()
        results = ray.get(results)
        
        if type(results[0]) == dict:
            result = results[0].copy()
            for segment_result in results:
                for key in list(result.keys()):
                    result[key] = result[key] + segment_result[key]

            for key in list(result.keys()):
                result[key] = result[key] - results[0][key]

        else:
            result = np.zeros_like(results[0])
            for segment_result in results:
                result = result + segment_result

        return result

class RayManager:

    num_cpus = 0
    num_max_cpus = cpu_count()
    num_largest_batch = 0

    def __init__(self):
        self.__batch = 0

    @classmethod
    def __get_num_cpus(cls):
        return cls.num_cpus

    @classmethod
    def __change_num_cpus(cls, num):
        cls.num_cpus = num

    @classmethod
    def __get_max_cpus(cls):
        return cls.num_max_cpus

    @classmethod
    def __get_largest_batch(cls):
        return cls.num_largest_batch

    @classmethod
    def __change_largest_batch(cls, batch):
        if batch == "reset":
            cls.num_largest_batch = 0
        else:
            cls.num_largest_batch = max(cls.num_largest_batch, batch)

    @property
    def batch(self):
        return self.__batch

    @property
    def cpu_info(self):
        return {
            "Maximum available CPUs":self.__get_max_cpus(),
            "Currently used CPUs":self.__get_num_cpus(),
            "Current number of batches":self.batch
        }

    def __print_cpu_info(self):
        print("Maximum available CPUs: {cpu}".format(cpu=self.__get_max_cpus()))
        print("Currently used CPUs: {cpu}".format(cpu=self.__get_num_cpus()))


    def _initialize(self, isWhere):
        if ray.is_initialized():
            if isWhere == 'ts':
                self.__batch = self.__get_num_cpus()
            elif isWhere == 'cs':
                self.__batch = 1

        else:
            ray.init(num_cpus=self.__get_max_cpus()-1, log_to_driver=False, include_dashboard=False)
            self.__change_num_cpus(self.__get_max_cpus()-1)
            if isWhere == 'ts':
                self.__batch = self.__get_num_cpus()
            elif isWhere == 'cs':
                self.__batch = 1

            self.__print_cpu_info()

        self.__change_largest_batch(self.__batch)

    def restart(self, num_cpus, num_batches):
        # ValueError Check
        if type(num_cpus) != int:
            raise ValueError("num_cpus must be int")
        elif num_cpus > self.__get_max_cpus():
            raise ValueError("num_cpus cannot be larger than your maximum number of cpus({cpu})".format(cpu=self.__get_max_cpus()))
        
        if type(num_batches) != int:
            raise ValueError("num_batches must be int")
        elif num_batches > num_cpus:
            raise ValueError("num_batches cannot be larger than your number of cpus allocation({cpu})".format(cpu=num_cpus))
        elif num_batches < 1:
            raise ValueError("num_batches must be at least 1")

        # Ray Restart
        ray.shutdown()
        ray.init(num_cpus=num_cpus, log_to_driver=False, include_dashboard=False)

        # Reset number of cpus allocated, largest number of batches
        self.__change_num_cpus(num_cpus)
        self.__change_largest_batch(-1)

        self.__batch = num_batches
        
        self.__change_largest_batch(self.__batch)

        self.__print_cpu_info()

        if num_cpus < self.__get_largest_batch():
            print("Warning! num_cpus is smaller than the largest number of batches({batch}) before restarting".format(batch=self.__get_largest_batch()))

    def set_batch(self, num_batches=None, info=True):
        if type(num_batches) != int:
            raise ValueError("num_batches must be int")
        elif num_batches > self.__get_num_cpus():
            raise ValueError("num_batches cannot be larger than your number of cpus allocation({cpu})".format(cpu=self.__get_num_cpus()))
        elif num_batches < 1:
            raise ValueError("num_batches must be at least 1")

        self.__batch = num_batches
        if info == True:
            self.__print_cpu_info()
            print("Current number of batches: {batch}".format(batch=self.__batch))

### Ray-tqdm Code source comes from official ray-document
# https://docs.ray.io/en/releases-1.11.1/ray-core/examples/progress_bar.html#progress-bar-for-ray-actors-tqdm

@ray.remote
class ProgressBarActor:
    counter: int
    delta: int
    event: Event

    def __init__(self) -> None:
        self.counter = 0
        self.delta = 0
        self.event = Event()

    def update(self, num_items_completed: int) -> None:
        """Updates the ProgressBar with the incremental
        number of items that were just completed.
        """
        self.counter += num_items_completed
        self.delta += num_items_completed
        self.event.set()

    async def wait_for_update(self) -> Tuple[int, int]:
        """Blocking call.

        Waits until somebody calls `update`, then returns a tuple of
        the number of updates since the last call to
        `wait_for_update`, and the total number of completed items.
        """
        await self.event.wait()
        self.event.clear()
        saved_delta = self.delta
        self.delta = 0
        return saved_delta, self.counter

    def get_counter(self) -> int:
        """
        Returns the total number of complete items.
        """
        return self.counter

class ProgressBar:
    progress_actor: ActorHandle
    total: int
    description: str
    pbar: tqdm

    def __init__(self, total: int, description: str = ""):
        # Ray actors don't seem to play nice with mypy, generating
        # a spurious warning for the following line,
        # which we need to suppress. The code is fine.
        self.progress_actor = ProgressBarActor.remote()  # type: ignore
        self.total = total
        self.description = description

    @property
    def actor(self) -> ActorHandle:
        """Returns a reference to the remote `ProgressBarActor`.

        When you complete tasks, call `update` on the actor.
        """
        return self.progress_actor

    def print_until_done(self) -> None:
        """Blocking call.

        Do this after starting a series of remote Ray tasks, to which you've
        passed the actor handle. Each of them calls `update` on the actor.
        When the progress meter reaches 100%, this method returns.
        """
        pbar = tqdm(desc=self.description, total=self.total)
        while True:
            delta, counter = ray.get(self.actor.wait_for_update.remote())
            pbar.update(delta)
            if counter >= self.total:
                pbar.close()
                return
