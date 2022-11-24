import math
import time
from threading import Thread


from aperturedb import ProgressBar


class Parallelizer:
    """**Generic Parallelizer**
    A parallelizer converts a series of operations to be executed and partitions it into
    batches, to be execued by multiple threads of execution.
    .. image:: /_static/parallelizer.svg
    """

    def __init__(self, progress_to_file=""):

        self.pb_file = progress_to_file

        self._reset()

    def _reset(self, batchsize=1, numthreads=1):

        # Default Values
        self.batchsize  = batchsize
        self.numthreads = numthreads

        self.total_actions = 0
        self.times_arr = []
        self.total_actions_time = 0
        self.error_counter  = 0
        self.actual_stats = []

        if self.pb_file:
            self.pb = ProgressBar.ProgressBar(self.pb_file)
        else:
            self.pb = ProgressBar.ProgressBar()

    def get_times(self):

        return self.times_arr

    def run(self, generator, batchsize, numthreads, stats):

        self._reset(batchsize, numthreads)
        self.stats = stats
        self.generator = generator
        self.total_actions = len(generator)

        start_time = time.time()

        if self.total_actions < batchsize:
            elements_per_thread = self.total_actions
            self.numthreads = 1
        else:
            elements_per_thread = math.ceil(
                self.total_actions / self.numthreads)

        thread_arr = []
        for i in range(self.numthreads):
            idx_start = i * elements_per_thread
            idx_end   = min(idx_start + elements_per_thread,
                            self.total_actions)

            thread_add = Thread(target=self.worker,
                                args=(i, generator, idx_start, idx_end))
            thread_arr.append(thread_add)

        a = [th.start() for th in thread_arr]
        a = [th.join() for th in thread_arr]

        # Update progress bar to completion
        self.pb.update(1)

        self.total_actions_time = time.time() - start_time

        if self.stats:
            self.print_stats()

    def print_stats(self):
        """
            Must be implemented by child class
        """
        pass
