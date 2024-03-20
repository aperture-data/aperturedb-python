import math
import time
from threading import Thread


from aperturedb import ProgressBar


class Parallelizer:
    """**Generic Parallelizer**

    A parallelizer converts a series of operations to be executed and partitions it into
    batches, to be executed by multiple threads of execution.
    ```mermaid
    gantt
        title Parallel execution
        dateFormat HH:mm:ss
        section Worker1
        Batch1 :w1, 00:00:00, 10s
        Batch3 :w3, after w1, 10s
        Batch5 :after w3, 10s

        section Worker2
        Batch2 :w2, 00:00:00, 10s
        Batch4 :w4, after w2, 10s
        Batch6 :w6, after w4, 10s

    ```
    """

    def __init__(self, progress_to_file=""):

        self.pb_file = progress_to_file

        self._reset()

    def _reset(self, batchsize: int = 1, numthreads: int = 1):

        # Default Values
        self.batchsize = batchsize
        self.numthreads = numthreads

        self.total_actions = 0
        self.times_arr = []
        self.total_actions_time = 0
        self.error_counter = 0
        self.actual_stats = []

        if self.pb_file:
            self.pb = ProgressBar.ProgressBar(self.pb_file)
        else:
            self.pb = ProgressBar.ProgressBar()

    def get_times(self):

        return self.times_arr

    def run(self, generator, batchsize: int, numthreads: int, stats: bool):

        self._reset(batchsize, numthreads)
        self.stats = stats
        self.generator = generator
        if hasattr(generator, "sample_count"):
            print("sample_count", generator.sample_count)
            self.total_actions = generator.sample_count
        else:
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
            idx_end = min(idx_start + elements_per_thread,
                          self.total_actions)

            thread_add = Thread(target=self.worker,
                                args=(i, generator, idx_start, idx_end))
            thread_arr.append(thread_add)

        a = [th.start() for th in thread_arr]
        a = [th.join() for th in thread_arr]

        # Update progress bar to completion
        if self.stats:
            self.pb.update(1)

        self.total_actions_time = time.time() - start_time

        if self.stats:
            self.print_stats()

    def print_stats(self):
        """
            Must be implemented by child class
        """
        pass
