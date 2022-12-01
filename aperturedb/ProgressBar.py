import os
import time


class ProgressBar(object):

    def __init__(self, filename="", use_last=0):

        if filename != "":
            self.fd = open(filename, 'w')
            self.file = True
            self.filename = filename
        else:
            self.file = False

        self.use_last = 50 if use_last == 0 else use_last
        self.progress = 0.0
        self.eta = 0

        self.progress_arr  = []
        self.progress_time = []

        self.done = False

        self.last_msg_call = time.time()

    def __del__(self):

        if self.file:
            self.fd.close()

    def msg(self):

        self.progress = 1 if self.progress > 1 else self.progress

        progress = "{:.2f}".format(self.progress * 100)
        eta = "{:.2f}".format(self.eta)
        msg = "Progress: " + progress
        msg += "% - ETA(" + self.eta_unit + "): " + eta

        if self.done:
            msg += "\n"

        return msg

    def updatefile(self):

        self.fd.write("\r" + self.msg())
        self.fd.flush()

    def updatescreen(self):

        print('\r', end="")
        print(self.msg(), end="", flush=True)

    def compute_stats(self, progress):

        if int(progress) == 1:
            self.done = True

        if progress < self.progress:
            self.progress = []
            self.progress_time = []

        self.progress = progress
        self.progress_arr.append(progress)
        self.progress_time.append(time.time())

        progress_per_sec = 0

        firsttime = True
        for (progress, rec_time) in zip(self.progress_arr, self.progress_time):

            if firsttime:
                firsttime = False
            else:
                delta = rec_time - prev_time
                progress_diff = progress - prev_progress
                progress_per_sec += progress_diff / delta

            prev_progress = progress
            prev_time     = rec_time

        samples = len(self.progress_arr)
        avg_progress_per_sec = 0

        if samples >= 2:
            avg_progress_per_sec = progress_per_sec / (samples - 1)

        now = time.time()
        # If we called this function less than a second ago,
        # we don't update ETE because jitters too much.
        if now - self.last_msg_call < 1 and self.eta > 0:
            return

        self.last_msg_call = now

        if avg_progress_per_sec > 0:
            self.eta = (1 - progress) / avg_progress_per_sec
        else:
            self.eta = 0

        if self.eta > 3600.0:
            self.eta = self.eta / 3600.0
            self.eta_unit = "h"
        elif self.eta > 60.0:
            self.eta = self.eta / 60.0
            self.eta_unit = "m"
        else:
            self.eta_unit = "s"

        if len(self.progress_arr) > self.use_last:
            self.progress_arr  = self.progress_arr[-(self.use_last):]
            self.progress_time = self.progress_time[-(self.use_last):]

    def update(self, progress):

        if self.done:
            return

        self.compute_stats(progress)

        if self.file:
            self.updatefile()
        else:
            self.updatescreen()
