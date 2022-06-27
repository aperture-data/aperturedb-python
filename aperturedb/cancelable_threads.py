import threading
import time
import logging
import random

logging.basicConfig(level=logging.DEBUG,
        format='(%(threadName)-9s) %(message)s',)

# this is done without locking here, since event is only ever set on.
def process_item(id,ex_e,cnt,wait_time):
    logging.debug('starting process_item count=%i',cnt)
    for i in range(0,cnt):
        logging.debug('processing %i',i)
        if ex_e.isSet():
            logging.debug('%i exiting on event',i)
            break
        time.sleep(wait_time)
        if tid == 3 and i == 4:
            logging.debug("%i simulating failure",i)
            ex_e.set()
            break


if __name__ == '__main__':
    tcnt = 4
    ex = threading.Event()
    threads = []
    args_map = {}
    for tid in range(0,tcnt):
        count = random.randint(3,10)
        t = threading.Thread(name='processing-%i' % tid, target=process_item, args=(tid,ex,count,1))
        args_map[t] = (tid,ex,count,1)
        t.start()
        threads.append(t)

    active_threads = threads
    logging.debug('Waiting on threads')
    while len(active_threads) != 0:
        ex.wait(0.250) # wait for 250ms for error
        new_active = []
        for active in active_threads:
            if not active.is_alive():
                (tid,th_ex,count,wait) = args_map[active]
                logging.debug("Thread %i exited",tid)
                active.join()
            else:
                new_active.append(active)
        active_threads = new_active


