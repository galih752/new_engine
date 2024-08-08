import threading
from pusher_core import pusher_core
from pusher_nextpage import main as pusher_nextpage

def run_pusher_core():
    pusher_core()

def run_pusher_nextpage():
    pusher_nextpage()

if __name__ == '__main__':
    thread1 = threading.Thread(target=run_pusher_core)
    thread2 = threading.Thread(target=run_pusher_nextpage)

    thread1.start()
    thread2.start()

    thread1.join()
    thread2.join()

    print("Both functions have completed.")