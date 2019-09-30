from multiprocessing import Process, Manager
from time import sleep


def fn(process_id, shared_list):
    try:
        print("Process running: ", process_id)
        while True:
            shared_list.append(process_id)
            sleep(3)
    except KeyboardInterrupt:
        print("KeyboardInterrupt received. Process: ", process_id)
    finally:
        print("Exiting process: ", process_id)


if __name__ == '__main__':

    processes = []

    manager = Manager()
    _list = manager.list()

    p = Process(target=fn, args=(42, _list))
    p.start()

    try:
        p.join()
    except KeyboardInterrupt:
        print("Keyboard interrupt in main")

    # we want to access the shared list for purposes like saving it to a persistent store or some cleanup logic
    print("Final state of _list")
    for item in _list:
        print(item)

