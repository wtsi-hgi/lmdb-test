"""
The worst case is when a reader locks during a read transaction. This prevents
the content that they were reading from being deleted. This means that, without
using locks, it can only be guaranteed that the usable size of the database can
be written once.
"""

from threading import Lock, Thread
from time import sleep

from _common import create_database, ONE_MB, put, calculate_usable_bytes, delete, get


def run():
    run_with_map_size(8 * ONE_MB)


def run_with_map_size(map_size):
    database, database_location = create_database(map_size)
    available_size = calculate_usable_bytes(database, map_size / 50)

    for i in range(100):
        key = str(i)
        print("Starting %s" % i)
        put(key, bytearray(available_size), database)
        Thread(target=paused_get, args=(key, database)).start()
        sleep(0.01)
        deleted = delete(key, database)
        assert deleted
        assert get(key, database) is None


def paused_get(key, database):
    lock = Lock()
    lock.acquire()
    with database.begin() as transaction:
        lock.acquire()
        return transaction.get(key)


if __name__ == "__main__":
    run()
