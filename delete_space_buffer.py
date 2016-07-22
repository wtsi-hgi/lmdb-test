import shutil
from UserDict import UserDict
from bisect import bisect_left

from lmdb.cpython import MapFullError

from _common import calculate_largest_possible_entry, ONE_GB, EXAMPLE_KEY, \
    delete, ONE_MB, get, add_files_with_total_size
from _common import create_database


class _LazyCalculatedDict(UserDict):
    """
    Dict with values that are lazily calculated using a calculator function.
    """
    def __init__(self, calculator, *args, **kwargs):
        UserDict.__init__(self, *args, **kwargs)
        self._calculator = calculator

    def __getitem__(self, key):
        if key not in self.data:
            self.data[key] = self._calculator(key)
        return self.data[key]

    def __setitem__(self, key, value):
        if self._calculator(key) != value:
            raise ValueError("")
        self.data[key] = value


FILES_TO_WRITE_INITIALLY = 10
FILES_TO_WRITE_AFTER_DELETE = 10


def run():
    map_size = 8 * ONE_MB
    while map_size < 32 * ONE_GB:
        run_with_map_size(map_size)
        map_size *= 2


def run_with_map_size(map_size):
    # Expressing problem as 2D landscape where we want to find the smallest size
    # buffer more than -1
    def buffer_size_mapper(size_buffer):
        success, _ = can_delete_entries(
            map_size, size_buffer, FILES_TO_WRITE_INITIALLY, FILES_TO_WRITE_AFTER_DELETE)
        return size_buffer if success else -1

    landscape = _LazyCalculatedDict(buffer_size_mapper)
    # Set a smaller max buffer size to reduce the search range based on observed
    # values
    max_buffer_size = int(map_size * 1.0)
    # Binary search the landscape
    minimum_size_buffer = bisect_left(landscape, 1, hi=max_buffer_size) - 1
    assert minimum_size_buffer < max_buffer_size
    if minimum_size_buffer == max_buffer_size:
        raise RuntimeError("Max buffer size set to low for search")

    database, database_location = create_database(map_size)
    available_size = calculate_largest_possible_entry(database, map_size)
    shutil.rmtree(database_location)
    print("Buffer space of %d bytes needed with database of size %d bytes with "
          "%d bytes of actual capacity" % (minimum_size_buffer, map_size, available_size))


def can_delete_entries(map_size, size_buffer, files_to_write_initially, files_to_write_after_delete):
    database, database_location = create_database(map_size)
    largest_entry_size = calculate_largest_possible_entry(database, map_size)
    available_size = largest_entry_size - size_buffer
    assert available_size > 0

    add_files_with_total_size(files_to_write_initially, available_size, database)
    for i in range(files_to_write_initially):
        assert get("%s_%d" % (EXAMPLE_KEY, i), database) is not None
        delete("%s_%d" % (EXAMPLE_KEY, i), database)
        assert get("%s_%d" % (EXAMPLE_KEY, i), database) is None
    try:
        add_files_with_total_size(files_to_write_after_delete, available_size, database)
        return True, largest_entry_size
    except MapFullError as e:
        return False, largest_entry_size
    finally:
        database.close()
        shutil.rmtree(database_location)


if __name__ == "__main__":
    run()
