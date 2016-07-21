import shutil
from UserDict import UserDict

import sys
from bisect import bisect_left

from math import ceil, floor

from _common import calculate_largest_possible_entry, ONE_GB, put, EXAMPLE_KEY, \
    delete, ONE_MB
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


def run():
    map_size = 8 * ONE_MB
    while map_size < 32 * ONE_GB:
        run_with_map_size(map_size)
        map_size *= 2


def run_with_map_size(map_size):
    # Expressing problem as fitness maximisation - small size buffers are
    # preferred, size buffers that are not viable have no value
    def fitness(size_buffer):
        success, _ = can_delete_entries(map_size, size_buffer, 4)
        return map_size - size_buffer if success else 0

    fitness_landscape = _LazyCalculatedDict(fitness)
    # Set a smaller max buffer size to reduce the search range based on observed
    # values
    max_buffer_size = int(map_size * 1.0)
    # Binary search the landscape to find the first viable size buffer (plus 1
    # because get index to left)
    minimum_size_buffer = bisect_left(fitness_landscape, 1, hi=max_buffer_size) + 1
    assert minimum_size_buffer < max_buffer_size
    if minimum_size_buffer == max_buffer_size:
        raise RuntimeError("Max buffer size set to low for search")

    database, database_location = create_database(map_size)
    available_size = calculate_largest_possible_entry(database, map_size)
    shutil.rmtree(database_location)
    print("Buffer space of %d bytes needed with database of size %d bytes with "
          "%d bytes of actual capacity" % (minimum_size_buffer, map_size, available_size))


def can_delete_entries(map_size, size_buffer, files_to_delete):
    database, database_location = create_database(map_size)
    largest_entry_size = calculate_largest_possible_entry(database, map_size)
    available_size = largest_entry_size - size_buffer

    file_size = int(floor(float(available_size) / files_to_delete))
    content = bytearray(file_size)
    i = 0
    while i < files_to_delete - 1:
        put("%s_%d" % (EXAMPLE_KEY, i), content, database)
        i += 1
    remaining_size = available_size - (file_size * i)
    put("%s_%d" % (EXAMPLE_KEY, i), bytes(remaining_size), database)

    try:
        for i in range(files_to_delete):
            delete("%s_%d" % (EXAMPLE_KEY, i), database)
        put(EXAMPLE_KEY, bytearray(available_size), database)
        return True, largest_entry_size
    except:
        return False, largest_entry_size
    finally:
        shutil.rmtree(database_location)


if __name__ == "__main__":
    run()
