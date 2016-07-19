import shutil

from _common import ONE_GB, ONE_MB, create_database, put, EXAMPLE_KEY, delete, \
    calculate_largest_possible_entry


def run():
    map_size = ONE_MB
    while map_size < ONE_GB:
        map_size *= 2
        run_with_map_size(map_size)
        print("")


def run_with_map_size(map_size):
    print("Running delete related testing with map size %d" % map_size)

    division = 10
    for i in range(division):
        proportion_of_capacity = (1.0 / division) * (i + 1)
        run_with_map_size_and_proportion_of_capacity(map_size, proportion_of_capacity)


def run_with_map_size_and_proportion_of_capacity(map_size, proportion_of_capacity):
    database, database_location = create_database(map_size)

    available_size = calculate_largest_possible_entry(database, map_size)
    content = bytearray(int(available_size * proportion_of_capacity))

    iterations = 20
    for i in range(iterations):
        try:
            put(EXAMPLE_KEY, content, database)
            print("Put content of size equal to %d%% actual capacity in a database of %d bytes"
                  % ((100 * proportion_of_capacity), map_size))
        except Exception as e:
            print("Exception adding content on iteration %d: %s" % (i + 1, e))
            break
        try:
            deleted = delete(EXAMPLE_KEY, database)
            assert deleted
            print("Content deleted successfully (iteration %d/%d)" % (i + 1, iterations))
        except Exception as e:
            print("Exception deleting content on iteration %d: %s" % (i + 1, e))
            break

    shutil.rmtree(database_location)


if __name__ == "__main__":
    run()
