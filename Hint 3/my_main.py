# --------------------------------------------------------
#           PYTHON PROGRAM
# Here is where we are going to define our set of...
# - Imports
# - Global Variables
# - Functions
# ...to achieve the functionality required.
# When executing > python 'this_file'.py in a terminal,
# the Python interpreter will load our program,
# but it will execute nothing yet.
# --------------------------------------------------------

import re


# ------------------------------------------
# HELPERS
# ------------------------------------------

def parse_line(line_str):
    # Use regex to extract data
    cp, term, count = re.split('(.*?)\s(.*?)\s(\d+)', line_str)[1:-1]
    # build map:
    return cp, (term, int(count))


def process(rec, num_top_entries):
    return sorted(dict(rec).items(), key=lambda v: v[1], reverse=True)[:num_top_entries]


def format_record(rec):
    key, val = rec
    return [(key, value) for value in val]


# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(dataset_dir, o_file_dir, languages, num_top_entries):
    #
    # 1. We remove the solution directory, to rewrite into it
    dbutils.fs.rm(o_file_dir, True)

    # 1. We read the dataset into an RDD
    inputRDD = sc.textFile(dataset_dir)

    # 1.1 persist
    inputRDD.persist()

    # 2. Extract data into map
    mapRDD = inputRDD.map(lambda x: parse_line(x))

    # 3. Filter Language (includes project)
    mapRDD = mapRDD.filter(lambda x: x[0][:2] in languages)

    # 4. We group by the country/langauge
    mapRDD = mapRDD.groupByKey()

    # 5. We sort the groups (en -> zu)
    mapRDD = mapRDD.sortByKey()

    # 6. Process Values
    solutionRDD = mapRDD.mapValues(lambda rec: process(rec, num_top_entries))

    # 7. Flatten the result
    solutionRDD = solutionRDD.flatMap(format_record)

    # 8. Save to file
    solutionRDD.saveAsTextFile(o_file_dir)


if __name__ == '__main__':
    dataset_dir = "/FileStore/tables/A01_my_dataset/"
    o_file_dir = "/FileStore/tables/A01_my_result/"

    languages = ["en", "es", "fr"]
    num_top_entries = 5

    my_main(dataset_dir, o_file_dir, languages, num_top_entries)
