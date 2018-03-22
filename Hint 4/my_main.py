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
    return cp, len(term.split('_'))


def process_key(rec, per_language_or_project):
    key, value = rec
    if per_language_or_project:
        key = rec[0].split('.')[0]
    else:
        key = rec[0].split('.')[-1]

    return key, value


def filter_rdd(rec, per_language_or_project):
    country = rec[0]
    if per_language_or_project:

        if '.' in country:
            return False
    else:
        if '.' not in country:
            return False

    return True


# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------


def my_main(dataset_dir, o_file_dir, per_language_or_project):
    # 1. We remove the solution directory, to rewrite into it
    dbutils.fs.rm(o_file_dir, True)

    # 1. We read the dataset into an RDD
    inputRDD = sc.textFile(dataset_dir)

    # 1.1 persist
    inputRDD.persist()

    # 2. Extract data into map
    mapRDD = inputRDD.map(lambda x: parse_line(x))

    # 3. Filter
    mapRDD = mapRDD.filter(lambda x: filter_rdd(x, per_language_or_project))

    # 4. Mutate key
    mapRDD = mapRDD.map(lambda rec: process_key(rec, per_language_or_project))

    # 5. Get total
    total = mapRDD.values().sum()

    # 6. We group by the country/langauge/project
    mapRDD = mapRDD.groupByKey()

    # 7. Process Values
    solutionRDD = mapRDD.mapValues(lambda x: (float(sum(x)) / float(total) * 100))

    # 8. Save to file
    solutionRDD.saveAsTextFile(o_file_dir)


# Complete the Spark Job

# ---------------------------------------------------------------
#           PYTHON EXECUTION
# This is the main entry point to the execution of our program.
# It provides a call to the 'main function' defined in our
# Python program, making the Python interpreter to trigger
# its execution.
# ---------------------------------------------------------------
if __name__ == '__main__':
    dataset_dir = "/FileStore/tables/A01_my_dataset/"
    o_file_dir = "/FileStore/tables/A01_my_result/"

    per_language_or_project = False  # True for language and False for project

    my_main(dataset_dir, o_file_dir, per_language_or_project)
