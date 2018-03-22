#!/usr/bin/python

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

import codecs


# ---------------------------------------
#  FUNCTION get_key_value
# ---------------------------------------
def get_key_value(line):
    # 1. Get rid of the end of line at the end of the string
    line = line.replace('\n', '')

    # 2. Split the string by the tabulator delimiter
    words = line.split('\t')

    # 3. Get the key and the value and return them

    key = words[0]
    value = words[1]
    return key, value


# ------------------------------------------
# FUNCTION simulating_sort
# ------------------------------------------
def simulating_sort(input_stream, output_stream):
    # 1. We read the content from input to store it to a list
    my_list = []
    for line in input_stream.readlines():
        # 1.1. We get the key and the value
        (k, v) = get_key_value(line)

        # 1.2. We store it to the dictionary
        my_list.append((k, v))

    # 2. We sort the list
    my_list.sort()

    # 3. We print the sorted file to the output_stream
    for i in range(0, len(my_list)):
        # 3.1. We get the element
        (key, value) = my_list[i]

        res = key + '\t' + value + '\n'
        output_stream.write(res)


# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(i_file_name, o_file_name):
    # 1. Select the map_simulation_result file
    input_file = codecs.open(i_file_name, "r", encoding='utf-8')
    output_file = codecs.open(o_file_name, "w", encoding='utf-8')

    # 2. We trigger the map_simulation
    simulating_sort(input_file, output_file)

    # 3. We close the files
    input_file.close()
    output_file.close()


# ---------------------------------------------------------------
#           PYTHON EXECUTION
# This is the main entry point to the execution of our program.
# It provides a call to the 'main function' defined in our
# Python program, making the Python interpreter to trigger
# its execution.
# ---------------------------------------------------------------
if __name__ == '__main__':
    # 1. Input parameters
    i_file_name = "map_simulation.txt"
    o_file_name = "sort_simulation.txt"

    # 2. Call to the function
    my_main(i_file_name, o_file_name)
