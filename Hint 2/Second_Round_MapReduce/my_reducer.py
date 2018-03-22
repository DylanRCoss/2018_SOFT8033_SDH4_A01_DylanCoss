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

import sys
import codecs

# ------------------------------------------
# GLOBALS
# ------------------------------------------
import re
from collections import OrderedDict
from pprint import pprint

LINE_FORMAT = "{country}\t({count},{percentage}%)"


# ------------------------------------------
# HELPERS
# ------------------------------------------

def parse_line(line_str):
    cp, count = line_str.split('\t')
    # cp, term, count = line_str.split()[:-1]
    return str(cp), int(count)


def line_parser(order_dict):
    lines = []
    for country, listing in order_dict.items():
        count, per = listing

        lines.append(LINE_FORMAT.format(
            country=country,
            percentage=per,
            count=count
        ))
    return lines


# ------------------------------------------
# FUNCTION my_reduce
# ------------------------------------------
def my_reduce(input_stream, total_petitions, output_stream):
    counts = {}

    # Creates Map
    for line in input_stream.readlines():
        try:

            country, count = parse_line(line)

            # Reference map entry
            if country in counts:
                counts[country] = counts[country] + count
            else:
                counts[country] = count

        except:
            pass

    # Calculate
    index, out = {}, OrderedDict({})
    for k, v in counts.items():
        index[v] = k
        counts[k] = v, (v / total_petitions) * 100

    # # Sorts Map's by value
    for country, entry in sorted(index.items(), key=lambda v: v, reverse=True):
        out[entry] = counts[entry]

    # Write output
    str_lines = "\n".join(line_parser(out))
    output_stream.writelines(str_lines)
    print (str_lines)


# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(debug, i_file_name, total_petitions, o_file_name):
    # We pick the working mode:

    # Mode 1: Debug --> We pick a file to read test the program on it
    if debug == True:
        my_input_stream = codecs.open(i_file_name, "r", encoding='utf-8')
        my_output_stream = codecs.open(o_file_name, "w", encoding='utf-8')
    # Mode 2: Actual MapReduce --> We pick std.stdin and std.stdout
    else:
        my_input_stream = sys.stdin
        my_output_stream = sys.stdout

    # We launch the Map program
    my_reduce(my_input_stream, total_petitions, my_output_stream)


# ---------------------------------------------------------------
#           PYTHON EXECUTION
# This is the main entry point to the execution of our program.
# It provides a call to the 'main function' defined in our
# Python program, making the Python interpreter to trigger
# its execution.
# ---------------------------------------------------------------
if __name__ == '__main__':
    # 1. Input parameters
    debug = True

    # This variable must be computed in the first stage
    total_petitions = 16032047

    i_file_name = "sort_simulation.txt"
    o_file_name = "reduce_simulation.txt"

    my_main(debug, i_file_name, total_petitions, o_file_name)
