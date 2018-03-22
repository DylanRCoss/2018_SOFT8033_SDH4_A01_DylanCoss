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

LINE_FORMAT = "{country}\t{count}"


# ------------------------------------------
# HELPERS
# ------------------------------------------

def parse_line(line_str):
    cp, term, count = re.split('(.*?)\s(.*?)\s(\d+)', line_str)[1:-1]
    # cp, term, count = line_str.split()[:-1]
    return str(cp), str(term), int(count)


def line_parser(order_dict):
    lines = []
    for country, total in order_dict.items():
        lines.append(LINE_FORMAT.format(
            country=country,
            count=total
        ))
    return lines


# ------------------------------------------
# FUNCTION my_map
# ------------------------------------------
def my_map(input_stream, per_language_or_project, output_stream):


    counts = {}

    for line in input_stream.readlines():
        try:

            country, term, count = parse_line(line)

            if per_language_or_project:
                if '.' in country:
                    continue
            else:
                if '.' not in country:
                    continue
                else:

                    country = country.split('.')[-1]

            words = len(term.split('_'))

            # Reference map entry

            if country in counts:
                counts[country] += words
            else:
                counts[country] = words

        except:
            pass

    lines = "\n".join(line_parser(counts))
    # print (lines)
    output_stream.writelines(lines)

# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(debug, i_file_name, o_file_name, per_language_or_project):
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
    my_map(my_input_stream, per_language_or_project, my_output_stream)


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

    i_file_name = "./my_dataset/pageviews-20180219-100000_48.txt"
    o_file_name = "mapResult.txt"

    per_language_or_project = False  # True for language and False for project

    # 2. Call to the function
    my_main(debug, i_file_name, o_file_name, per_language_or_project)
