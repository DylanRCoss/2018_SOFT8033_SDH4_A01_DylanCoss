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

LINE_FORMAT = "{country}\t({term},{count})"


# ------------------------------------------
# HELPERS
# ------------------------------------------

def parse_line(line_str):

    cp, term, count = re.split('(.*?)\s(.*?)\s(\d+)', line_str)[1:-1]
    # cp, term, count = line_str.split()[:-1]
    return str(cp), str(term), int(count)


def line_parser(order_dict):
    lines = []
    for country, listing in order_dict.items():
        for count, term in listing.items():
            lines.append(LINE_FORMAT.format(
                country=country,
                term=term,
                count=count
            ))
    return lines


# ------------------------------------------
# FUNCTION my_map
# ------------------------------------------
def my_map(input_stream, languages, num_top_entries, output_stream):
    _index_map = {}

    # _index_map: This is a map where the key is the count and
    # all the terms are with that count are listed as the value
    # Example:
    # {
    #   en: {
    #       12: "term_with_12_count1"
    #       ...
    #       3: "term_with_3_count"
    #    }
    # }
    #

    # Creates Map
    for line in input_stream.readlines():
        try:

            country, term, count = parse_line(line)

            # Ignore all languages not defined
            if country[:2] not in languages:
                continue

            # Reference map entry
            if country in _index_map:
                map_entry = _index_map[country]
            else:
                map_entry = _index_map[country] = {}

            map_entry[count] = term
        except:
            print ('Error', country, term, count)

    # Sorts Map's by value
    for country, entry in _index_map.items():
        _sorted = sorted(entry.items(), key=lambda v: v, reverse=True)

        _index_map[country] = OrderedDict(
            _sorted[:num_top_entries]
        )

    # Sorts map by key (Operation facilitated by sort_simulation)
    _index_map = OrderedDict(sorted(_index_map.items(), key=lambda v: v))

    # Write output
    str_lines = "\n".join(line_parser(_index_map))
    output_stream.writelines(str_lines)
    # print (str_lines)


# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(debug, i_file_name, o_file_name, languages, num_top_entries):
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
    my_map(my_input_stream, languages, num_top_entries, my_output_stream)


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

    languages = ["en", "es", "fr"]

    num_top_entries = 5

    # 2. Call to the function
    my_main(debug, i_file_name, o_file_name, languages, num_top_entries)
