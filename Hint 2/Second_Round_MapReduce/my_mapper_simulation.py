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

import os
import my_mapper
import codecs

#------------------------------------------
# FUNCTION get_parent_directory
#------------------------------------------
def get_parent_directory(d):
    # 1. We create the output variable
    res = ""

    # 2. We reverse the string representing the path
    rev = d[::-1]

    # 3. We look for the '\\' character representing the parent folder
    if "\\" in rev:
        # 3.1. If there is, we take all the name of the directory until the last '\\', which turns to be the first one when the
        # folder string name has been reversed.
        i = rev.index("\\")
        sub_rev = rev[(i+1):len(rev)]

        # 3.2. We reverse the parent folder again so as to make the string going forward once again
        res = sub_rev[::-1]
    # 4. If there is no parent directory, we just return d
    else:
        res = d
        print("No posible to go one directory up")

    # 5. We return the output variable
    return res

#------------------------------------------
# FUNCTION get_file_extension
#------------------------------------------
def get_file_extension(file_name):
    # 1. We get the size of a string
    size = len(file_name) - 1

    # 2. We look for the position where the last point was placed
    found = False
    while found == False and size >= 0:
        if file_name[size] == '.':
            found = True
        else:
            size = size - 1

    # 3. We collect the extension
    if found == True:
        extension = file_name[(size+1):(len(file_name))]
    else:
        extension = ''

    # 4. We return the extension
    return extension

#------------------------------------------
# FUNCTION select_files_from_directory_with_concrete_extension
#------------------------------------------
def select_files_from_directory_with_concrete_extension(directory, extension):
    # 1. We get the current directory and ask for all its elements
    os.chdir(directory)
    dir = os.getcwd()
    files = os.listdir(dir)

    # 2. We create a list with all files we are interested at
    # Initially, the list is empty
    selected_files = []

    # 3. We traverse the files of the directory to select the ones we are interested at
    for i in range(0, len(files)):
        name = files[i]
        # 3.1. If the file is not a subdirectory and has txt extension then we pick it
        if os.path.isdir(name) == False and get_file_extension(name) == extension:
            selected_files.append(name)

    # 4. We return the list of files
    return selected_files

# ------------------------------------------
# FUNCTION simulating_my_map
# ------------------------------------------
def simulating_my_map(directory, extension, output_stream, per_language_or_project):
    # 1. We get the files to be processed
    input_files = select_files_from_directory_with_concrete_extension(directory, extension)

    # 2. We process them
    for i in range(0, len(input_files)):
        # 3.1. We open the file to be read
        input_stream = codecs.open(input_files[i], "r", encoding='utf-8')

        # 3.2. We process it
        my_mapper.my_map(input_stream, per_language_or_project, output_stream)

        # 3.3. We close the file
        input_stream.close()

# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(ext, o_file_name, per_language_or_project):
    # 1. Set the directory
    current_dir = os.getcwd()
    dataset_dir = "my_dataset"

    # 2. We open the file for writing
    output_file = codecs.open(o_file_name, "w", encoding='utf-8')

    # 3. We trigger the map_simulation
    simulating_my_map(dataset_dir, ext, output_file, per_language_or_project)

    # 4. We close the file
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
    ext = "txt"

    o_file_name = "map_simulation.txt"
    per_language_or_project = False # True for language and False for project

    # 2. Call to the function
    my_main(ext, o_file_name, per_language_or_project)
