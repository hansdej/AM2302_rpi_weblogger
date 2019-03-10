#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Module docstring goes here"""
import sys
import os
import argparse
from storage import copy_old_to_new
from storage import determine_replacements

__author__ = 'Hans de Jonge'
__version__ = ''
__date__ = ''
__license__ = 'Gpl'

__all__ = [ ]

def main(args):
    """The main function is the entry point of a program"""
    # main is separated out into a function so that it too can be thoroughly
    # tested.
    copy_old_to_new("am2303.db", "am2302.db")
    determine_replacements("am2302.db")
    return 0

if __name__ == '__main__':
    # This is the main body of this module. Ideally it should only contain at
    # most **one** call to the entry point of the program.
    sys.exit(main(sys.argv))


