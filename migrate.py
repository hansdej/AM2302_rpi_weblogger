#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Module docstring goes here"""
import sys
import os
import argparse
from storage import copy_old_to_new
from storage import determine_replacements
from storage import build_new_from_old, connect, update_displaylist
import storage
logger = storage.logger
storage.init_logging(logger)

__author__ = 'Hans de Jonge'
__version__ = ''
__date__ = ''
__license__ = 'Gpl'

__all__ = [ ]

def main():
    """The main function is the entry point of a program"""
    # main is separated out into a function so that it too can be thoroughly
    # tested.

    the_file = './am2302log.db'
    build_new_from_old(the_file)
    session= connect(the_file)
    update_displaylist(session)
    return 0

if __name__ == '__main__':
    # This is the main body of this module. Ideally it should only contain at
    # most **one** call to the entry point of the program.
    sys.exit(main())


