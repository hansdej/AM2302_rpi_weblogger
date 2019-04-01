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
import datetime
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
    start_date = datetime.datetime(2017,1,1)
    month = 24*365.25/12
    generate_end_date = lambda s: s + datetime.timedelta(hours=2*month)
    end_date = generate_end_date(start_date)
    now  = datetime.datetime.now()
    dates = []
    while end_date <= now:
        end_date = generate_end_date(start_date)
        dates.append([start_date, end_date])
        start_date = end_date

    dates.append([end_date, now])
    clear_t =True
    for start, end in dates:    
        build_new_from_old(the_file,start_date=start,
            end_date=end,clear_tables=clear_t)
        clear_t=False
    session= connect(the_file)
    update_displaylist(session)
    return 0

if __name__ == '__main__':
    # This is the main body of this module. Ideally it should only contain at
    # most **one** call to the entry point of the program.
    sys.exit(main())


