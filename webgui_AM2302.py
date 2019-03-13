#!/usr/bin/env python3

import sqlite3
import sys
import cgi
import cgitb
import numpy as np
import logging
import logging.config
import storage
import datetime
logger = logging.getLogger()


# global variables
speriod=(15*60)-1
dbname='/var/www/templog.db'
# Redefine the path to the actual databasefile:
pad = '/home/hansdej/PiSensor/Data'
dbname=pad+'/am2302log.db'

# print the HTTP header
def printHTTPheader():
    return "Content-type: text/html\n\n" 

# print the HTML head section
# arguments are the page title and the table for the chart
def printHTMLHead(title, table):
    the_head  ="""<head>
    <title>%s</title>
    """%title + """
    %s</head>
    """%print_graph_script(table)

    return the_head

import pandas as pd
def unspike( rows ):

    oldrows=rows.copy()
    # define the numpy dtype:
    dataLength = len(rows[0]) - 1 
    # Assume that the first element is the date and the rest are
    # floating values of the magnitudes of interest
    dt = tuple( ['str' ] + [ 'float' for i in range(dataLength)] )
    dt = np.dtype( *dt )
    dataArray = np.array(rows, dtype=dt)
    for serie in range(dataLength):
        arr = pd.Series(dataArray[:,1+serie].copy() )
        arr = arr.rolling(3).median()
        arr = arr[1:-1]
        dataArray[1:-1,1+serie] = arr
    # en terug:
    rows = [ tuple(dataArray[i,:]) for i in range(dataArray.shape[0])]


    return rows

#from pandas import rolling_median

# get data from the database
# if an interval is passed, 
# return a list of records from the database
def get_data(interval):

    conn = sqlite3.connect(dbname)
    curs = conn.cursor()

    if interval == None:
        #curs.execute("SELECT * FROM readings")
        interval = "%d"%(24*2)
    now = datetime.datetime.now()
    then = now - datetime.timedelta(hours=float(interval))
    session = storage.connect(dbname)
    data = storage.fetch_daterange(session, start_date=then, end_date=now)

    rows = data

    conn.close()
    # The output shows up as a list of tuples with n(=3) elements of which
    # the first one is the date.
    #rows = unspike(rows)

    return rows

# convert rows from database into a javascript table
def create_table(rows, indent = None):
    chart_table=""
    indent = "\t\t\t" if indent is None else indent
    indent = "\n" + indent

    for row in rows:
        #if isinstance( row[1],float) and isinstance(row[2], float):
        rowstr="['%s', %g , %g ],"%(row[1],float(row[2]),float(row[3]))
        if not "nan" in rowstr:
            chart_table+=indent+rowstr

    # remove the last comma:
    if chart_table[-1] == ',':
        chart_table = chart_table[:-1]

    return chart_table


# print the javascript to generate the chart
# pass the table generated from the database info
def print_graph_script(table):

    # google chart snippet
    chart_code_begin="""
    <script type="text/javascript" src="https://www.google.com/jsapi"></script>
    <script type="text/javascript">
      google.load("visualization", "1", {packages:["corechart"]});
      google.setOnLoadCallback(drawChart);
      function drawChart() {
        var data = google.visualization.arrayToDataTable([
          ['Time', 'Temperature','Humidity'],
          """

    chart_code_end="""
        ]);

        var options = {
          title: 'Conditions',
          series: {
            0: {targetAxisIndex: 0, color: 'red'},
            1: {targetAxisIndex: 1, color: 'blue'}
          },
          vAxes: {
            0: { title: 'Temperatuur (Celcius)'},
            1: { title: 'Vochtigheid (%)'}
          }
        };

        var chart = new google.visualization.LineChart(document.getElementById('chart_div'));
        chart.draw(data, options);
      }
    </script>
    """
    chart_code = "%s%s%s"%(chart_code_begin,table,chart_code_end)

    return chart_code




# print the div that contains the graph
def show_graph():
    return """
    <h2>Temperature Chart</h2>
    <div id="chart_div" style="width: 900px; height: 500px;"></div>
    """

# connect to the db and show some stats
# argument option is the number of hours
def show_stats(option):

    conn=sqlite3.connect(dbname)
    curs=conn.cursor()

    if option is None:
        option = str(24)

    curs.execute("SELECT timestamp,max(temp) FROM readings "+ \
                 "WHERE timestamp>datetime('now','-%s hour') "% option + \
                 "AND timestamp<=datetime('now')")
 #   curs.execute("SELECT timestamp,max(temp) FROM readings " + \
 #              "WHERE timestamp>datetime('2013-09-19 21:30:02','-%s hour') "+\
 #              "AND timestamp<=datetime('2013-09-19 21:31:02')" % option)
    rowmax=curs.fetchone()
    rowstrmax="{0}&nbsp&nbsp&nbsp{1}C".format(str(rowmax[0]),str(rowmax[1]))

    curs.execute(   "SELECT timestamp,min(temp) FROM readings "+ \
                    "WHERE timestamp>datetime('now','-%s hour') " % option + \
                    "AND timestamp<=datetime('now')")
 #   curs.execute("SELECT timestamp,min(temp) FROM readings " + \
 #              "WHERE timestamp>datetime('2013-09-19 21:30:02','-%s hour') "+\
 #              "AND timestamp<=datetime('2013-09-19 21:31:02')" % option)
    rowmin=curs.fetchone()
    rowstrmin="{0}&nbsp&nbsp&nbsp{1}C".format(str(rowmin[0]),str(rowmin[1]))

    curs.execute("SELECT avg(temp) FROM readings " + \
                 "WHERE timestamp>datetime(" + \
                 "'now','-%s hour') " % option + \
                 "AND timestamp<=datetime('now')" )
#    curs.execute("SELECT avg(temp) FROM readings " + \
#                   "WHERE timestamp>datetime(" + \
#                   "'2013-09-19 21:30:02','-%s hour') " + \
#                   "AND timestamp<=datetime('2013-09-19 21:31:02')" % option)
    rowavg=curs.fetchone()


    stats  ="""
    <hr
    <h2>Minumum temperature&nbsp</h2>
    """+ "%r"%(rowstrmin) + """
    <h2>Maximum temperature</h2>
    """+ "%r"%(rowstrmax) + """
    <h2>Average temperature</h2>
    """+ "%.3f" % rowavg+"C" + """
    <hr>
    <h2>In the last hour:</h2>
    <table>
    <tr><td><strong>Date/Time</strong></td><td><strong>Temperature</strong></td></tr>
    """

    rows=curs.execute("SELECT * FROM readings WHERE timestamp>" + \
                    "datetime('new','-1 hour') " + \
                    "AND timestamp<=datetime('new')")
 #   rows=curs.execute("SELECT * FROM readings WHERE timestamp>datetime('2013-09-19 21:30:02','-1 hour') AND timestamp<=datetime('2013-09-19 21:31:02')")
    for row in rows:
        rowstr="<tr><td>{0}&emsp;&emsp;</td><td>{1}C</td></tr>\n".format(str(row[0]),str(row[1]))
        stats += rowstr
    stats += """
    </table>
    <hr>
    """

    conn.close()
    return stats
# Het label word de triviale naam van de periode en de waarde natuurlijk de feitelijke waarde van de periode: meestal een integer
# aantal uren.
periodes = dict()


def print_time_selector(option):

    time_selector = """<form action="/cgi-bin/webgui.py" method="POST">
        Show the temperature logs for
        <select name="timeinterval">
        """

    used_valid_option = False
    options_string = ''
    for hours in [6, 12, 24, 168]:

        options_string += '\t\t\t<option value="%d" '%hours

        if str(hours) == option:
            options_string += 'selected=\"selected\" '

        options_string += '>the last ' 
        if hours <= 25:
            options_string += ' %d hours.</option>'%hours
        elif hours <=168:
            days = "%.1f"%(1.0*hours/24.0)
            options_string += ' %s days.</option>'%days
        else:
            weeks = "%.1f"%(1.0*hours/(7.0*24.0))
            if weeks > 1:
                options_string += ' %s weeks.</option>'%weeks
            else:
                options_string += ' 1 week.</option>'%weeks
        options_string += "\n"
    options_string += """        </select>
        <input type="submit" value="Display">
    </form>"""


    return time_selector + options_string


# check that the option is valid
# and not an SQL injection
def validate_input(option_str):
    # check that the option string represents a number
    if option_str.isalnum():
        # check that the option is within a specific range
        if int(option_str) > 0 and int(option_str) <= 168:
            return option_str
        else:
            return None
    else: 
        return None


#return the option passed to the script
def get_option():
    form=cgi.FieldStorage()
    if "timeinterval" in form:
        option = form["timeinterval"].value
        return validate_input (option)
    else:
        return None

# main function
# This is where the program starts 
def main():

    cgitb.enable()

    # get options that may have been passed to this script
    try:
        option=get_option()
    except:
        option = None
    option = str(240)

    #try:
    #    if option is None:
    #        option = str(24*7*8)
    #except:
    #   option = str(24*7*8)

    # get data from the database
    records=get_data(option)

    # print the HTTP header
    page = printHTTPheader()

    if len(records) != 0:
        # convert the data into a table
        table = create_table(records)
    else:
        print("No data found")
        return

    # start printing the page
    page += "<html>\n"
    # print the head section including the table
    # used by the javascript for the chart
    page += "%s\n"%printHTMLHead("Raspberry Pi Temperature Logger", table)

    # print the page body
    page += """
    <body>
    <h1>Raspberry Pi Temperature Logger</h1>\n
    <hr>
    """ + "%s"%print_time_selector(option)  + """
    """ + "%s"%show_graph()                 + """
    """ + "%s"%show_stats(option)           + """
    </body>
    </html>
    """
    print(page)

    sys.stdout.flush()

if __name__=="__main__":
    main()
