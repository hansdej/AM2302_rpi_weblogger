#!/usr/bin/env python3

import sqlite3
import sys
import cgi
import cgitb
import numpy as np


# global variables
speriod=(15*60)-1
dbname='/var/www/templog.db'
# Redefine the path to the actual databasefile:
pad = '/home/hansdej/Sensorlog/rpi_temp_logger-master'
dbname=pad+'/am2303log.db'



# print the HTTP header
def printHTTPheader():
    print("Content-type: text/html\n\n")



# print the HTML head section
# arguments are the page title and the table for the chart
def printHTMLHead(title, table):
    print("<head>")
    print("    <title>")
    print(title)
    print("    </title>")
    
    print_graph_script(table)

    print("</head>")

def unspike( rows ):
    newrows=rows
    # define the numpy dtype:
    dataLength = len(rows[0]) - 1 
    # Assume that the first element is the date and the rest are
    # floating values of the magnitudes of interest
    dt = tuple( ['str' ] + [ 'float' for i in range(dataLength)] )
    dt = np.dtype( *dt )
    dataArray = np.array(rows, dtype=dt)

    
    return newrows

# get data from the database
# if an interval is passed, 
# return a list of records from the database
def get_data(interval):

    conn = sqlite3.connect(dbname)
    curs = conn.cursor()

    if interval == None:
        curs.execute("SELECT * FROM readings")
    else:
        curs.execute("SELECT * FROM readings WHERE timestamp>datetime('now','-%s hours')" % interval)
#        curs.execute("SELECT * FROM readings WHERE timestamp>datetime('2013-09-19 21:30:02','-%s hours') AND timestamp<=datetime('2013-09-19 21:31:02')" % interval)

    rows=curs.fetchall()

    conn.close()
    # The output shows up as a list of tuples with n(=3) elements of which
    # the first one is the date.
    rows = unspike(rows)

    return rows


# convert rows from database into a javascript table
def create_table(rows):
    chart_table=""

    for row in rows[:-1]:
        rowstr="['{0}', {1}, {2}],\n".format(str(row[0]),str(row[1]),str(row[2]))
        chart_table+=rowstr

    row=rows[-1]
    rowstr="['{1}', {1}, {2}]\n".format(str(row[0]),str(row[1]),str(row[2]))
    chart_table+=rowstr

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
          ['Time', 'Temperature','Humidity'],"""
          
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
    </script>"""
    chart_code = "%s%s%s"%(chart_code_begin,table,chart_code_end)

    print(chart_code)




# print the div that contains the graph
def show_graph():
    print("<h2>Temperature Chart</h2>")
    print('<div id="chart_div" style="width: 900px; height: 500px;"></div>')



# connect to the db and show some stats
# argument option is the number of hours
def show_stats(option):

    conn=sqlite3.connect(dbname)
    curs=conn.cursor()

    if option is None:
        option = str(24)

    curs.execute("SELECT timestamp,max(temp) FROM readings WHERE timestamp>datetime('now','-%s hour') AND timestamp<=datetime('now')" % option)
 #   curs.execute("SELECT timestamp,max(temp) FROM readings WHERE timestamp>datetime('2013-09-19 21:30:02','-%s hour') AND timestamp<=datetime('2013-09-19 21:31:02')" % option)
    rowmax=curs.fetchone()
    rowstrmax="{0}&nbsp&nbsp&nbsp{1}C".format(str(rowmax[0]),str(rowmax[1]))

    curs.execute("SELECT timestamp,min(temp) FROM readings WHERE timestamp>datetime('now','-%s hour') AND timestamp<=datetime('now')" % option)
 #   curs.execute("SELECT timestamp,min(temp) FROM readings WHERE timestamp>datetime('2013-09-19 21:30:02','-%s hour') AND timestamp<=datetime('2013-09-19 21:31:02')" % option)
    rowmin=curs.fetchone()
    rowstrmin="{0}&nbsp&nbsp&nbsp{1}C".format(str(rowmin[0]),str(rowmin[1]))

    curs.execute("SELECT avg(temp) FROM readings WHERE timestamp>datetime('now','-%s hour') AND timestamp<=datetime('now')" % option)
#    curs.execute("SELECT avg(temp) FROM readings WHERE timestamp>datetime('2013-09-19 21:30:02','-%s hour') AND timestamp<=datetime('2013-09-19 21:31:02')" % option)
    rowavg=curs.fetchone()


    print("<hr>")


    print("<h2>Minumum temperature&nbsp</h2>")
    print(rowstrmin)
    print("<h2>Maximum temperature</h2>")
    print(rowstrmax)
    print("<h2>Average temperature</h2>")
    print("%.3f" % rowavg+"C")

    print("<hr>")

    print("<h2>In the last hour:</h2>")
    print("<table>")
    print("<tr><td><strong>Date/Time</strong></td><td><strong>Temperature</strong></td></tr>")

    rows=curs.execute("SELECT * FROM readings WHERE timestamp>datetime('new','-1 hour') AND timestamp<=datetime('new')")
 #   rows=curs.execute("SELECT * FROM readings WHERE timestamp>datetime('2013-09-19 21:30:02','-1 hour') AND timestamp<=datetime('2013-09-19 21:31:02')")
    for row in rows:
        rowstr="<tr><td>{0}&emsp;&emsp;</td><td>{1}C</td></tr>".format(str(row[0]),str(row[1]))
        print(rowstr)
    print("</table>")

    print("<hr>")

    conn.close()
# Het label word de triviale naam van de periode en de waarde natuurlijk de feitelijke waarde van de periode: meestal een integer
# aantal uren.
periodes = dict()


def print_time_selector(option):

    print("""<form action="/cgi-bin/webgui.py" method="POST">
        Show the temperature logs for  
        <select name="timeinterval">""")
        


    if option is not None:

        if option == "6":
            print("<option value=\"6\" selected=\"selected\">the last 6 hours</option>")
        else:
            print("<option value=\"6\">the last 6 hours</option>")

        if option == "12":
            print("<option value=\"12\" selected=\"selected\">the last 12 hours</option>")
        else:
            print("<option value=\"12\">the last 12 hours</option>")

        if option == "24":
            print("<option value=\"24\" selected=\"selected\">the last 24 hours</option>")
        else:
            print("<option value=\"24\">the last 24 hours</option>")

        if option == "168":
            print("<option value=\"168\" selected=\"selected\">the last week</option>")
        else:
            print("<option value=\"168\">the last week</option>")

    else:
        print("""<option value="6">the last 6 hours</option>
            <option value="12">the last 12 hours</option>
            <option value="24" selected="selected">the last 24 hours</option>
            <option value="168" selected="selected">the last week</option>""")

    print("""        </select>
        <input type="submit" value="Display">
    </form>""")


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
    option=get_option()

    if option is None:
        option = str(24)
        option = str(5400)

    # get data from the database
    records=get_data(option)

    # print the HTTP header
    printHTTPheader()

    if len(records) != 0:
        # convert the data into a table
        table=create_table(records)
    else:
        print("No data found")
        return

    # start printing the page
    print("<html>")
    # print the head section including the table
    # used by the javascript for the chart
    printHTMLHead("Raspberry Pi Temperature Logger", table)

    # print the page body
    print("<body>")
    print("<h1>Raspberry Pi Temperature Logger</h1>")
    print("<hr>")
    print_time_selector(option)
    show_graph()
    show_stats(option)
    print("</body>")
    print("</html>")

    sys.stdout.flush()

if __name__=="__main__":
    main()



