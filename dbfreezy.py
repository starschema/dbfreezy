#!/usr/bin/env python
#
# Copyright (c) 2010,2011 Starschema Ltd., wwww.starschema.net
# 
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions
# are met:
# 
# 1. Redistributions of source code must retain the above copyright
#    notice, this list of conditions and the following disclaimer.
# 2. Redistributions in binary form must reproduce the above copyright
#    notice, this list of conditions and the following disclaimer in the
#    documentation and/or other materials provided with the distribution.
# 
# THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY EXPRESS OR
# IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
# OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
# IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT, INDIRECT,
# INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
# NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
# DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
# THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF
# THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

import sys, getopt, logging
from time import strftime, localtime
import csv, gzip, codecs, cStringIO

import pyodbc

ROWS_PER_FLUSH=1000

def help():
    print """Help screen:
Starting:    %s <parameters>
Parameters:
    -c   --config <filename>            Config file location
    -C   --connstring <string>          Connection string (eg: DSN=myConn;UID=myUser;PWD=myPassword)
                                        Used when no config file is provided.
    -s   --statement  <sql statement>   SQL Statement to execute
                                        Used when no config file is provided.
    -r   --result <filename>            Result CSV file name. 
                                        If it ends with '.gz', gzip file will be created.
                                        Used when no config file is provided.
    -d   --debug                        Debug mode (verbose printouts)
    -h   --help                         Help screen

""" % sys.argv[0]

"""
# Example config file:
connectString = DSN=myConnection;uid=myUserName;pwd=myPassword;

run runOne = {
    statement = select * from test1
    filebase  = runOneLog-\%Y-\%m-\%d_\%H-\%M-\%S.csv
}

run runTwo = {
    statement = select * from test2 
    filebase  = runTwoLog-\%Y-\%m-\%d_\%H-\%M-\%S.csv.gz
}
"""

class UnicodeWriter:
    """
    A CSV writer which will write rows to CSV file "f",
    which is encoded in the given encoding.
    """

    def __init__(self, f, dialect=csv.excel, encoding="utf-8", **kwds):
        # Redirect output to a queue
        self.queue = cStringIO.StringIO()
        self.writer = csv.writer(self.queue, dialect=dialect, **kwds)
        self.stream = f
        self.encoder = codecs.getincrementalencoder(encoding)()

    def writerow(self, row):
        self.writer.writerow([s.encode("utf-8") for s in row])
        # Fetch UTF-8 output from the queue ...
        data = self.queue.getvalue()
        data = data.decode("utf-8")
        # ... and reencode it into the target encoding
        data = self.encoder.encode(data)
        # write to the target stream
        self.stream.write(data)
        # empty queue
        self.queue.truncate(0)

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)

def parseConfig(filename):
    logging.debug('Parsing file: %s' % filename)
    runs = []
    connString = ""

    run = { 'name': None, 'stmt': None, 'file': None }
    inARun = False

    for line in open(filename):
        line = line.lstrip().rstrip()

        # ignore empty lines and comments
        if len(line) == 0: continue
        if line.startswith('#'): continue

        if (line.startswith('connectString') or line.startswith('connString')) \
          and line.find('=') != -1:
            start = line.index('=') + 1
            stop = len(line)

            connString = line[start:stop]
            connString = connString.lstrip().rstrip()

            logging.debug('Parsed connection string: %s' % connString)
        elif line.startswith('run') and line.find('=') != -1:
            start = line.index('run') + len('run')
            stop = line.index('=')

            runName = line[start:stop]
            runName = runName.lstrip().rstrip()
            
            inARun = True
            run['name'] = runName
            
            logging.debug('Parsed run definition: %s' % runName)

        elif line.startswith('}'):
            if inARun:
                runs.append(run)
                logging.debug('Parsed run closing bracket, registered run: %s' % run['name'])

                run = { 'name': None, 'stmt': None, 'file': None }
                inARun = False
            else:
                logging.error("Closing bracket ( } ) found but no RUN defined. Check your config file")

        elif line.startswith('statement') and line.find('=') != -1:
            if inARun:
                start = line.index('=') + 1
                stop = len(line)

                stmt = line[start:stop]
                stmt = stmt.lstrip().rstrip()

                run['stmt'] = stmt
                logging.debug('Parsed sql statement: %s' % run['stmt'])
            else:
                logging.error("SQL Stmt found but no RUN defined. Check your config file")
        elif line.startswith('filebase') and line.find('=') != -1:
            if inARun:
                start = line.index('=') + 1
                stop = len(line)

                file = line[start:stop]
                file = file.lstrip().rstrip()

                run['file'] = file
                logging.debug('Parsed result file: %s' % run['file'])
            else:
                logging.error("Filebase found but no RUN defined. Check your config file")

    return runs, connString

# main routine
def main(config = None, connString = None, statement = None, result = None):
    if config is not None:
        # config file is given, parse it
        runs, connString = parseConfig(config)
    else:
        # no config file given, prepare a run from the cmd line parms
        runs = [ { 'name': 'CmdLine', 
                   'stmt': statement,
                   'file': result, 
                 }, ]

    # connect to odbc
    logging.debug('Connecting to: %s' % connString)
    try:
        odbcConnection = pyodbc.connect(connString, unicode_results=True)
        odbcCursor = odbcConnection.cursor()
    except pyodbc.Error as e:
        logging.error("Unable to connect to database: %s" % e)
        sys.exit(-1)

    # get the actual time for the csv output filename
    now = localtime()

    # process all the parsed runs
    for run in runs:
        logging.debug('Processing run: %s' % run['name'])

        csvFilename = strftime(run['file'], now)
        logging.debug('Basefilename expanded to: %s' % csvFilename)
      
        try:
            if csvFilename.endswith('.gz'):
                logging.debug('Using gzipped csv output')
                csvFile = gzip.open(csvFilename, 'wb')
            else:
                logging.debug('Using uncompressed csv output')
                csvFile = open(csvFilename, 'w+')
        except IOError as (errNo, strErr):
            logging.error("Unable to open result file %s (errNo=%d): %s" % (csvFilename, errNo, strErr)) 
            continue

        # add the BOM header to indicate utf-8 file type
        csvFile.write(codecs.BOM_UTF8)
        csvWriter = UnicodeWriter(csvFile, quoting=csv.QUOTE_NONNUMERIC, dialect=csv.excel)

        logging.debug('Executing SQL statement: %s' % run['stmt'])
        try:
            odbcCursor.execute(run['stmt'])
        except pyodbc.Error as e:
            logging.error("Unable to execute statement: %s" % e)
            continue

        # build up the header
        header = [d[0] for d in odbcCursor.description]
        csvWriter.writerow(header)

        # process fetched rows
        rowsFetched = 0
        rows = []
        for row in odbcCursor:
            unicodeRow = []
            for field in row:
                if field is None:
                    unicodeRow.append(u'')
                else:
                    if type(field) != unicode: 
                        unicodeRow.append(unicode(field))
                    else:
                        unicodeRow.append(field)

            rows.append(unicodeRow)
            rowsFetched += 1

            if rowsFetched % ROWS_PER_FLUSH == 0:
                csvWriter.writerows(rows)
                csvFile.flush()
                rows = []

        # Save the rest
        if len(rows) != 0:
            csvWriter.writerows(rows)
            csvFile.flush()

        logging.debug("%d rows saved to CSV", rowsFetched)
    
        csvFile.close()
        logging.debug('Run processing done for run: %s' % run['name'])


# Application started from command line:
# Parse command line parameters and call the main()
if __name__ == "__main__":
    try:
        opts, args = getopt.getopt(sys.argv[1:], 
                                   "hdc:C:s:r:",
                                   ["help", "debug",
                                    "config=", 
                                    "connstring=", "statement=", "result=",
                                   ]
                                  )
    except getopt.GetoptError, err:
        print str(err)
        help(); sys.exit()

    config = None
    connString = None; statement = None; result = None
    debug = False
    for o, a in opts:
        if o in ("-h", "--help"): help(); sys.exit()
        elif o in ("-d", "--debug"):      debug = True 
        elif o in ("-c", "--config"):     config = a 
        elif o in ("-C", "--connstring"): connString = a
        elif o in ("-s", "--statement"):  statement = a
        elif o in ("-r", "--result"):     result = a
        else:  help(); sys.exit()
     
    # if config is not set and the command line query parameters are also not set
    # display help screen
    if config is None and (connString is None or statement is None or result is None):
        help(); sys.exit()

    # set verbosity
    if debug: logging.basicConfig(level=logging.DEBUG)
    else:     logging.basicConfig(level=logging.INFO)

    # call the main routine
    main(config, connString, statement, result)
