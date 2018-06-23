# -*- coding: utf-8 -*-

import lib.x_CAParser as x_CAParser
import lib.ppg_utils  as p_utils
from   lib.ppg_log    import p_log_init, p_log_start, p_log_this, p_log_end
import lib.x_glbls

import fnmatch
import string
import os
import objectpath
import json

import sqlite3
import time
import csv
from operator import itemgetter, attrgetter, methodcaller
import datetime


"""
DOING:

-> Programm soll so in Module zerlegt werden,  dass es als
* eigenständiges Programm laufen kann ...  oder als 
* Modul in ein anderes importiert werden kann. 
  In diesem Fall allerdings muss ich rausbekommen, wie die Konfigurationsparameter übergeben werden??  

  See: Python: import module from another directory at the same level in project hierarchy:
     https://stackoverflow.com/questions/20075884


Dieses schöne Programm ...
... überträgt Zeilen aus JSON-Dateien (identischer Struktur) in eine sqlite-db.
Dadurch wird der Zugriff auf die Daten einfacher.
Das Programm kann lokal (zB cubie-truck) oder auf einem  Server laufen. 

Alle JSON-Dateien liegen in einem einzigen Directory.
In den JSON-Zeilen steht jeweils Zeitpunkt, Messstation, Messdaten. 

Die Struktur der sqlite-db ist einfach (und redundant): Zeitpunkt, Mess-Station, Messdaten (als JSON), original JSON-str. 
Redundant idem die Messdaten zweimal vorkommen: im original JSON-str und separat als Messdaten-JSON.

Einige Daten werden beim Übertragen JSON-Datei -> db leicht modifiziert:
 - die Messzeitpunkte werden 'normalisiert' == um ca 150 sec nach früher/später verschieben, 
   damit sie auf einem vorgegebenen Gitter von Zeitpunkten zu liegen kommen (um besser vergleichbar zu sein)
 - Wenn Lücken auftreten, werden am Anfang der Lücke in die sqlite - db : Null (python: None) einfgefügt. 
   Dadurch werden Lücken in den Zeitreihen der Datensätze in plotly nicht verbunden (sondern bleiben leer).
 - Die Modifikation geschieht in: >def normalize_and_save_data(...)<

nb: esp8266id == last 24 Bit of MAC address (first 24 Bit = manufacturer)

"""

cnt_files = 0
cnt_lines = 0

cnt_files_fail = 0
cnt_lines_fail = 0


# 'confargs' are your configuration parameters / cmdline arguments
confargs = lib.x_glbls.arg_ns


class Data(object):
    def __init__(self):
        self.ip        = ''
        self.unix_time = ''
        self.esp8266id = ''
        self.software_version = ''
        self.sensordatavalues = ''
        self.zeit      = ''
        self.datum     = ''
        self.uhrzeit   = ''
        self.humidity  = ''
        self.temperature = ''
        self.SDS_P1    = ''
        self.SDS_P2    = ''
        self.line_JSON = ''   # original line from *.log file
        self.line_nr   = ''

    def data_print(self):
        print self.ip,
        print self.unix_time,
        print self.esp8266id,
        print self.software_version,
        print self.sensordatavalues,
        print self.zeit,
        print self.datum,
        print self.uhrzeit,
        print self.humidity,
        print self.temperature,
        print self.SDS_P1,
        print self.SDS_P2,
        print self.line_JSON,
        print self.line_nr


# https://pymotw.com/2/sqlite3/

def create_tables(fn_db):
    print 'Creating schema'
    with sqlite3.connect(fn_db) as conn:
        schema = "CREATE TABLE fstb(unix_time INTEGER, esp8266id STRING, software_version STRING," \
                 "zeit, datum, uhrzeit, humidity REAL, temperature REAL, SDS_P1 REAL, SDS_P2 REAL, line_nr INTEGER, " \
                 "PRIMARY KEY(esp8266id, unix_time));"
        conn.executescript(schema)
        #
        schema = "CREATE TABLE fstb_JSON(unix_time INTEGER, station_name STRING, sensordatavalues STRING, line_JSON STRING, line_nr INTEGER, " \
                 "PRIMARY KEY(station_name, unix_time));"
        conn.executescript(schema)
        #
        schema = """
        CREATE TABLE saved_files(saved_file STRING PRIMARY KEY, unix_time INTEGER, date_time STRING, lines INTEGER, 
                 cnt INTEGER, cnt_ok INTEGER, cnt_fail INTEGER);"""
        conn.executescript(schema)


def make_sqlite_db(fn_db, make_new_db = False):
    # +/- testing :
    if make_new_db:
        p_utils.p_file_delete(fn_db)

    msge = 'checking database: >' + fn_db + '<'
    print msge
    p_log_this(msge)

    if not os.path.exists(fn_db) :
        print 'create_tables: >' + fn_db + '<'
        # conn = sqlite3.connect(fn_db)
        # conn.close()
        create_tables(fn_db)
    # conn.close() # Not necessary because of 'with'


def delete_fn_in_db(db_fn, db_table, data_file_name):
    # Delete >data_file_name< in >db_fn<, >db_table<
    sql  = "DELETE FROM " + db_table + " WHERE "
    sql += " saved_file = '" + str(data_file_name) + "' ; "
    print sql; p_log_this(sql)

    with sqlite3.connect(db_fn) as conn:
        cursor = conn.execute(sql)
        return True
    print ' failed !!!'
    return False


def check_fn_in_db(db_fn, db_table, data_file_name):
    # If >data_file_name< is in db return >True<
    msge = 'Check if filename of data_file: >' + data_file_name + '< is in database: >' + db_fn + '<.'
    print msge
    # p_log_this(msge)
    sql = "SELECT * FROM " + db_table
    sql += " WHERE "
    sql += " saved_file = '" + str(data_file_name) + "' ; "
    print sql ,
    p_log_this(sql)

    with sqlite3.connect(db_fn) as conn:
        cursor = conn.execute(sql)
        for row in cursor.fetchall():
            date_time = row[0]
            print ' ok: ' + date_time
            return True
    print ' filename not in database:  -> '
    return False


def check_fn_data_in_db(db_fn, db_table, data_file_name, cnt_lines, cnt_ele):
    # If       all data from file named 'fn' is completely in db => return >date_time<
    # If only some data from file named 'fn' is in db            => return >Null<
    msge = 'Check if all values from data_file: >' + data_file_name + '< are in database: >' + db_fn + '<.'
    print msge
    # p_log_this(msge)
    sql = "SELECT date_time FROM " + db_table
    sql += " WHERE "
    sql += " saved_file = '" + str(data_file_name) + "' AND "
    sql += " lines = '" + str(cnt_lines) + "' AND "
    sql += " cnt = '" + str(cnt_ele) + "' ;   "
    print sql,
    p_log_this(sql)

    with sqlite3.connect(db_fn) as conn:
        cursor = conn.execute(sql)
        for row in cursor.fetchall():
            date_time = row[0]
            print ' ok: ' + date_time
            return date_time
    print ' failed !!!'
    return


def insert_fn_in_db(db_fn, db_table, data_file_name, cnt_lines, cnt, cnt_ok, cnt_fail):
    msge = 'Insert name of data_file: >' + data_file_name + '< into database: >' + db_fn + '<.'
    print msge
    p_log_this(msge)
    unix_time = int(time.time())
    date_time = datetime.datetime.fromtimestamp(int(unix_time)).strftime('%Y-%m-%d %H:%M:%S')

    sql  = "INSERT INTO " + db_table
    sql += " (saved_file, unix_time, date_time, lines, cnt, cnt_ok, cnt_fail )"
    sql += " VALUES ('" + data_file_name + "', '" + str(unix_time) + "', '" + date_time + "', '"
    sql += str(cnt_lines) + "', '" + str(cnt) + "', '" + str(cnt_ok) + "', '" + str(cnt_fail) + "')"
    # print sql

    with sqlite3.connect(db_fn) as conn:
        ret_val = conn.execute(sql)


def check_all_values_ok(ele):
    "Sind alle Werte != None? (Manchmal, d.h. bei den ersten Files, fehlt zB esp8266id)"
    # print dir(ele)
    for property, value in vars(ele).iteritems():
        # print property, ": ", value
        if not value:
            # print property, ": ", value, "   ",
            msge = str (property) + ": " + str (value) + " fehlt!  "
            p_log_this(msge); print msge
            return False
    return True


def insert_data_in_db(table, db_fn, db_table, db_table_JSON, data_file_name):
    cnt         = 0 ; cnt_value_fail = 0
    cnt_ok      = 0 ; cnt_fail      = 0
    cnt_JSON_ok = 0 ; cnt_JSON_fail = 0
    with sqlite3.connect(db_fn) as conn:
        for ele in table:
            # print_data_ele(ele)
            cnt += 1    # counts line of (!) table (not of file)

            # Dumme Korrektur: erst mit der File >20170605.log< werden sowohl ip-Adresse
            # als auch esp8266id aufgezeichnet. Aber beide Daten werden zur späteren Identifikation der Sensoren
            # benötigt. => bis zum 2017-06-05 also diese Daten substituieren.
            #
            # nb: Die Daten sind trotzdem falsch!

            limit = int(1496613625)
            # if (int(ele.unix_time) <= limit) and (ele.ip == "192.168.2.102") and (ele.esp8266id is None):
            if (int(ele.unix_time) <= limit) and (ele.esp8266id is None):
                if (ele.ip  == "http://192.168.2.101"):
                    ele.esp8266id = "3912953"
                else:
                    ele.esp8266id = "2326588"


            if check_all_values_ok(ele):
                sql = "INSERT INTO " + db_table
                sql += " (unix_time, esp8266id, software_version," +\
                          " zeit, datum, uhrzeit," + \
                          " humidity, temperature, SDS_P1, SDS_P2, line_nr)"
                sql += " VALUES ('" + str(ele.unix_time) + "', '" + str(ele.esp8266id) + "', '" + str(ele.software_version)
                sql += "', '" + ele.zeit + "', '" + ele.datum + "', '" + ele.uhrzeit
                sql += "', '" + ele.humidity + "', '" + ele.temperature + "', '" + ele.SDS_P1 + "', '" + ele.SDS_P2
                sql += "', '" + str(ele.line_nr) + "')"
                # print sql
                try:
                    ret_val = conn.execute(sql)
                    if ret_val != -1:
                        cnt_ok += 1
                    else:
                        mssge = data_file_name + ': INSERT failed: line number:' + str(cnt)
                        p_utils.p_terminal_mssge_note_this(mssge)
                        mssge = 'conn.execute(sql) == -1: ' + sql
                        p_utils.p_terminal_mssge_note_this(mssge)
                        cnt_fail += 1
                except:
                    mssge = data_file_name + ': INSERT failed: line number:' + str(cnt)
                    p_utils.p_terminal_mssge_note_this(mssge)
                    p_utils.p_terminal_mssge_note_this('SQL: >' + sql + '<')
                    cnt_fail += 1
                    # print sql, ret_val

                ##################################################################################################
                # https://sqlite.org/json1.html
                # https://www.sqlite.org/json1.html
                # http://www.samadhiweb.com/blog/2016.04.24.sqlite.json.html
                # https://stackoverflow.com/questions/33432421/sqlite-json1-example-for-json-extract-set
                # https://nelsonslog.wordpress.com/2015/09/22/json1-a-sqlite-extension/
                # http://nbviewer.jupyter.org/gist/coleifer/f1fc90c7d4938c73951c
                # https://www.google.de/search?client=firefox-b&dcr=0&ei=W17yWbuRJJOja_mlougK&q=sqlite+JSON+windows&oq=sqlite+JSON+windows&gs_l=psy-ab.3..33i160k1l3.580486.585101.0.587404.8.8.0.0.0.0.268.860.6j1j1.8.0....0...1.1.64.psy-ab..0.8.857...0j0i22i30k1j33i21k1.0.BWIGYE5y43I
                # https://github.com/coleifer?tab=overview&from=2017-09-01&to=2017-09-30&utf8=%E2%9C%93
                # http://t3n.de/news/nodejs-package-lowdb-641330/
                # https://dba.stackexchange.com/questions/122198/is-it-possible-to-store-and-query-json-in-sqlite


                # unix_time INTEGER, esp8266id STRING, daten STRING
                #  !!!! Gutes Beispiel sqlite + JSON !!!!
                #  https://stackoverflow.com/questions/33432421/sqlite-json1-example-for-json-extract-set   !!!! Gutes Beispiel sqlite + JSON !!!!
                #  !!!! Gutes Beispiel sqlite + JSON !!!!

                # unix_time INTEGER, esp8266id STRING, sensordatavalues STRING, line_JSON STRING, line_nr INTEGER,

                # https: // stackoverflow.com / questions / 4547274 / convert - a - python - dict - to - a - string - and -back
                # dict to str json.dumps() and str to dict json.loads()
                sql = "INSERT INTO " + db_table_JSON
                sql += " (unix_time, station_name, sensordatavalues , line_JSON, line_nr)"
                sql += " VALUES ('" + str(ele.unix_time) + "', '" + str(ele.esp8266id) + "', '" + \
                       json.dumps(ele.sensordatavalues) + "', '" + ele.line_JSON + "', '" + str(ele.line_nr) + "')"
                # print sql
                try:
                    ret_val = conn.execute(sql)
                    if ret_val != -1:
                        cnt_JSON_ok += 1
                    else:
                        mssge = data_file_name + ': INSERT failed (JSON) (1): line number:' + str(cnt)
                        p_utils.p_terminal_mssge_note_this(mssge)
                        mssge = 'conn.execute(sql) == -1: ' + sql
                        p_utils.p_terminal_mssge_note_this(mssge)
                        cnt_JSON_fail += 1
                except:
                    mssge = data_file_name + ': INSERT failed (JSON) (2): line number:' + str(cnt)
                    p_utils.p_terminal_mssge_note_this(mssge)
                    p_utils.p_terminal_mssge_note_this('SQL: >' + sql + '<')
                    cnt_JSON_fail += 1
                    # print sql, ret_val
            else:
                mssge = data_file_name + ': line not inserted: table line number:' + str(cnt) + ' (Some val == None)'
                p_utils.p_terminal_mssge_note_this(mssge)
                cnt_value_fail += 1

    # print data_file_name, cnt, cnt_ok , cnt_fail
    msge = 'insert_data_in_db(): insert values from: >' + data_file_name + '< to database: >' + db_fn + '<.'
    print msge
    p_log_this(msge)
    msge = 'insert_data_in_db(): table has ' + str(len(table)) + ' values'
    print msge
    p_log_this(msge)

    msge = 'insert_data_in_db(): cnt, cnt_ok, cnt_JSON_ok, cnt_fail, cnt_JSON_fail, cnt_value_fail = '\
           + str(cnt) + ', ' + str(cnt_ok) + ', ' + str(cnt_JSON_ok)  + ', '\
           + str(cnt_fail) + ', ' + str(cnt_JSON_fail) + ', ' + str(cnt_value_fail)
    print msge
    p_log_this(msge)
    return cnt, cnt_ok, cnt_fail


def get_data_files(feinstaub_dir):
    # returns filenames in >feinstaub_dir< that correspond to '*.log'.
    fn_s = p_utils.p_dir_return_paths_of_level(path=feinstaub_dir, level=1, do_log=True)
    log_files = []
    for fn in fn_s:
        if fnmatch.fnmatch(fn, '*.log'):
            log_files.append(fn)
            print fn

    # return log_files[0:2]
    return log_files


def adjust_feinstaub_logfiles(feinstaub_dir):
    # korrigiert Formatierungsfehler in den frühen YYYYMMDD.log Datenfiles
    # - durch mich ()rh hineingebracht
    # nicht mehr nötig

    # print msge, feinstaub_dir
    p_log_this(msge)
    log_files = get_data_files(feinstaub_dir)

    for log_file_name in log_files:
        org_log_file_name = log_file_name[:-4] + ".org"
        tmp_log_file_name = log_file_name[:-4] + ".tmp"

        log_file = p_utils.p_file_open(log_file_name, mode='r')
        tmp_file = p_utils.p_file_open(tmp_log_file_name, mode='w')
        if p_utils.p_file_exists(tmp_log_file_name):
            for line in iter(log_file):
                pos = string.find(line, ',"unix_time":')
                line = line[:pos] + '"' + line[pos:]
                pos = string.find(line, '"ip":') + len('"ip":')
                line = line[:pos] + '"' + line[pos:]
                tmp_file.write(line)
                # print line [:60]
                # print line.find('"ip":'), line[:30]
            p_utils.p_file_close(tmp_file)
        p_utils.p_file_close(log_file)

        os.rename(log_file_name, org_log_file_name)
        os.rename(tmp_log_file_name, log_file_name)

        print log_file_name
        print org_log_file_name
        print tmp_log_file_name


# https://stackoverflow.com/questions/1373164/how-do-i-create-a-variable-number-of-variables
def val_fetch_from_tree_sensor(obj, json_tree, var_name, new_name):
    # Gets from jsontree the value of element 'varname' and stores it
    # to instance of class Data (== obj) - if new_name is given under the name 'new_name'
    if new_name:
        setattr(obj, new_name, json_tree.execute("$." + var_name))
        return getattr(obj, new_name)
    else:
        setattr(obj, var_name, json_tree.execute("$." + var_name))
        return getattr(obj, var_name)


def val_fetch_from_tree_sensor_data (obj, json_tree, var_name):
    # Gets from jsontree the value of element 'varname', IF IT IS ELEMENT OF AN GENERATOR...
    # ... and stores it to instance of class Data (== obj)
    # - if new_name is given under the name 'new_name'
    query_str = "$.daten.sensordatavalues[@.value_type is " + var_name + "].value"
    value_type_generator = json_tree.execute(query_str)
    # val_set(obj, var_name, list(value_type_generator)[0]),
    setattr(obj, var_name, list(value_type_generator)[0]),
    return getattr(obj, var_name)


def write_csv (table, fn_csv):
    if p_utils.p_file_make(fn_csv, print_message=False):
        f_csv = p_utils.p_file_open(fn_csv, mode= 'wb')
        header_str = ('unix_time', 'esp8266id', 'zeit' , 'datum' , 'uhrzeit', 'humidity', 'temperature', 'SDS_P1', 'SDS_P2')
        # print header_str
        try:
            # writer = csv.writer(f_csv, quoting=csv.QUOTE_NONNUMERIC, lineterminator='\n')
            writer = csv.writer(f_csv, quoting=csv.QUOTE_NONNUMERIC)
            writer.writerow((header_str))

            for row in table:
                # line = (row.esp8266id, row.datum, row.uhrzeit, row.humidity, row.temperature, row.SDS_P1, row.SDS_P2)
                # writer.writerow((line))

                writer.writerow(
                    (
                    row.unix_time,
                    row.esp8266id,
                    row.zeit,
                    row.datum,
                    row.uhrzeit,
                    row.humidity,
                    row.temperature,
                    row.SDS_P1,
                    row.SDS_P2
                    )
                )
        finally:
            p_utils.p_file_close(f_csv)
    else:
        p_utils.p_terminal_mssge_error(fn_csv)


def transform_json_file_to_column_table(data_file_name):
    # In die python >data_table< werden die Daten aus der *.log File zwischengespeichert
    # Zwei Versionen:
    #   Zum einen werden aus der JSON Zeile einzelne Werte extrahiert und in einer Spalte der Tabelle
    # abgelegt (die später in der SQL-Tabelle >fstb< eine Tabellenspalte wird).
    #   Zum anderen werden JSON-(Teil)-Strings der ursprünglichen JSON-Zeile in einer anderen Python-
    # Tabelle abgelegt, die später in der sqlite-Tabelle  >fstb_JSON< mit Hilfe der JSON1-sqlite
    # Erweiterung weiterverarbeitet werden können.

    data_table = []
    cnt_line = 0 ; cnt_ele = 0 ; cnt_fail_01 = 0 ; cnt_fail_02 = 0
    mssge_01 = '' ; mssge_02 = ''
    with open(data_file_name, 'r') as f:
        p_log_this('processing: >' + data_file_name + '<')
        for line in f:
            cnt_line += 1
            ele = Data()
            ele.line_JSON = line.strip (",")
            # delete leading comma:
            if line[0] == ',': line = line[1:]
            try:  # to transform JSON-Data to data_table
                json_data = json.loads(line)
                json_tree = objectpath.Tree(json_data)
                try:
                    val_fetch_from_tree_sensor(ele, json_tree, 'ip', 'ip'),
                    # print json_tree.execute('$.datum')   # == query JSON string for item: >datum<
                    val_fetch_from_tree_sensor(ele, json_tree, 'time', 'unix_time'),

                    val_fetch_from_tree_sensor(ele, json_tree, 'daten.esp8266id', 'esp8266id'),
                    # print '$.daten.esp8266id', json_tree.execute('$.daten.esp8266id')
                    val_fetch_from_tree_sensor(ele, json_tree, 'daten.software_version', 'software_version'),
                    # print 'software_version', json_tree.execute('$.daten.software_version')
                    val_fetch_from_tree_sensor(ele, json_tree, 'daten.sensordatavalues', 'sensordatavalues'),
                    # print 'sensordatavalues', json_tree.execute('$.daten.sensordatavalues')
                    # print type(ele.sensordatavalues), ele.sensordatavalues

                    val_fetch_from_tree_sensor(ele, json_tree, 'datum', ''),
                    val_fetch_from_tree_sensor(ele, json_tree, 'zeit', 'uhrzeit'),

                    # query for first element of dict:

                    # query_str = "$.daten.sensordatavalues[@.value_type is 'SDS_P1'].value"
                    # value_type_generator = json_tree.execute(query_str)
                    # print val_set(ele, 'SDS_P1', list(value_type_generator)[0]),

                    val_fetch_from_tree_sensor_data(ele, json_tree, 'SDS_P1'),
                    val_fetch_from_tree_sensor_data(ele, json_tree, 'SDS_P2'),
                    val_fetch_from_tree_sensor_data(ele, json_tree, 'temperature'),
                    val_fetch_from_tree_sensor_data(ele, json_tree, 'humidity')

                    HrMnSec = ele.uhrzeit.split(':')
                    if len(HrMnSec[0]) == 1:
                        HrMnSec[0] = '0' + HrMnSec[0]
                        ele.uhrzeit = ':'.join(HrMnSec)
                    ele.zeit = ele.datum + ' ' + ele.uhrzeit
                    # print ele.zeit

                    ele.line_nr = cnt_line
                    data_table.append(ele)
                    cnt_ele += 1
                except:
                    cnt_fail_01 += 1
                    mssge_01 = data_file_name + ': ' + line + str(cnt_fail_01) + ' ERROR transform_json_file_to_column_table() ... val_fetch_*()'
            except:
                cnt_fail_02 += 1
                mssge_02 = data_file_name  + ': ' + line + str(cnt_fail_02) + ' ERROR transform_json_file_to_column_table() ... json.loads()'

    if mssge_01:
        p_utils.p_terminal_mssge_note_this(mssge_01)
        p_log_this(mssge_01)

    if mssge_02:
        p_utils.p_terminal_mssge_note_this(mssge_02)
        p_log_this(mssge_02)

    mssge = 'transform_json_file_to_column_table: >' + data_file_name + '<; lines total:' + str(cnt_line)
    p_log_this(mssge); print mssge
    # cnt_line == lines in file;
    # data_table    == data_table of ele (objects of type Data)
    # cnt_ele   == cnt   of ele (objects of type Data)
    return data_table, cnt_line, cnt_ele


def process_all_json_data_files(feinstaub_dir, fn_db):
    """
    Macht aus JSON Files eine große SQL-Tabelle.
    - für jede Data-File im Daten-Dir:
      - für jede Zeile:
        - transformiere die JSON-Struktur in eine Python-Liste
        - hänge diese Liste an eine interne python-Tabelle
      - wenn Data-File vollständig gelesen:
        - wenn Daten dieser File noch nicht in SQL-Tabelle:
          speichere sie in der SQL-Tabelle
    Kern ist die Format-Unwandlung JSON -> orthogonale aber nicht normierte SQL-Tabelle
    """

    msge = 'Dir of JSON -Data files: >' + str(feinstaub_dir) + '<'
    p_log_this(msge)
    table = []

    data_files = get_data_files(feinstaub_dir)
    cnt_files = 0 ;  cnt_ele = 0;  cnt_ok = 0
    for data_file_name in sorted(data_files):
        if p_utils.p_file_exists(data_file_name):
            cnt_files += 1
            mssge = 'File #' + str(cnt_files) + '  ' + data_file_name
            p_utils.p_terminal_mssge_note_this(mssge)

            table[:] = []
            # cnt_line == lines in file;
            # table    == table of ele (objects of type Data)
            # cnt_ele  == cnt   of ele (objects of type Data)
            table, cnt_lines, cnt_ele = transform_json_file_to_column_table(data_file_name)

            # https://wiki.python.org/moin/HowTo/Sorting
            tmp_table = sorted(table, key=attrgetter('unix_time', 'esp8266id'))
            table [:] = []; table = tmp_table
            # print ' len(table): ', len(table), ' nach tmp_table = sorted(table, ... )'

            # make new csv-file with identical fn but extension == 'csv':
            # fn_csv = ('.').join(data_file_name.split('.')[:-1]) + '.csv'
            # fn_csv = os.path.normpath(fn_csv)
            # print fn_csv
            # write_csv(table, fn_csv)

            data_file_base_name = os.path.basename(os.path.normpath(data_file_name))
            # Sind die Daten schon in der sqlite-Tabelle? <=> insertdate != ''
            insert_date = check_fn_data_in_db(fn_db, 'saved_files', data_file_base_name, cnt_lines, cnt_ele)

            if not insert_date:
                msge = '>' + data_file_name + '<: data will be inserted!'
                print msge; p_log_this(msge)
                # Hier und jetzt werden die Daten in die Tabelle geschrieben:
                cnt, cnt_ok, cnt_fail = insert_data_in_db(table, fn_db, 'fstb', 'fstb_JSON', data_file_name)
                #
                # Jetzt noch eintragen, dass die Daten aus der File >data_file_base_name< verarbeitet wurden:
                if check_fn_in_db(fn_db, 'saved_files', data_file_base_name):
                    msge = "deleting: " + data_file_base_name + " from: " + fn_db
                    print msge; p_log_this(msge)
                    delete_fn_in_db(fn_db, 'saved_files', data_file_base_name)
                insert_fn_in_db(fn_db, 'saved_files', data_file_base_name, cnt_lines, cnt, cnt_ok, cnt_fail)
            else:
                msge = '>' + data_file_name + '<: already inserted (' + insert_date + ')'
                print msge; p_log_this(msge)
        else:
            p_utils.p_terminal_mssge_error('File not found: ' + data_file_name)

