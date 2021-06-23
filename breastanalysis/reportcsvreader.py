#-------------------------------------------------------------------------------
# Name:        reportdatelib
# Purpose:     get information from csvfile generated from CMUH HIS system
#              and access report finished date
#
# Author:      kpchang
#
# Created:     16/08/2016
# Copyright:   (c) kpchang 2016
# Licence:     <your licence>
#-------------------------------------------------------------------------------
import csv
import pathlib
import sqlite3

def takereportcsv(filename, dbname, sourcename):
    """
        Import a csvfile of structure provided by CMUH, into newdb in dbname,
        with report source sourcename
    """
    li = []
    newdb = sqlite3.connect(dbname)
    create_duedate_table(newdb)
    sourcedb = sqlite3.connect(sourcename)
    with open(filename, newline="", encoding="utf-8") as csvfile:
        spamreader = csv.reader(csvfile)
        print("reading the csv file {0}".format(filename))
        for row in spamreader:
            if len(row[9]) == 7:
                year = int(row[9][:3]) + 1911
                month = int(row[9][3:5])
                day = int(row[9][5:])
            else:
                (year, month, day) = (-1,-1,-1)
            id = row[2]
            doc = row[7]
            found_report = find_report_by_number(id, sourcedb)
            if not found_report:
                (category, diagnosis_column) = (-1, "")
            else:
                (category, diagnosis_column)= find_report_by_number(id, sourcedb)
            if category == 0:
                write_path_table_duedate(id, category, doc, year, month, day,
                                        diagnosis_column, newdb)
    newdb.commit()
    newdb.close()
    sourcedb.close()

def read_db_by_duedate(year, month, day, connection):
    c = connection.cursor()
    c.execute('''SELECT * FROM reports_finishdate
            WHERE finish_year = ? AND finish_month = ? AND finish_day = ?''', (year, month, day))
    return c.fetchall()

def create_duedate_table(connection):
    """
        Create a sql table of particular format in given db if the table does
        not exist. Requires a sql connection as argument.
    """
    try:
        c = connection.cursor()
        c.execute('''CREATE TABLE reports_finishdate
        (pathology_number    text    PRIMARY KEY,
        pathology_category  int,
        report_physician    text,
        finish_year        int,
        finish_month       int,
        finish_day         int,
        diagnosis_column    text)
        ''')
        connection.commit()
    except sqlite3.OperationalError:
        pass

def find_report_by_number(pathnumber, connection):
    c = connection.cursor()
    c.execute('''SELECT pathology_category, diagnosis_column FROM reports
                  WHERE pathology_number=?''', (pathnumber,))
    connection.commit()
    return c.fetchone()

def write_path_table_duedate(id, category, doc, year, month, day, diagnosis, connection):
    """
        Write a pathologic report into a connected database.
    """
    c = connection.cursor()
    try:
        print(id, category, doc, year, month, day, diagnosis)
        c.execute('''INSERT INTO reports_finishdate
            (pathology_number, pathology_category, report_physician,
             finish_year, finish_month, finish_day, diagnosis_column)
            VALUES(?,?,?,?,?,?,?)''', (id, category, doc, year, month, day, diagnosis))
    except sqlite3.IntegrityError:
        pass

def main():
    takereportcsv("D:\\報告存檔\\list20150701_20151231.csv", "D:\\報告存檔\\duedatedb.db",
                           "D:\\報告存檔\\testcase2015.db")
    newdb = sqlite3.connect("D:\\報告存檔\\duedatedb.db")
    print(read_db_by_duedate(2015, 8, 1, newdb))

if __name__ == '__main__':
    main()
