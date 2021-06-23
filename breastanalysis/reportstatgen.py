#-------------------------------------------------------------------------------
# Name:        reportstatgen
# Purpose:     Generate report statistics from sql library
#
# Author:      kpchang
#
# Created:     24/07/2016
# Copyright:   (c) kpchang 2016
# Licence:     <your licence>
#-------------------------------------------------------------------------------
import sqlite3
import time
import pathlib
import csv
import pathconstruct
from pathsqlite import reportsqlwriter
from reportreading import diagclassifier

def report_statistics(reportli):
    """
        take a list of tuples of format
        (id, organcode, diagcode, organ, diag),
        then count ocurrence of (organcode, diagcode).
    """
    reportdic = {}
    for i in range(49): #total 49 organs
        for j in range(11): #total 11 classification
            #print(i, j)
            reportdic[(i,j)] = 0
    for item in reportli:
        #print(item)
        (organ, diag) = (item[1], item[2])
        reportdic[(organ, diag)] += 1
    return reportdic

def take_monthly_data(connection, year, month):
    """
        take an already-open pathology database, take monthly data.
    """
    c = connection.cursor()
    c.execute('''SELECT pathology_number, diagnosis_column, patient_id FROM reports
            WHERE pathology_category = 0 and year = ? and month = ?
            ''', (year, month))
    print("Extracting data in {0}/{1}...".format(year, month))
    return c.fetchall()

def take_monthly_data_mol(connection, year, month):
    """
        take an already-open pathology database, take monthly data.
    """
    c = connection.cursor()
    c.execute('''SELECT pathology_number, diagnosis_column, patient_id FROM reports
            WHERE pathology_category = 3 and year = ? and month = ?
            ''', (year, month))
    print("Extracting data in {0}/{1}...".format(year, month))
    return c.fetchall()

def take_monthly_data_nocategory(connection, year, month):
    """
        take an already-open pathology database, take monthly data.
    """
    c = connection.cursor()
    c.execute('''SELECT pathology_number, diagnosis_column, patient_id FROM reports
            WHERE year = ? and month = ?
            ''', (year, month))
    print("Extracting data in {0}/{1}...".format(year, month))
    return c.fetchall()

def take_monthly_data_cyto(connection, year, month):
    """
        take an already-open pathology database, take monthly data.
    """
    c = connection.cursor()
    c.execute('''SELECT pathology_number, diagnosis_column, patient_id FROM reports
            WHERE pathology_category = 2 and year = ? and month = ?
            ''', (year, month))
    print("Extracting data in {0}/{1}...".format(year, month))
    return c.fetchall()


def take_monthly_data_crude(connection, year, month):
    """
        take an already-open pathology database, take monthly data.
    """
    c = connection.cursor()
    c.execute('''SELECT pathology_number, diagnosis_column, description_column, patient_id FROM reports
            WHERE pathology_category = 0 and year = ? and month = ?
            ''', (year, month))
    print("Extracting data in {0}/{1}...".format(year, month))
    return c.fetchall()

def take_monthly_data_crude_p(connection, year, month):
    """
        take an already-open pathology database, take monthly data.
    """
    c = connection.cursor()
    c.execute('''SELECT pathology_number, diagnosis_column, description_column, patient_id FROM reports
            WHERE pathology_category = 4 and year = ? and month = ?
            ''', (year, month))
    print("Extracting out data in {0}/{1}...".format(year, month))
    return c.fetchall()

def take_monthly_data_crude_n(connection, year, month):
    """
        take an already-open pathology database, take monthly data.
    """
    c = connection.cursor()
    c.execute('''SELECT pathology_number, diagnosis_column, description_column, patient_id FROM reports
            WHERE pathology_category = 4 and year = ? and month = ?
            ''', (year, month))
    print("Extracting south data in {0}/{1}...".format(year, month))
    return c.fetchall()

def take_monthly_diag(connection, year, month):
    """
        Extract diagnosis classification of all cases in a month in an
        already-open pathology database.
    """
    crudedata = take_monthly_data(connection, year, month)
    diaglist = []
    for item in crudedata:
        #seperate the diagnostic column from sql into a list of lines for diagclassifier
        li = item[1].split("|")
        (organcode, diagcode, organ, diag) = diagclassifier.first_diag_classify(li)
        diaglist.append((item[0], organcode, diagcode, organ, diag))
    return diaglist

def take_monthly_diag_mela(connection, year, month):
    crudedata = take_monthly_data(connection, year, month)
    diaglist = []
    for item in crudedata:
        #seperate the diagnostic column from sql into a list of lines for diagclassifier
        li = item[1].split("|")
        (organcode, diagcode, organ, diag) = diagclassifier.first_diag_classify(li)
        if diagcode == diagclassifier.MELANOMA:
            diaglist.append((item[0], organcode, diagcode, organ, diag, li[0]))
    print(diaglist)
    return diaglist

def write_yearly_stat(connection, year, csvpath, csvlock = ""):
    if csvlock == "":
        statcsvname = csvpath + str(year) + "_stat.csv"
    else:
        statcsvname = csvlock
    for mo in range(12):
        month = mo + 1
        curr_date = str(year) + "/" + str(month)
        monthdata = take_monthly_diag(connection, year, month)
        statdata = report_statistics(monthdata)
        with open(statcsvname, "a", encoding = "utf-8", newline='') as statcsv:
            spamwriter = csv.writer(statcsv)
            #spamwriter.writerow(["month", "organ", "diagnosis", "number"])
            for item in statdata.keys():
                (organ, diagnosis) = (diagclassifier.organ_code_name[item[0]],
                                diagclassifier.diag_code_name[item[1]])
                spamwriter.writerow([curr_date, organ, diagnosis, statdata[item]])


def write_monthly_data(connection, year, month, csvpath):
    """
        take an already-open pathology database, write monthly data.
        require a place to write csv, give a csvpath.
    """
    casecsvname = csvpath + str(year) + "_" + str(month) + "_cases.csv"
    statcsvname = csvpath + str(year) + "_" + str(month) + "_stat.csv"
    monthdata = take_monthly_diag(connection, year, month)
    statdata = report_statistics(monthdata)
    with open(casecsvname, "w", encoding = "utf-8", newline='') as casecsv:
        spamwriter = csv.writer(casecsv)
        for item in monthdata:
            spamwriter.writerow(item)
    with open(statcsvname, "w", encoding = "utf-8", newline='') as statcsv:
        spamwriter = csv.writer(statcsv)
        for item in statdata.keys():
            (organ, diagnosis) = (diagclassifier.organ_code_name[item[0]],
                                diagclassifier.diag_code_name[item[1]])
            spamwriter.writerow([organ, diagnosis, statdata[item]])

def write_monthly_data_mela(connection, year, month, csvpath):
    """
        take an already-open pathology database, write monthly data.
        require a place to write csv, give a csvpath.
    """
    casecsvname = csvpath + str(year) + "_" + str(month) + "_cases.csv"
    statcsvname = csvpath + str(year) + "_" + str(month) + "_stat.csv"
    monthdata = take_monthly_diag_mela(connection, year, month)
    statdata = report_statistics(monthdata)
    with open(casecsvname, "w", encoding = "utf-8", newline='') as casecsv:
        spamwriter = csv.writer(casecsv)
        for item in monthdata:
            spamwriter.writerow(item)
    with open(statcsvname, "w", encoding = "utf-8", newline='') as statcsv:
        spamwriter = csv.writer(statcsv)
        for item in statdata.keys():
            (organ, diagnosis) = (diagclassifier.organ_code_name[item[0]],
                                diagclassifier.diag_code_name[item[1]])
            spamwriter.writerow([organ, diagnosis, statdata[item]])

def main():
    folderpath = pathlib.Path("D:\\報告存檔")
    dbpath = folderpath / "testcase2016.db"
    lockcsvpath = folderpath / "allstat.csv"
    lockcsv = str(lockcsvpath)
    before = time.time()
    conn = sqlite3.connect(str(dbpath))
    write_yearly_stat(conn, 2016, str(dbpath), lockcsv)
    conn.close()
    """
    for mo in range(12):
        month = mo + 1
        write_monthly_data(conn, 2015, month, str(dbpath))
    """


if __name__ == '__main__':
    main()
