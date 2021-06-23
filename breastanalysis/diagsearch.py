#-------------------------------------------------------------------------------
# Name:        module1
# Purpose:
#
# Author:      kpchang
#
# Created:     11/09/2018
# Copyright:   (c) kpchang 2018
# Licence:     <your licence>
#-------------------------------------------------------------------------------
import sqlite3
import time
import pathlib
import csv
import re
from reportreading import diagclassifier
from pathsqlite import reportsqlwriter
from reportreading import diagclassifier
from reportreading import reportreader
import reportstatgen
import reportdatelib

NOAFS = 0
AFSNEG = 1
AFSPOS = 2

ACIDFASTNEGATIVELIST = ["negative for myco", "acid[- ]fast[^.]+no myco[^.]+\.",
    "[Nn]o[^.^;]+micro[^.^;]+acid[- ]fast", "acid[- ]fast[^.^;]+no micro",
    "[Nn]o[^.^;]+micro[^.^;]+AFS", "[Nn]o[^.^;]+myco[^.^;]+acid[- ]fast",
    "no acid[ -]fast[^.^;^,]+positive"]
ACIDFASTPOSITIVELIST = ["acid[- ]fast[^.^;^,]+positive", "presence[^.^;]+mycobacte",
    "acid[- ]fast[^.^;]+presence of[^.^;]+myco", "positive[^.^;]+mycobac"]

def diag_match_all(keywords, crudedata, exclusion = []):
    """
        Take a crudedata from report in database, search for any keywords.
    """
    match = 0
    li = crudedata.split("|")
    for item in li:
        #print(item)
        if reportreader.contain_procedure(item.split(",")):
            #print(item)
            if all(word in item.lower() for word in keywords):
                #print("match")
                if not any(word in item.lower() for word in exclusion):
                    match += 1
    return match

def diag_match_net(crudedata, exclusion = []):
    diagcode = None

def diag_match_re(expression, crudedata):
    """
        Take a crudedata from report in database, search for any keywords.
    """
    match = re.search(expression, crudedata)
    return match

def diag_match_acid(crudedata):
    for pattern in ACIDFASTNEGATIVELIST:
        if diag_match_re(pattern, crudedata):
            return AFSNEG
    for pattern in ACIDFASTPOSITIVELIST:
        if diag_match_re(pattern, crudedata):
            return AFSPOS
    return NOAFS


def diag_match_crude(keywords, crudedata, exclusion = []):
    """
        Take a crudedata from report in database, search for any keywords.
    """
    match = 0
    if all(word in crudedata.lower() for word in keywords):
        if not any(word in crudedata.lower() for word in exclusion):
            match += 1
    return match

def diag_match_any_crude(keywords, crudedata, exclusion = []):
    """
        Take a crudedata from report in database, search for any keywords.
    """
    match = 0
    if any(word in crudedata.lower() for word in keywords):
        if not any(word in crudedata.lower() for word in exclusion):
            match += 1
    return match

def search_month_for_words(year, month, keywords, connection, exc=[]):
    reportlist = reportstatgen.take_monthly_data(connection, year, month)
    result = []
    for item in reportlist:
        if diag_match_all(keywords, item[1], exclusion=exc):
            result.append(item)
    return result

def search_month_for_words_nocategory(year, month, keywords, connection, exc=[]):
    reportlist = reportstatgen.take_monthly_data_nocategory(connection, year, month)
    result = []
    for item in reportlist:
        if diag_match_all(keywords, item[1], exclusion=exc):
            result.append(item)
    return result

def search_month_for_words_include(year, month, keywords, connection, exc=[]):
    reportlist = reportstatgen.take_monthly_data_crude(connection, year, month)
    result = []
    for item in reportlist:
        if diag_match_all(keywords, item[1], exclusion=exc):
            result.append(item)
    return result

def search_month_for_words_crude_diag(year, month, keywords, connection, exc=[]):
    reportlist = reportstatgen.take_monthly_data(connection, year, month)
    result = []
    for item in reportlist:
        if diag_match_crude(keywords, item[1], exclusion=exc):
            result.append(item)
    return result

def search_month_for_words_re(year, month, expression, connection, exc=[]):
    reportlist = reportstatgen.take_monthly_data_crude(connection, year, month)
    result = []
    for item in reportlist:
        if "病理號" in item[2]:
            descolumn = item[2].split("病理號")[0]
        else:
            descolumn = item[2]
        match = diag_match_re(expression, item[1]+descolumn)
        if match:
            result.append((item[0], match.group(0), item[1], descolumn))
    return result

def search_month_for_words_acid_neg(year, month, connection, exc=[]):
    reportlist = reportstatgen.take_monthly_data_crude(connection, year, month)
    result = []
    for item in reportlist:
        if "病理號" in item[2]:
            descolumn = item[2].split("病理號")[0]
        else:
            descolumn = item[2]
        if diag_match_acid(item[1]+descolumn) == AFSNEG:
            result.append((item[0], item[1], descolumn, item[3]))
    return result

def search_month_for_words_acid_pos(year, month, connection, exc=[]):
    reportlist = reportstatgen.take_monthly_data_crude(connection, year, month)
    result = []
    for item in reportlist:
        if "病理號" in item[2]:
            descolumn = item[2].split("病理號")[0]
        else:
            descolumn = item[2]
        if diag_match_acid(item[1]+descolumn) == AFSPOS:
            result.append((item[0], item[1], descolumn, item[3]))
    return result

def search_month_for_words_cyto(year, month, keywords, connection, exc=[]):
    reportlist = reportstatgen.take_monthly_data_cyto(connection, year, month)
    result = []
    for item in reportlist:
        if diag_match_all(keywords, item[1], exclusion=exc):
            result.append(item)
    return result

def search_month_for_words_crude(year, month, keywords, connection, exc=[]):
    reportlist = reportstatgen.take_monthly_data_crude(connection, year, month)
    result = []
    for item in reportlist:
        if diag_match_crude(keywords, item[1]+item[2], exclusion=exc):
            result.append(item)
    return result

def search_month_for_words_crudediag(year, month, keywords, connection, exc=[]):
    reportlist = reportstatgen.take_monthly_data_crude(connection, year, month)
    result = []
    for item in reportlist:
        if diag_match_crude(keywords, item[1], exclusion=exc):
            result.append(item)
    return result

def search_month_for_words_any_crude(year, month, keywords, connection, exc=[]):
    reportlistin = reportstatgen.take_monthly_data_crude(connection, year, month)
    reportlistout = reportstatgen.take_monthly_data_crude_p(connection, year, month)
    reportlistsouth = reportstatgen.take_monthly_data_crude_n(connection, year, month)
    reportlist = reportlistin + reportlistout + reportlistsouth
    result = []
    for item in reportlist:
        if diag_match_any_crude(keywords, item[1]+item[2], exclusion=exc):
            result.append(item)
    return result

def search_year_for_words(year, keywords, connection, ex=[]):
    result = []
    for i in range(12):
        mo = i+1
        result = result + search_month_for_words(year, mo, keywords, connection, exc=ex)
    return result

def search_year_for_words_nocategory(year, keywords, connection, ex=[]):
    result = []
    for i in range(12):
        mo = i+1
        result = result + search_month_for_words_nocategory(year, mo, keywords, connection, exc=ex)
    return result

def search_year_for_words_include(year, keywords, connection, ex=[]):
    result = []
    for i in range(12):
        mo = i+1
        result = result + search_month_for_words_include(year, mo, keywords, connection, exc=ex)
    return result

def search_year_for_words_cyto(year, keywords, connection, ex=[]):
    result = []
    for i in range(12):
        mo = i+1
        result = result + search_month_for_words_cyto(year, mo, keywords, connection, exc=ex)
    return result

def search_year_for_words_crude(year, keywords, connection, ex=[]):
    result = []
    for i in range(12):
        mo = i+1
        result = result + search_month_for_words_crude(year, mo, keywords, connection, exc=ex)
    return result

def search_year_for_words_crude_diag(year, keywords, connection, ex=[]):
    result = []
    for i in range(12):
        mo = i+1
        result = result + search_month_for_words_crude_diag(year, mo, keywords, connection, exc=ex)
    return result

def search_year_for_words_any_crude(year, keywords, connection, ex=[]):
    result = []
    for i in range(12):
        mo = i+1
        result = result + search_month_for_words_any_crude(year, mo, keywords, connection, exc=ex)
    return result

def search_year_for_words_acid_neg(year, connection):
    result = []
    for i in range(12):
        mo = i+1
        result = result + search_month_for_words_acid_neg(year, mo, connection)
    return result

def search_year_for_words_acid_pos(year, connection):
    result = []
    for i in range(12):
        mo = i+1
        result = result + search_month_for_words_acid_pos(year, mo, connection)
    return result


def main():

    #duedbpath = folderpath / "duedatedb.db"
    #conn = sqlite3.connect(str(dbpath))
    #dueconn = sqlite3.connect(str(duedbpath))
    #print(reportstatgen.take_monthly_data_crude_n(conn, 2018, 1))
    #keywords = ["eber", "eber(", "eber,", "eber."]
    #li = search_year_for_words_acid_pos(2007, conn)

    """
    #li = search_month_for_words_re(2018, 3, "negative for myco", conn)
    #print(li)
    li = search_month_for_words_re(2018, 9, "acid[- ]fast[^.^;]+positive\.", conn)
    print(li)
    li = search_month_for_words_re(2018, 9, "presence[^.^;]+mycobacte", conn)
    print(li)
    #li = search_month_for_words_re(2018, 3, "acid[- ]fast[^.]+no myco[^.]+\.", conn)
    #print(li)
    #li = search_month_for_words_re(2018, 3, "[Nn]o[^.^;]+micro[^.^;]+acid[- ]fast", conn)
    #print(li)
    #li = search_month_for_words_re(2018, 3, "acid[- ]fast[^.^;]+no micro", conn)
    #print(li)
    li = search_month_for_words_re(2018, 9, "acid[- ]fast[^.^;]+presence of[^.^;]+myco", conn)
    print(li)
    #li = search_month_for_words_re(2018, 3, "[Nn]o[^.^;]+micro[^.^;]+AFS", conn)
    #print(li)
    #li = search_month_for_words_re(2018, 3, "[Nn]o[^.^;]+myco[^.^;]+acid[- ]fast", conn)
    #print(li)
    li = search_month_for_words_re(2018, 9, "positive[^.^;]+mycobac", conn)
    print(li)
    li = search_month_for_words_re(2018, 9, "no acid[ -]fast[^.^;^,]+positive", conn)
    print(li)
    """
    folderpath = pathlib.Path("D:\\2021db")
    dbpath = folderpath / "testcase2021.db"
    conn = sqlite3.connect(str(dbpath))
    c= conn.cursor()
    csvpath = folderpath / "result.csv"
    keywords = ["kikuchi"]
    exclusion = []
    li = search_year_for_words (2021, keywords, conn, exclusion)
    print(li)
    """
    newli = []
    for item in li:
        c.execute(SELECT pathology_number, diagnosis_column
                FROM reports WHERE
                pathology_category = 2 AND patient_id =?, (item[2],))
        cytolist = c.fetchall()
        if len(cytolist) > 0:
            cytonumber = cytolist[0][0]
            cytostring = cytolist[0][1]
            if "non-diagnostic" in cytostring.lower():
                cyto_diagnosis = "Non-diagnostic"
            elif "benign" in cytostring.lower():
                cyto_diagnosis = "Benign"
            elif "atypia" in cytostring.lower():
                cyto_diagnosis = "Atypia"
            elif "suspicious" in cytostring.lower():
                cyto_diagnosis = "Suspicious"
        pathology_number = item[0]
        if "papillary" in item[1].lower:
            diagnosis = "Papillary carcinoma"
        elif "medullary" in item[1].lower:
            diagnosis = "Medullary carcinoma"
        else:
            diagnosis = "Others"
        i.append(c.fetchall())
        newli.append(i)
        print(i)
    """
    conn.close()

    with csvpath.open("w", encoding="big5", newline="") as csvfile:
        spamwriter = csv.writer(csvfile)
        spamwriter.writerow(["病理號", "診斷及描述", "病歷號"])
        for item in li:
            spamwriter.writerow(item)


    """
    #dbpath = folderpath / "testcase2018.db"
    #conn = sqlite3.connect(str(dbpath))
    li_two = search_year_for_words_any_crude(2018, keywords, conn, exclusion)
    print(li_two)
    conn.close()
    """
    """
    dbpath = folderpath / "testcase2018.db"
    conn = sqlite3.connect(str(dbpath))
    li_three = search_year_for_words_any_crude(2018, keywords, conn, exclusion)
    conn.close()
    li = li + li_two + li_three
    newli = []
    for item in li:
        c = dueconn.cursor()
        c.execute('''SELECT report_physician FROM reports_finishdate
                  WHERE pathology_number=?''', (item,))
        doc = c.fetchone()
        if doc:
            newli.append((doc[0].split("(")[0], item))
    with open(str(csvpath), "w", encoding = "big5", newline='') as casecsv:
        spamwriter = csv.writer(casecsv)
        for item in newli:
            spamwriter.writerow(item)
    """

    """
    with csvpath.open("w", encoding="big5", newline="") as csvfile:
        spamwriter = csv.writer(csvfile)
        spamwriter.writerow(["病理號", "病歷號", "報告內容"])
        for item in li:
            spamwriter.writerow([item[0], item[2], item[1]])
    """


if __name__ == '__main__':
    main()
