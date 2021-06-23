#-------------------------------------------------------------------------------
# Name:        reportsqlwriter
# Purpose:     Utility for sql writing of pathology reports
#
# Author:      kpchang
#
# Created:     24/07/2016
# Copyright:   (c) kpchang 2016
# Licence:     <your licence>
#-------------------------------------------------------------------------------

import sqlite3

PATIENTID = "patient_id"
PATHNO = "pathology_number"
FORMNO = "form_number"
APPNO = "application_number"
IDENTITYID = "identity_id"
PATHCATEGORY = "pathology_category"
AGE_COLUMN = "age column"
DIAG_COLUMN = "diagnosis column"
DESC_COLUMN = "description column"
REF_COLOMN = "reference column"

#Macros for filename resolvement
PATIENTID = "patient_id"
PATHNO = "pathology_number"
FORMNO = "form_number"
APPNO = "application_number"
IDENTITYID = "identity_id"
PATHCATEGORY = "pathology_category"

#Macros for pathology category
CMUHSP = 0 #Surgical pathology, China Medical University
CMUHGYN = 1 #Gynecological cytology, China Medical University
CMUHNONGYN = 2 #Non-gyn cytology, China Medical University
CMUHMOL = 3 #Molecular pathology, China Medical University
OUTSP = 4 #Surgical pathology, Beigang and other hospital
OUTGYN = 5 #Gynecological Cytology, Beigang and other hospital
OUTNONGYN = 6 #Non-gyn cytology, Beigang and other hospital
ANNANSP = 7 #Surgical pathology, An-nan
ANNANMOL = 8 #Molecular pathology, An-nan

#Macros for sql table
KEY_PATIENT_ID = 0
KEY_PATHNO = 1
KEY_FORMNO = 2
KEY_APPNO = 3
KEY_IDENTITY_ID = 4
KEY_CATEGORY = 5
KEY_YEAR = 6
KEY_MONTH = 7
KEY_DAY = 8
KEY_DIAG = 9
KEY_DESC = 10
KEY_REF = 11
KEY_AGE = 12
KEY_GENDER = 13

class PathReport(object):
    """
        A container to contain a pathology report of the format specified by
        CMUH HIS system.
    """
    def __init__(self):
        self.patient_id = -1
        self.pathology_number = ""
        self.form_number = ""
        self.application_number = -1
        self.pathology_category = -1
        self.identity_id = ""
        self.year = -1
        self.month = -1
        self.day = -1
        self.diagnosis_column = ""
        self.description_column = ""
        self.reference_column = ""
        self.age = -1
        self.gender = ""

    def take_pathnamereader(self, pathdic):
        """
            inject a dictionary generated from pathnamereader.resolve_filename into
            PathReport.
            Warning: Doesn't work if not generated from pathnamereader.resolve_filename!
        """
        try:
            if not pathdic[PATIENTID][0].isdigit():
                self.patient_id = -1
        except IndexError:
            self.patient_id = -1
        else:
            try:
                self.patient_id = int(pathdic[PATIENTID])
            except ValueError:
                self.patient_id = -1
        self.pathology_number = pathdic[PATHNO]
        self.form_number = pathdic[FORMNO]
        self.application_number = int(pathdic[APPNO])
        self.pathology_category = int(pathdic[PATHCATEGORY])
        self.identity_id = pathdic[IDENTITYID]

    def take_reportreader(self, content):
        """
            inject pathology report content generated from reportreader.read_report
            into PathReport.
            Warning: Doesn't work if not generated from reportreader.read_report!
        """
        age_column = content[AGE_COLUMN]
        curr_date = age_column[2]
        self.age = age_column[0]
        gender = age_column[1]
        if gender == "女":
            self.gender = "F"
        else:
            self.gender = "M"
        if curr_date == "":
            pass
        else:
            self.year = int(curr_date[0:3]) + 1911
            self.month = int(curr_date[3:5])
            self.day = int(curr_date[5:])
        # make the lists into texts separated by "|", so that they can
        # be inserted into sql column
        self.diagnosis_column = "|".join(content[DIAG_COLUMN])
        self.description_column = "|".join(content[DESC_COLUMN])
        self.reference_column = "|".join(content[REF_COLOMN])


def db_connect(dbname):
    return sqlite3.connect(dbname)

def create_path_table(connection):
    """
        Create a sql table of particular format in given db if the table does
        not exist. Requires a sql connection as argument.
    """
    try:
        c = connection.cursor()
        c.execute('''CREATE TABLE reports
        (patient_id         int,
        pathology_number    text    PRIMARY KEY,
        form_number         text,
        application_number  int,
        identity_id         text,
        pathology_category  int,
        year                int,
        month               int,
        day                int,
        diagnosis_column    text,
        description_column  text,
        reference_column    text,
        age                 int,
        gender              text)
        ''')
        connection.commit()
    except sqlite3.OperationalError:
        pass

def write_path_table(preport, connection):
    """
        Write a pathologic report into a connected database.
        Take only formatted pathologic report (see Class PathReport)
                self.patient_id = -1
        self.pathology_number = ""
        self.form_number = ""
        self.application_number = -1
        self.pathology_category = -1
        self.identity_id = ""
        self.year = -1
        self.month = -1
        self.date = -1
        self.diagnosis_column = ""
        self.description_column = ""
        self.reference_column = ""
    """
    c = connection.cursor()
    pentry = (preport.patient_id, preport.pathology_number,
              preport.form_number, preport.application_number,
              preport.identity_id, preport.pathology_category,
              preport.year, preport.month, preport.day,
              preport.diagnosis_column, preport.description_column,
              preport.reference_column, preport.age,
              preport.gender)
    try:
        c.execute('''INSERT INTO reports
            (patient_id, pathology_number, form_number,
            application_number, identity_id, pathology_category,
            year, month, day, diagnosis_column, description_column,
            reference_column, age, gender)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ''', pentry)
    except sqlite3.IntegrityError:
        pass

def main():
    pass

if __name__ == '__main__':
    main()
