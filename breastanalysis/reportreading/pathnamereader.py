#-------------------------------------------------------------------------------
# Name:        pathnamereader
# Purpose:     A module to read the filename containing patient data exported from
#              Health Information System of China Medical University Hospital
#
# Author:      kpchang
#
# Created:     23/07/2016
# Copyright:   (c) kpchang 2016
# Licence:     <your licence>
#-------------------------------------------------------------------------------

import pathlib

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


def list_file_name(folder):
    """
        Return list of files in a given folder, as a list of pathlib.Path.
    """
    curr_path = pathlib.Path(folder)
    if not curr_path.is_dir():
        raise TypeError("Not a folder")
    else:
        filelist = []
        for item in list(curr_path.glob('*.*')):
            filelist.append(item)
        return filelist

def resolve_file_name(path):
    """
        Resolve the filename containing patient data of particular format
        (PATIENTID_PATHNO_FORMNO_APPNO_IDENTITYID) into a dictionary containing
        patientid, pathno, formno, appno, identityid, and category
        Input: mustbe pathlib.Path
        Output: a dictionary
    """
    resolved_dic = {}
    namestr = path.name.split(".")[0]
    namelist = namestr.split("_")
    if namelist[1] == "":
        return None
    pathtoken_two = namelist[1][0:2]
    pathtoken_one = namelist[1][0]
    if len(namelist)!=5:
        raise(ValueError("Invalid file name: not from CMUH HIS"))
    category_twochar = {"PP": OUTNONGYN, "PS": OUTNONGYN, "TM": OUTGYN,
                        "NM": ANNANMOL, "MP": CMUHMOL}
    category_onechar = {"P": OUTSP, "S": CMUHNONGYN, "H": CMUHGYN, "N": ANNANSP}
    path_category = category_twochar.get(pathtoken_two)
    if not path_category:
        path_category = category_onechar.get(pathtoken_one)
    if not path_category:
        path_category = CMUHSP
    #print(path_category)
    namelist.append(path_category)
    keylist = [PATIENTID, PATHNO, FORMNO, APPNO, IDENTITYID, PATHCATEGORY]
    for (k,v) in enumerate(keylist):
        resolved_dic[keylist[k]] = namelist[k]
    return resolved_dic


def main():
    pass

if __name__ == '__main__':
    main()
