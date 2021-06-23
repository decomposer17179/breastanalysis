#-------------------------------------------------------------------------------
# Name:        breastanalysis.py
# Purpose:     functions for analysis of breast report
#
# Author:      kpchang
#
# Created:     13/03/2016
# Copyright:   (c) kpchang 2016
# Licence:     <your licence>
#-------------------------------------------------------------------------------
import re
import reportreader
import breastmacro

#testing change in git

def breast_classify(report):
    """
        Classify a report processed by reportreader.read_report into
        (organ, invasive/CIS/benign, diagnosis).
        If the diagnosis is not relavant to breast,
        would be reported as NOT_BREAST_RELATED
    """
    numbered = False
    if report[reportreader.DIAG_COLUMN] == []:
        return(breastmacro.NOT_BREAST_RELATED, breastmacro.NOT_BREAST_RELATED, breastmacro.NOT_BREAST_RELATED)
    for item in report[reportreader.DIAG_COLUMN]:
        if item.strip() != "":
            if item.strip()[0].isdigit():
                numbered = True
                break
        else:
            continue
    k = 0
    if numbered:
        while 1:
            if report[reportreader.DIAG_COLUMN][k].strip() == "":
                k = k+1
                continue
            elif report[reportreader.DIAG_COLUMN][k].strip()[0].isdigit():
                break
            else:
                k = k+1
                continue
    else:
        while len(report[reportreader.DIAG_COLUMN]) < (k-1):
            if len(report[reportreader.DIAG_COLUMN]) <= 1:
                break
            elif "report" in report[reportreader.DIAG_COLUMN][k]:
                k = k+1
                continue
            elif "ddend" in report[reportreader.DIAG_COLUMN][k]:
                k = k+1
                continue
            elif "revised" in report[reportreader.DIAG_COLUMN][k]:
                k = k+1
                continue
            else:
                break
    main_diagnosis = report[reportreader.DIAG_COLUMN][k]
    diag_dict = reportreader.take_diag_data(main_diagnosis)
    #print(main_diagnosis)
    #print(diag_dict)
    if diag_dict[reportreader.ORGANNAME] == breastmacro.BREAST:
        for item in diag_dict[reportreader.DIAGNOSIS]:
            for diagnosis in breastmacro.invasive_list:
                if diagnosis in item:
                    return (breastmacro.BREAST, breastmacro.INVASIVE_CARCINOMA, diagnosis)
            for diagnosis in breastmacro.insitu_list:
                if diagnosis in item:
                    return (breastmacro.BREAST, breastmacro.CIS, diagnosis)
            return (breastmacro.BREAST, breastmacro.BENIGN, breastmacro.BENIGN)
    else:
        for item in diag_dict[reportreader.DIAGNOSIS]:
            if breastmacro.BREAST in item:
                return (diag_dict[reportreader.ORGANNAME], breastmacro.INVASIVE_CARCINOMA,
                        breastmacro.METAC)
        return(diag_dict[reportreader.ORGANNAME], breastmacro.NOT_BREAST_RELATED, breastmacro.NOT_BREAST_RELATED)

def find_erprher(report):
    """
        take the ihc got by reportreader.read_report and reportreader.take_ihc
        report ER, PR, Her-2 result
    """
    result = {breastmacro.ER: None, breastmacro.PR: None, breastmacro.AR: None, breastmacro.HERTWO: None}
    ihc_data = reportreader.take_ihc(report)
    if not ihc_data:
        return result
    for item in ihc_data:
        loweritem = item.lower()
        if result[breastmacro.ER] == None:
            if re.search("er\ *[\:\(]", loweritem):
                if reportreader.POSITIVE in loweritem or "+" in loweritem:
                    result[breastmacro.ER] = (reportreader.POSITIVE, find_percentage(loweritem))
                else:
                    for item in ["1/4", "2/4", "3/4", "4/4"]:
                        if item in loweritem:
                            result[breastmacro.ER] = (reportreader.POSITIVE, -1)
                    if result[breastmacro.ER] == None:
                        result[breastmacro.ER] = (reportreader.NEGATIVE, 0)
        if result[breastmacro.AR] == None:
            if re.search("ar\ *[\:\(]", loweritem):
                if reportreader.POSITIVE in loweritem or "+" in loweritem:
                    result[breastmacro.AR] = (reportreader.POSITIVE, find_percentage(loweritem))
                else:
                    for item in ["1/4", "2/4", "3/4", "4/4"]:
                        if item in loweritem:
                            result[breastmacro.AR] = (reportreader.POSITIVE, -1)
                    if result[breastmacro.AR] == None:
                        result[breastmacro.AR] = (reportreader.NEGATIVE, 0)
        if result[breastmacro.PR] == None:
            if re.search("pg?r\ *[\:\(]", loweritem):
                if reportreader.POSITIVE in loweritem or "+" in loweritem:
                    result[breastmacro.PR] = (reportreader.POSITIVE, find_percentage(loweritem))
                else:
                    for item in ["1/4", "2/4", "3/4", "4/4"]:
                        if item in loweritem:
                            result[breastmacro.PR] = (reportreader.POSITIVE, -1)
                    if result[breastmacro.PR] == None:
                        result[breastmacro.PR] = (reportreader.NEGATIVE, 0)
        if result[breastmacro.HERTWO] == None:
            if re.search("her-*2", loweritem):
                result[breastmacro.HERTWO] = find_her_score(loweritem)
    return result
def find_percentage(ihcstr):
    """
        find ER, PR data processed by reportreader.read_report and reportreader.take_ihc
        and report percentage.
    """
    match = re.findall("[0-9]+%", ihcstr)
    #print(match)
    if len(match) == 1:
        return int(match[0][:-1])
    elif len(match) == 0:
        return None
    else:   #林醫師的特殊寫法，會有多個percentage
        if re.search("[0-9]+\%,", ihcstr): #有報一個總量
            return int(match[0][:-1])
        else:
            i = 0
            for item in match:
                i = i + int(item[:-1])
            return i

def find_her_score(ihcstr):
    """
        find Her-2 score processed by reportreader.read_report and reportreader.take_ihc
        and report score.
    """
    #print(ihcstr)
    match = re.search("score\ [0-9]", ihcstr)
    if match:
        return int(match.group(0).split(" ")[1])
    else:
        match = re.search("[0-9]\+", ihcstr)
        if match:
            return int(match.group(0)[0])
        elif ("+3") in ihcstr:
            return 3
        elif ("+2") in ihcstr:
            return 2
        elif ("+++/+++") in ihcstr:
            return 3
        elif ("++/+++") in ihcstr:
            return 2
        elif ("+/+++") in ihcstr:
            return 1
        elif "neg" in ihcstr:
            return 0
        elif "-" in ihcstr:
            return 0
        else:
            return None

def main():
    pass

if __name__ == '__main__':
    main()
