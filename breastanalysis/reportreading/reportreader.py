#-------------------------------------------------------------------------------
# Name:        reportreader.py
# Purpose:     file reader module for breastanalysis, and maybe of future use in other analysis program
#
# Author:      kpchang
#
# Created:     12/03/2016
# Copyright:   (c) kpchang 2016
# Licence:     <your licence>
#-------------------------------------------------------------------------------
import re
import io

AGE_COLUMN = "age column"
DIAG_COLUMN = "diagnosis column"
DESC_COLUMN = "description column"
REF_COLOMN = "reference column"
ORGANNAME = "organ name"
POSITION = "position"
PROCEDURE = "procedure"
DIAGNOSIS = "diagnosis"
LEFT = "left"
RIGHT = "right"
POSITIVE = "positive"
NEGATIVE = "negative"
PATHOLOGYNO = "pathology number"


def read_report(fname):
    """
        聰文給我的檔案格式如下:
            年齡/性別
            [報告內容]
            .......................
            [描述]
            ......................
            Ref:
                ..................
        所以，每一個檔案都可以照例拆成
        ([年齡, 性別], [報告行1, 報告行2...], [描述行1, 描述行2],
         [Ref行1, Ref行2])這樣。
    """
    container = {AGE_COLUMN:[], DIAG_COLUMN:[], DESC_COLUMN:[], REF_COLOMN:[]}
    column_list = [AGE_COLUMN, DIAG_COLUMN, DESC_COLUMN, REF_COLOMN]
    with open(fname, mode = "r", encoding = "big5", errors="ignore") as f: #很不幸的聰文還在用大五碼
        n = 1 #行號
        itemno = 0 # 項次
        for line in f:
            if n == 1: #第一行
                container[AGE_COLUMN] = line.strip().split("/")
                if not container[AGE_COLUMN][0].isdecimal():
                    raise ValueError("這報告不是聰文的格式!")
                n +=1
            elif line.startswith("[") or line.strip().startswith("Ref") or line.strip().lower().startswith("macro"):
                if itemno < 3:
                    itemno += 1  #遇到[報告內容][描述]或[Ref]跳一項
            else:
                container[column_list[itemno]].append(line)
    return container

def read_report_text(txt):
    """
        聰文給我的檔案格式如下:
            年齡/性別
            [報告內容]
            .......................
            [描述]
            ......................
            Ref:
                ..................
        所以，每一個檔案都可以照例拆成
        ([年齡, 性別], [報告行1, 報告行2...], [描述行1, 描述行2],
         [Ref行1, Ref行2])這樣。
    """
    container = {AGE_COLUMN:[], DIAG_COLUMN:[], DESC_COLUMN:[], REF_COLOMN:[]}
    column_list = [AGE_COLUMN, DIAG_COLUMN, DESC_COLUMN, REF_COLOMN]
    with io.StringIO(txt) as f: #很不幸的聰文還在用大五碼
        n = 1 #行號
        itemno = 0 # 項次
        for line in f:
            if n == 1: #第一行
                container[AGE_COLUMN] = line.strip().split("/")
                if not container[AGE_COLUMN][0].isdecimal():
                    raise ValueError("這報告不是聰文的格式!")
                n +=1
            elif line.startswith("[") or line.strip().startswith("Ref") or line.strip().lower().startswith("macro"):
                if itemno < 3:
                    itemno += 1  #遇到[報告內容][描述]或[Ref]跳一項
            else:
                container[column_list[itemno]].append(line)
    return container

def take_diag_data(diag):
    """
        Take a line of diagnostic column and take the organ name, sideness, procedure,
        and diagnosis.
    """
    container = {ORGANNAME:"", POSITION:[], PROCEDURE:[], DIAGNOSIS:[]}
    if diag == "":
        return container
    wordlist = diag.split(",")
    if wordlist[0][0].isdigit():  #屬於有項次的報告
        try:
            wordlist[0] = wordlist[0].split(".")[1].strip()
        except IndexError:
            wordlist[0] = wordlist[0].strip()
    if "mmunohistochem" in wordlist[0] or "ncillary" in wordlist[0]:
        container[ORGANNAME] = "immune"
        return container
    elif "athologic stag" in wordlist[0]:
        container[ORGANNAME] = "stage"
        return container
    else:
        container[ORGANNAME] = wordlist.pop(0).strip().lower() #器官一定放在最前面
        if contain_procedure(wordlist):
            while not is_procedure(wordlist[0]): #有術士，術式出現之前都當作部位
                container[POSITION].append(wordlist.pop(0).strip())
            container[PROCEDURE].append(wordlist.pop(0).strip()) #抄術式
            container[DIAGNOSIS] = wordlist.copy()
        else:
            container[DIAGNOSIS] = wordlist.copy()
    return container

def take_ihc(report):
    """
        Take immunohistochemistry data from a diagnosis column or description.
        Will resort to take_ihc_from_desc if failed to take from diagnostic column.
    """
    ihc_list = []
    if report[DIAG_COLUMN] == None or report[DIAG_COLUMN] == []:
        return None
    for (k,v) in enumerate(report[DIAG_COLUMN]):   #Search diagnostic column for IHC line
        if "mmunohisto" in v or "ncillary" in v :
            #print(v)
            if v.strip().endswith(":") or (v.strip()[-2].isdigit() and v.strip()[-3].isalpha()): #林醫師的特殊分行寫法
                rownum = k+1
                if rownum >= len(report[DIAG_COLUMN]):
                    break
                else:
                    while not report[DIAG_COLUMN][rownum][0].isdigit(): #不斷往下找，直到下一個項目編號出現
                        if rownum > (len(report[DIAG_COLUMN])-2):
                            break
                        elif "consistent" not in report[DIAG_COLUMN][rownum]:
                            ihc_list.append(report[DIAG_COLUMN][rownum].strip())
                        rownum += 1
            else:  #正常寫法
                #print(v)
                ihc_list = split_ihc(v)
    if ihc_list == []:
        return take_ihc_from_desc(report)
    else:
        return(ihc_list)

def take_ihc_from_desc(report):
    """
        take immunohistochemistry data from a description.
    """
    for item in report[DESC_COLUMN]:
        #print(item)
        match = re.search("[Ii]mmunohisto.*\)", item)
        if match:
            return split_ihc(match.group(0))
    return None

def contain_procedure(diagli):
    for item in diagli:
        if is_procedure(item):
            return True
    return False



def split_ihc(ihcstr):
    #print(ihcstr)
    if ":" in ihcstr:
        ihccontent = ihcstr.split(":")[1]
    elif "shows" in ihcstr:
        ihccontent = ihcstr.split("shows")[1]
    elif "show" in ihcstr:
        ihccontent = ihcstr.split("show")[1]
    elif "reveal" in ihcstr:
        ihccontent = ihcstr.split("reveal")[1]
    else:
        return None
    return ihccontent.split(")")

def is_procedure(word):
    for procedure in ["surgery", "ectomy", "excision", "resection", "biopsy",
                    "section", "MRM", "otomy", "BCS", "etomy", "erctomy", "EMR",
                    "TUR", "SMR", "plasty", "debride", "rection", "endoscopic", "UPPP",
                    "replace", "TLIF", "rhaphy", "biospy", "FESS", "LMS", "shaving",
                    "remove", "D&C", "curettage", "take down", "curretage", "removal",
                    "aspiration", "biops", "LSC", "strip", "delivery", "EAC", "ectom",
                    "closure", "desis", "TAMIS", "amput"]:
        if procedure in word:
            if "status post" in word:
                continue
            else:
                return True
        else:
            continue

def read_bigfivef(fname):
    return

def main():
    report = read_report("testfile/201505812.txt")
    print(take_ihc(report))

if __name__ == '__main__':
    main()
