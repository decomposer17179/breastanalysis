#-------------------------------------------------------------------------------
# Name:        module1
# Purpose:
#
# Author:      kpchang
#
# Created:     14/03/2016
# Copyright:   (c) kpchang 2016
# Licence:     <your licence>
#-------------------------------------------------------------------------------
import sys
import json
import csv
import pathlib
import breastmacro
import breastanalysis
import reportreader
import readlist

def find_list_file(customdir): # 聰文改格式以後不用了
    cpath = pathlib.Path(customdir)
    listfilename = list(cpath.glob("list*"))[0]
    if not listfilename:
        raise ValueError("no list file present, please suggest other directory!")
    else:
        return listfilename

def take_monthlist_data_inv(breastlist):
    """
        take json dumped rawdata of breast, and count various data
    """
    #print("  Total invasive case: {0}".format(len(breastlist)))
    er_positive = 0
    pr_positive = 0
    hertwo_zero = 0
    hertwo_one = 0
    hertwo_two = 0
    hertwo_three = 0
    for item in breastlist:
        erresult = item[breastmacro.ER]
        prresult = item[breastmacro.PR]
        hertworesult = item[breastmacro.HERTWO]
        if erresult:
            if erresult[0] == reportreader.POSITIVE:
                er_positive += 1
        if prresult:
            if prresult[0] == reportreader.POSITIVE:
                pr_positive += 1
        if hertworesult != None:
            if hertworesult == 0:
                hertwo_zero += 1
            elif hertworesult == 1:
                hertwo_one += 1
            elif hertworesult == 2:
                hertwo_two += 1
            elif hertworesult == 3:
                hertwo_three += 1
    return (len(breastlist), er_positive, pr_positive, hertwo_zero, hertwo_one, hertwo_two, hertwo_three)


def main():
    maindir = "D:\\6月"
    if not maindir:
        print ("Please give a directory name")
        exit()
    else:
        maindirpath = pathlib.Path(maindir)
        if not maindirpath.is_dir:
            print("Invalid directory")
            exit()
    resultdirpath = pathlib.Path(maindir + "\\result")
    if not resultdirpath.is_dir():
        resultdirpath.mkdir()
    print("Reading database....")
    maindic = readlist.make_dic_from_folder(maindir)[1]
    #print(maindic)
    #write crude data
    csvpath = maindir + "\\result\\result.csv"
    with open(csvpath, mode = "w", encoding = "utf-8", newline="") as csvfile:
        spamwriter = csv.writer(csvfile)
        for k in maindic.keys():
            currentreport = reportreader.read_report(maindir + "\\" + k + ".txt")
            print(k, currentreport[reportreader.DIAG_COLUMN])
            spamwriter.writerow([k, currentreport[reportreader.DIAG_COLUMN]])
        """
                currentclass = breastanalysis.breast_classify(currentreport)
                ihcresult = breastanalysis.find_erprher(currentreport)
                if currentclass[0] == breastmacro.BREAST and currentclass[1] == breastmacro.INVASIVE_CARCINOMA:
                    invdic = {}
                    if ihcresult[breastmacro.ER]:
                        invdic[breastmacro.ER] = list(ihcresult[breastmacro.ER])
                    else:
                        invdic[breastmacro.ER] = None
                    if ihcresult[breastmacro.AR]:
                        invdic[breastmacro.AR] = list(ihcresult[breastmacro.AR])
                    else:
                        invdic[breastmacro.AR] = None
                    if ihcresult[breastmacro.PR]:
                        invdic[breastmacro.PR] = list(ihcresult[breastmacro.PR])
                    else:
                        invdic[breastmacro.PR] = None
                    invdic[breastmacro.HERTWO] = ihcresult[breastmacro.HERTWO]
                    invdic[reportreader.ORGANNAME] = breastmacro.BREAST
                    invdic[breastmacro.BREASTCLASS] = breastmacro.INVASIVE_CARCINOMA
                    invdic[reportreader.DIAGNOSIS] = currentclass[2]
                    invdic[reportreader.PATHOLOGYNO] = item
                    invasive_raw.append(invdic)
                if currentclass[0] == breastmacro.BREAST and currentclass[1] == breastmacro.CIS:
                    cisdic = {}
                    if ihcresult[breastmacro.ER]:
                        cisdic[breastmacro.ER] = list(ihcresult[breastmacro.ER])
                    else:
                        cisdic[breastmacro.ER] = None
                    if ihcresult[breastmacro.PR]:
                        cisdic[breastmacro.PR] = list(ihcresult[breastmacro.PR])
                    else:
                        cisdic[breastmacro.PR] = None
                    cisdic[reportreader.ORGANNAME] = breastmacro.BREAST
                    cisdic[breastmacro.BREASTCLASS] = breastmacro.CIS
                    cisdic[reportreader.DIAGNOSIS] = currentclass[2]
                    cisdic[reportreader.PATHOLOGYNO] = item
                    cis_raw.append(cisdic)
        with open(invfname, encoding="utf-8", mode="w") as f:
            json.dump(invasive_raw, f)
        with open(cisfname, encoding="utf-8", mode="w") as f:
            json.dump(cis_raw, f)
    print("Rawdata writing done.")
    listrawinv = list(resultdirpath.glob("*inv.json"))
    csvpath = maindir + "\\result\\result.csv"
    with open(csvpath, mode = "w", encoding = "utf-8", newline="") as csvfile:
        spamwriter = csv.writer(csvfile)
        spamwriter.writerow(["month", "total invasive", "ER positive",
            "PR positive", "HER2 score 0", "HER2 score 1+",
            "HER2 score 2+", "HER2 score 3+"])
        for item in listrawinv:
            itemcsv = maindir + "\\result\\" + item.name[:5] + "invlist.csv"
            currentmonth = item.name[:5]
            with item.open(mode = "r", encoding="utf-8") as f:
                itemdata = json.load(f)
        #print(item)
            with open(itemcsv, mode = "w", encoding = "utf-8", newline="") as itemcsvfile:
                secondspamwriter = csv.writer(itemcsvfile)
                for data in itemdata:
                    secondspamwriter.writerow([data[reportreader.PATHOLOGYNO], data[reportreader.DIAGNOSIS],
                        "ER:{0}".format(data[breastmacro.ER]), "PR:{0}".format(data[breastmacro.PR]),
                        "Her-2:{}".format(data[breastmacro.HERTWO])])
            ihcdata = take_monthlist_data_inv(itemdata)
            spamwriter.writerow([currentmonth] + list(ihcdata))
            """


if __name__ == '__main__':
    main()
