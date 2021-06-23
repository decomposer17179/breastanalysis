#-------------------------------------------------------------------------------
# Name:        readlist.py
# Purpose:     read specific list to get pathology number and report date
#              (???????????)
# Author:      user
#
# Created:     14/03/2016
# Copyright:   (c) user 2016
# Licence:     <your licence>
#-------------------------------------------------------------------------------
import pathlib
import zipfile
import codecs
import io
from queue import Queue

def read_list_file(file):
    """
        ????抬魉??????stfile??????????????????????????(10206, 201320546)
    """
    linelist = []
    for line in file:
        if line[0].isdigit():
            rawline = line.split()
            linelist.append((rawline[0][:-2], rawline[-1]))
    return linelist

def read_month_from_file(file):
    """
        read the first line of file (format: 61/F/1050601, at first line of file)
        then take month from the file.
    """
    line = file.readline()
    month = line.split("/")[2]
    return month[:-3]


def make_dic_from_folder(pat, queue = None):
    """
        read a folder of files and categorize the files according to months
    """
    targetdic = {}
    filedic = {}
    p = pathlib.Path(pat)
    filelist = list(p.glob('*.txt'))
    if queue:
        q = True
    else:
        q = False
    for item in filelist:
        #print(item)
        itemname = item.parts[-1][:-4]
        if q:
            queue.put(itemname)
        if not item.is_file():
            continue
        if not itemname[0].isdigit():
            continue
        with item.open(mode="r", encoding="big5", errors="ignore") as f:
            filedic[itemname] = f.read()
        with io.StringIO(filedic[itemname]) as f:
            month = read_month_from_file(f)
        if month not in targetdic.keys():
            targetdic[month] = [itemname]
                #print(item[0])
        else:
            targetdic[month].append(itemname)
    return targetdic, filedic

def make_dic_from_folder_new(pat, queue = None):
    """
        read a folder of files and categorize the files according to months
    """
    targetdic = {}
    filedic = {}
    p = pathlib.Path(pat)
    filelist = list(p.glob('*.txt'))
    if queue:
        q = True
    else:
        q = False
    for item in filelist:
        #print(item)
        itemname = item.parts[-1][:-4]
        if q:
            queue.put(itemname)
        if not item.is_file():
            continue
        if not itemname[0].isdigit():
            continue
        with item.open(mode="r", encoding="big5", errors="ignore") as f:
            filedic[itemname] = f.read()
        with io.StringIO(filedic[itemname]) as f:
            month = read_month_from_file(f)
        if month not in targetdic.keys():
            targetdic[month] = [itemname]
                #print(item[0])
        else:
            targetdic[month].append(itemname)
    return targetdic, filedic

def make_dic_from_zip(zipf):
    """
        read a zipfile of files and categorize the files according to months
    """
    targetdic = {}
    with zipfile.ZipFile(zipf) as myzip:
        for item in myzip.namelist():
            if "txt" not in item:
                continue
            #print(item)
            with myzip.open(item) as f:
                fnew = io.TextIOWrapper(f, encoding="big5")
                month = read_month_from_file(fnew)
            if month not in targetdic.keys():
                targetdic[month] = [item]
                #print(item[0])
            else:
                targetdic[month].append(item)
    return targetdic


def main():
    print(make_dic_from_folder("D:\\backup"))

if __name__ == '__main__':
    main()
