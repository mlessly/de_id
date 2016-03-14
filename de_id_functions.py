##################################
#
# author: Jon Daries (daries@mit.edu)
#
# title: de_id_functions.py
#
# version: 5.0
#
# date last modified: 12/17/2014
#
# desc: Based on Latanya Sweeney's 'datafly' algorithm
#       but with different implementation specific to the
#       MITx/Harvardx Person-Course dataset
#       Contains functions for loading data from .csv
#       Generalizing and de-identifying variables
#       And checking for k-anonymity across variables
#       To be used as helper functions to the De-identification.ipynb
#       IPython Notebook
#       
##################################

from __future__ import unicode_literals

import cPickle
import csv
import datetime
import hashlib
import itertools
import math
import os
import random
import sqlite3
import string

import pycountry

from csv2sqlite import convert


########################
# Simple SQL commands as functions
#######################


def add_column(cursor, table_name, column_name):
    """ Adds a TEXT column to a database table.

    Arguments:
        cursor (sqlite3.Cursor) -- Database cursor
        table_name (str) -- Name of table to modify
        column_name (str) -- Name of column to add
    """
    cursor.execute("ALTER TABLE {table_name} ADD COLUMN {column_name} text".format(
        table_name=table_name, column_name=column_name))


def count_unique_values(cursor, table_name, column_name):
    """ Returns the unique values, and their counts, for a column in the specified table.

    Arguments:
        cursor (sqlite3.Cursor) -- Database cursor
        table_name (str) -- Name of table to query
        column_name (str) -- Name of column to query

    Returns:
        List of tuples. First value element contains the unique value; second element contains the number of rows
        with that value.
    """
    cursor.execute("SELECT {column_name}, COUNT(*) FROM {table_name} GROUP BY {column_name}".format(
        table_name=table_name, column_name=column_name))
    return cursor.fetchall()


def simpleUpdate(cursor, tableName, varName, value):
    cursor.execute("UPDATE " + tableName + " SET " + varName + " = '" + value + "'")


def index_column(cursor, table_name, column_name):
    """ Adds a TEXT column to a database table.

    Arguments:
        cursor (sqlite3.Cursor) -- Database cursor
        table_name (str) -- Name of table to modify
        column_name (str) -- Name of column to index
    """
    cursor.execute("CREATE INDEX {column_name}_idx ON {table_name} ({column_name})".format(
        table_name=table_name, column_name=column_name))


def dbOpen(db):
    """
    db: string, name of file to write database to, 
    will create if doesn't already exist
    """
    conn = sqlite3.connect(db)
    c = conn.cursor()
    return c


def dbClose(cursor, closeFlag=True):
    """
    cursor: sqlite cursor object
    run this before re-run, in order to cleanup database and close safely
    """
    cursor.execute("VACUUM")
    if closeFlag:
        cursor.close()


##################
# Functions that need to be done for a new dataset, but not thereafter
#################

def sourceLoad(cursor, file_path, table_name):
    """
    Loads data from specified file into a table in the database.

    WARNING: If the table already exists in the database, it will be dropped and re-created.

    Arguments:
        cursor (sqlite3.Cursor) -- SQLite3 database cursor
        file_path (str) -- Path to the CSV file containing the data
        table_name (str) -- Name of the table where the data should be stored

    Returns:
        None
    """
    # Drop the existing table
    cursor.execute("DROP TABLE IF EXISTS {}".format(table_name))
    convert(file_path, None, table_name, cursor=cursor)


def countryNamer(cursor, tableName, countryCode):
    """
    cursor: sqlite cursor object
    tableName: string, name of table
    countryCode: string, name of variable containing 2-char alpha country codes

    takes a variable, finds the unique instances of country codes, generates map
    to country names, then updates the country codes to country names where possible
    """
    qry = count_unique_values(cursor, tableName, countryCode)
    cnameDict = {}
    for row in qry:
        try:
            cnameDict[row[0]] = pycountry.countries.get(alpha2=str(row[0])).name
        except Exception as err:
            print "Err %s on: cc=%s" % (err, row[0])
            cnameDict[row[0]] = str(row[0])
    try:
        add_column(cursor, tableName, countryCode + "_cname")
    except:
        cursor.execute("UPDATE " + tableName + " SET " + countryCode + "_cname = 'NULL'")
    dataUpdate(cursor, tableName, countryCode, cnameDict, True, countryCode + "_cname")


def contImport(cursor, tableName, inFileName, varName1, varName2='continent'):
    """
    cursor: sqlite3 cursor object
    tableName: string, name of table
    varName1: string, name of variable containing country names
    varName2: string, name of variable to write continent names to 
              default value is 'continent'
    inFileName: string, name of file to read from
    reads pickled dictionary from file, and then maps it to the specified
    country variable, loading it into a variable called "continent", unless
    otherwise specified
    """
    with open(inFileName, "r") as inFile:
        contDict = cPickle.load(inFile)
    try:
        add_column(cursor, tableName, varName2)
    except:
        cursor.execute("UPDATE " + tableName + " SET " + varName2 + " = 'NULL'")
    dataUpdate(cursor, tableName, varName1, contDict, True, varName2)


def sortHash(inWord):
    """
    inWord: string to be hashed
    creates a salted hash of a string input, returns hash
    """
    chars = string.ascii_letters + string.digits + '!@#$%^&*()'
    random.seed = (os.urandom(1024))
    inWord.join(random.choice(chars) for i in range(6))
    return hashlib.sha1(inWord).hexdigest()


def idGen(cursor, tableName, varName, prefix):
    """
    cursor: sqlite3 cursor object
    tableName: string, name of table in db of cursor
    varName: name of id variable
    prefix: string, to start username
    takes usernames or userIDs and then sorts them by a 
    salted hash of the usernames (to prevent replicable sorting) and then creates
    sequential IDs for de-identification of the format course name + sequential number
    e.g. "MITx147300937" and adds these IDs to the table
    """
    cursor.execute("SELECT DISTINCT " + varName + " FROM " + tableName)
    length = len(cursor.fetchall())
    count = len(str(length * 10))
    try:
        index_column(cursor, tableName, varName)
    except:
        pass
    try:
        cursor.execute("DROP TABLE idhash")
    except:
        pass
    cursor.execute("CREATE TABLE idhash (id TEXT, hash TEXT, newid TEXT)")
    ids = count_unique_values(cursor, tableName, varName)
    print "ids: " + str(len(ids))
    counter = 1
    for row in ids:
        # print counter
        counter += 1
        useridhash = random.random()
        cursor.execute("INSERT INTO idhash VALUES (?, ?, ?)", (str(row[0]), str(useridhash), ""))
    cursor.execute("SELECT * FROM idhash ORDER BY hash")
    hashTable = cursor.fetchall()
    counter = 1
    try:
        add_column(cursor, tableName, "userid_DI")
        index_column(cursor, tableName, "userid_DI")
    except:
        print "userid_DI column already exists, overwriting"
        cursor.execute("UPDATE " + tableName + " SET userid_DI = 'NULL'")
    for row in hashTable:
        input1 = str(row[0])
        input2 = '{number:0{width}d}'.format(width=count, number=counter)
        cursor.execute(
            "UPDATE " + tableName + " SET userid_DI = '" + prefix + input2 + "' WHERE " + varName + " = '" + input1 + "'")
        # print counter
        counter += 1


#######################
# Use this to recreate the country_continent file if it is lost
######################

def contExport(cursor, tableName, varName1, varName2, outFileName):
    """
    cursor: sqlite3 cursor object
    tableName: string, name of table
    varName1: name of variable containing country names
    varName2: name of variable containing continent names
    outFileName: name of file to write to
    outputs the mapping of country to continent to a pickled file, for later import
    """
    headers = ["country", "continent"]
    selectItems = varName1 + ", " + varName2
    with open(outFileName, "w") as outFile:
        cursor.execute("SELECT " + selectItems + " FROM " + tableName + " GROUP BY " + varName1)
        contDict = {}
        for row in cursor.fetchall():
            contDict[row[0]] = row[1]
        cPickle.dump(contDict, outFile)


########################
# functions for generalizing
######################

def contSwap(cursor, table_name, country_column_name, continent_column_name, ceiling):
    """
    Combines the country and continent values for all countries with fewer rows than the ceiling value.

    Arguments:
        cursor (sqlite3.Cursor) -- Database cursor
        table_name (str) -- Name of table containing data
        country_column_name (str) -- Name of column containing country data
        continent_column_name (str) -- Name of column containing continent data
        ceiling (int) -- Minimum group size
    """
    try:
        add_column(cursor, table_name, country_column_name + "_DI")
        index_column(cursor, table_name, country_column_name + "_DI")
    except sqlite3.Error:
        pass

    # Default the *_DI column values to the value of the original column
    cursor.execute("UPDATE {table_name} SET {country_column_name}_DI = {country_column_name}".format(
        table_name=table_name,
        country_column_name=country_column_name
    ))

    cursor.execute(
        "SELECT {country_column_name}, {continent_column_name}, COUNT(*) "
        "FROM {table_name} "
        "GROUP BY {country_column_name}".format(
            country_column_name=country_column_name,
            continent_column_name=continent_column_name,
            table_name=table_name))

    rows = cursor.fetchall()
    print "Table {table_name} contains {count} unique countries.".format(table_name=table_name, count=len(rows))

    for row in rows:
        country = row[0]
        continent = row[1]
        count = row[2]

        # NOTE (CCB): This may be improved by filtering in SQL
        if count < ceiling or country in ('A1', 'A2', 'AP', 'EU', ''):
            cursor.execute(
                "UPDATE {table_name} SET {country_column_name}_DI = ? WHERE {country_column_name} = ?".format(
                    table_name=table_name, country_column_name=country_column_name),
                (continent, country,)
            )
    values = count_unique_values(cursor, table_name, "{}_DI".format(country_column_name))
    print "Categories after country-continent swap: {}".format(len(values))


def tailFinder(cursor, tableName, varName, catSize):
    """
    cursor: sqlite cursor object
    tableName: string, name of table
    varName: string, name of variable with tails
    catSize: k, upper bound for category size
    only works for integers
    """
    qry = count_unique_values(cursor, tableName, varName)
    itemList = {}
    keyList = []
    for i in qry:
        try:
            itemList[int(i[0])] = i[1]
            keyList.append(int(i[0]))
        except:
            print "Skipping non-integer value: {}".format(i[0])
    keyList.sort()
    for j in keyList:
        if itemList[j] < catSize:
            print j, itemList[j]
    a = raw_input("Would you like to trim the tails? (y/n): ")
    while a not in ['y', 'n']:
        a = raw_input("Please choose y(es) or n(o): ")
    if a == 'n':
        return
    else:
        b = raw_input("High (h), Low (l), or Both (b)?: ")
        if b == 'b' or b == 'l':
            low = raw_input("Choose the low tail: ")
            try:
                low = int(low)
            except:
                "invalid value, must be int"
            while low not in keyList:
                low = raw_input("Please choose from the values available: ")
                low = int(low)
        if b == 'b' or b == 'h':
            high = raw_input("Choose the high tail: ")
            try:
                high = int(high)
            except:
                "invalid value, must be int"
            while high not in keyList:
                high = raw_input("Please choose from the values available: ")
                high = int(high)
        tailMap = {}
        if b == 'b' or b == 'l':
            print "Low tail for {column_name}: {value}".format(column_name=varName, value=low)
        if b == 'b' or b == 'h':
            print "High tail for {column_name}: {value}".format(column_name=varName, value=high)
        for j in keyList:
            keyFlag = False
            if b == 'b' or b == 'l':
                if j <= low:
                    tailMap[str(j)] = "<= " + str(low)
                    keyFlag = True
                else:
                    tailMap[str(j)] = str(j)
                    keyFlag = True
            if b == 'b' or b == 'h':
                if j >= high:
                    tailMap[str(j)] = ">= " + str(high)
                elif not keyFlag:
                    tailMap[str(j)] = str(j)
        try:
            add_column(cursor, tableName, varName + "_DI")
            index_column(cursor, tableName, varName + "_DI")
        except:
            print "column " + varName + "_DI" + " already exists, overwriting..."
            cursor.execute("UPDATE " + tableName + " SET " + varName + "_DI = " + varName)
        dataUpdate(cursor, tableName, varName, tailMap, True, varName + "_DI")


###################
# recommend: use tailFinder
# first, and choose a tail that synchs with 
# the bin size of num binner (so hi tail should be
# multiple of 5, low tail (multiple of 5)-1
# before using numBinner, as it will skip over these
# string tails
####################

def numBinner(cursor, tableName, varName, bw=5):
    """
    cursor: sqlite cursor object
    tableName: string, name of table
    varName: string, name of variable containing number to bin
    bw: int, bin width, default is 5
    if there are already string or unicode "bins" in the values, they will be preserved 
    """
    qry = count_unique_values(cursor, tableName, varName)
    numDict = {}
    for item in qry:
        try:
            numDict[int(item[0])] = item[1]
        except:
            numDict[item[0]] = item[1]
    keys_sorted = sorted(numDict)
    keys_num = keys_sorted[:]
    for j in keys_num:
        if type(j) != int:
            keys_sorted.pop(keys_sorted.index(j))
    minBin = min(keys_sorted)
    maxBin = (max(keys_sorted) / bw) * bw
    bins = range(minBin, maxBin + bw, bw)
    binMap = {}
    for item in bins:
        for num in range(item, item + bw):
            binMap[num] = (str(item) + "-" + str(item + bw - 1))
    newNumDict = {}
    for item in numDict:
        if item in binMap.keys():
            newNumDict[unicode(item)] = binMap[item]
        else:
            newNumDict[unicode(item)] = str(item)
    choice = raw_input("Copy into (n)ew variable or (o)verwrite?: ")
    while choice not in ['n', 'o']:
        choice = raw_input("Plz choose n or o: ")
    if choice == 'n':
        try:
            add_column(cursor, tableName, varName + "_DI")
            index_column(c, tableName, varName + "_DI")
        except:
            print "column " + varName + "_DI" + " already exists, overwriting..."
            cursor.execute("UPDATE " + tableName + " SET " + varName + "_DI = " + varName)
        dataUpdate(cursor, tableName, varName, newNumDict, True, varName + "_DI")
    else:
        dataUpdate(cursor, tableName, varName, newNumDict)


def dateSplit(cursor, table_name, column_name):
    """
    cursor: sqlite cursor object
    tableName: string, name of table
    varName: string, name of date variable stored as string
    takes date/time stamps formatted "MM/DD/YYYYTxxxxxx" and strips out the date
    requires the T to denote beginning of the time
    """
    try:
        index_column(cursor, table_name, column_name)
    except:
        pass
    try:
        add_column(cursor, table_name, column_name + "_DI")
        index_column(cursor, table_name, column_name + "_DI")
    except:
        print "Column {}_DI already exists. Overwriting!".format(column_name)
        cursor.execute(
            "UPDATE {table_name} SET {column_name}_DI = NULL".format(table_name=table_name, column_name=column_name))

    unique_values_and_counts = count_unique_values(cursor, table_name, column_name)
    for item in unique_values_and_counts:
        date = item[0]
        if date and 'T' in date:
            point = date.index('T')
            dateNew = date[:point]
        else:
            dateNew = date
        cursor.execute(
            "UPDATE {table_name} SET {column_name}_DI = ? WHERE {column_name} = ?".format(table_name=table_name,
                column_name=column_name), (dateNew, date))


#######################
# Diagnostic functions
#######################

def nullMarker(cursor, tableName, varList):
    """
    cursor: sqlite cursor object
    tableName: string, name of table
    varList: list of tuples, form of (col number, var name), var name unicode
    takes list of variables and then generates one column per variable 
    with values 1 or 0 to denote if that record has missing or null values
    for the corresponding variable
    """
    for var in varList:
        try:
            index_column(cursor, tableName, var[1])
        except:
            pass
        try:
            add_column(cursor, tableName, var[1] + "_NF")
            index_column(cursor, tableName, var[1] + "_NF")
        except:
            simpleUpdate(cursor, tableName, var[1] + "_NF", "NULL")
        simpleUpdate(cursor, tableName, var[1] + "_NF", "1")
        cursor.execute("UPDATE " + tableName + " SET " + var[1] + "_NF = '0' WHERE (" + var[1] + " = '' OR " + var[
            1] + " = 'NA' OR " + var[1] + " is NULL)")


def nullWrap(cursor, tableName):
    """
    cursor: sqlite3 cursor object
    tableName: string, name of table containing vars
    a wrapper function, prompts user to select variables, then creates dummy 
    variables of form 'varname_NF' (0/1) that say whether a record has valid values
    for that variable
    """
    varList = qiPicker(cursor, tableName)
    nullMarker(cursor, tableName, varList)


def iterKcheck(cursor, tableName, k, nullFlag=True):
    """
    cursor: sqlite3 cursor object
    tableName: string, name of table
    k: int, minimum n for groups
    nullFlag: bool, True means that variables will be chosen
    by user, and new null vars, etc. generated; default=True
    iteratively checks for k-anonymity, first with one variable, then two
    etc. and then as variables are checked, records with null values for the other 
    variables are excluded from future checks
    """
    # get list of QI variables
    varList = qiPicker(cursor, tableName)
    # create null marker variables for QI variables
    if nullFlag:
        # print "marking nulls, time: "
        # print datetime.datetime.now().time()
        nullMarker(cursor, tableName, varList)
    # create variable that concatenates null marker variables
    nullList = []
    for var in varList:
        nullVar = var[1] + "_NF"
        nullList.append((var[0], nullVar))
    try:
        add_column(cursor, tableName, "nullSum")
        index_column(cursor, tableName, "nullSum")
    except:
        pass
    kkeyUpdate(cursor, tableName, nullList, "nullSum")
    nullqry = count_unique_values(cursor, tableName, "nullSum")
    # create variable that says whether it's okay for future checks
    try:
        # print "inside try, time: "
        # print datetime.datetime.now().time()
        add_column(cursor, tableName, "kCheckFlag")
        index_column(cursor, tableName, "kCheckFlag")
        simpleUpdate(cursor, tableName, "kCheckFlag", "False")
    except:
        # print "inside except, time:"
        # print datetime.datetime.now().time()
        # simpleUpdate(cursor, tableName, "kCheckFlag","False")
        pass
    # run through each combo of null variables
    for combo in nullqry:
        if '0' not in combo[0]: continue
        tmpVarList = []
        for i in range(len(combo[0])):
            if combo[0][i] == '1':
                tmpVarList.append(varList[i])
        print "Checking " + combo[0] + "..."
        print tmpVarList
        print datetime.datetime.now().time()
        try:
            add_column(cursor, tableName, "nullkkey")
        except:
            pass
        kkeyUpdate(cursor, tableName, tmpVarList, "nullkkey")
        cursor.execute(
            "SELECT nullkkey, COUNT(*) FROM " + tableName + " WHERE kCheckFlag = 'False' GROUP BY nullkkey")
        qry = cursor.fetchall()
        print "rows in qry: " + str(len(qry)) + ", time:"
        print datetime.datetime.now().time()
        if combo[0] == None:
            print "error on " + str(combo)
            return
        for row in qry:
            if row[0] == None:
                print "error on " + str(row)
                continue
            if row[1] >= k:
                cursor.execute('UPDATE ' + tableName + ' SET kCheckFlag = "True" WHERE (nullkkey = "' + row[
                    0] + '" AND nullSum = "' + combo[0] + '")')


def kkeyUpdate(cursor, table_name, varList, kkey_column_name="kkey"):
    """
    cursor: sqlite cursor object
    tableName: string, name of table
    varList: list of tuples, form of (col number, var name), var name unicode
    takes the QI variables identified by varList and concatenates into kkey
    """
    try:
        index_column(cursor, table_name, kkey_column_name)
    except:
        pass

    kkey_formula = "IFNULL("
    if len(varList) == 1:
        kkey_formula = "IFNULL(" + str(varList[0][1]) + ",'NULL')"
    else:
        for item in varList[:-1]:
            kkey_formula += str(item[1])
            kkey_formula += ",'NULL') || IFNULL("
        kkey_formula += str(varList[-1][1]) + ",'NULL')"

    # TODO Figure out what this is supposed to do. I suspect the use of 'NULL' (a string, rather than NULL) is a bug.
    cursor.execute(
        "UPDATE {table_name} SET {kkey_column_name} = ?".format(
            table_name=table_name,
            kkey_column_name=kkey_column_name),
        (kkey_formula,)
    )


def qiPicker(cursor, tableName):
    """
    cursor: sqlite3 cursor object
    tableName: string, name of table
    takes a cursor in a db, and then asks the user to specify the QI columns
    """
    tableInfo = "Pragma table_info(" + tableName + ")"
    cursor.execute(tableInfo)
    columns = cursor.fetchall()

    print "Please choose the QI variables from the list below:"
    for colNum in range(len(columns)):
        print "{number}. {column_name}".format(number=colNum, column_name=columns[colNum][1])
    choice = raw_input("Enter your choices by number, separated by commas: ")

    selected_columns = choice.split(',')
    selected_columns = [int(column_index.strip()) for column_index in selected_columns]
    return [(column_index, columns[column_index][1]) for column_index in selected_columns]


def grainSize(cursor, tableName, qiName):
    """
    cursor: sqlite3 cursor object
    qiName: string, name of variable to check for grain size
    calls to table specified by cursor and tableName, then examines values,
    returns a float, "grain size" as given by n of categories/n of items, smaller
    value means less granular, bigger "grains"
    """
    cursor.execute("SELECT " + qiName + " FROM " + tableName)
    valList = cursor.fetchall()
    valList = colToList(valList)
    groupList = valList[:]
    groupList.sort()
    groups = [(g[0], len(list(g[1]))) for g in itertools.groupby(groupList)]
    return float(len(groups)) / (len(groupList))


def genPicker(cursor, tableName, varList):
    """
    cursor: sqlite3 cursor object
    tableName: string, name of sqlite table to query
    varList: list of strings corresponding to columns in tableName
    wrapper function to check the grain size of all of the QI variables
    returns string name of variable to generalize next
    """
    b = [(var[1], grainSize(cursor, tableName, var[1])) for var in varList]
    c = [i[1] for i in b]
    d = str(b[c.index(max(c))][0])
    return d


def isTableKanonymous(cursor, tableName, k):
    """
    cursor: sqlite3 cursor object 
    tableName: string, name of table in db
    k: int
    takes sqlite table that contains column called kkey which is concatenation of QI variables, checks for k-anonymity,
    returns bool and supression required for k-anonymity (float btw. 0,1)
    """
    try:
        cursor.execute("DROP TABLE kacheck")
    except:
        pass
    cursor.execute("CREATE TABLE kacheck (kkey TEXT, count INTEGER)")
    try:
        cursor.execute("SELECT COUNT(*) FROM " + tableName + " WHERE kCheckFlag = 'False'")
    except:
        cursor.execute("SELECT COUNT(*) FROM " + tableName)
    itemCount = cursor.fetchall()[0][0]
    try:
        cursor.execute(
            "INSERT INTO kacheck SELECT kkey, COUNT(*) FROM " + tableName + " WHERE kCheckFlag = 'False' GROUP BY kkey")
    except:
        cursor.execute("INSERT INTO kacheck SELECT kkey, COUNT(*) FROM " + tableName + " GROUP BY kkey")
    cursor.execute("SELECT COUNT(*) FROM kacheck WHERE count < " + str(k))
    ltk = cursor.fetchall()[0][0]
    if ltk != 0:
        return False, float(ltk) / float(itemCount)
    else:
        return True, 0.0


def kAnonWrap(cursor, tableName, k):
    """
    cursor: sqlite cursor object
    tableName: string, name of table
    k: minimum group size
    wrapper function, gets list of variables from user input, 
    updates kkey, checks for k-anonymity
    """
    varList = qiPicker(cursor, tableName)
    kkeyUpdate(cursor, tableName, varList)
    a, b = isTableKanonymous(cursor, tableName, k)
    return a, b


def userKanon(cursor, tableName, userVar, courseVar, k):
    """
    cursor: sqlite cursor object
    tableName: string, name of table
    userVar: string, name of userid variable
    courseVar: string, name of course variable
    k: minimum group size
    creates a unique row record that is combo of 
    courseid and userid, and then creates another variable 
    that says which courses someone has taken
    then checks for unique count of courses taken
    and unique combinations of courses
    """
    courseList = courseComboUpdate(cursor, tableName, userVar, courseVar)
    value, uniqueList, nonUniqueList = uniqUserCheck(cursor, tableName, userVar, k)
    uniqUserFlag(cursor, tableName, uniqueList)
    dropNum = 1
    courseDrops = {}
    while value != 0.0:  # and dropNum != 17:
        print "non-anon value: " + str(value)
        courseTup = optimumDrop(cursor, tableName, userVar, k, nonUniqueList)
        if len(courseTup) == 0 or len(courseTup[2]) == 0:
            # print "no more changes can be made"
            # dropNum +=1
            return courseDrops
        # print courseTup
        courseNum = courseTup[0]
        changeVals = courseTup[2]
        courseName = courseList[courseNum]
        courseDrops = courseDropper(cursor, tableName, courseVar, courseName, changeVals, courseDrops)
        courseList = courseComboUpdate(cursor, tableName, userVar, courseVar)
        value, uniqueList, nonUniqueList = uniqUserCheck(cursor, tableName, userVar, k)
        uniqUserFlag(cursor, tableName, uniqueList)
    return courseDrops


def courseComboUpdate(cursor, table_name, user_id_column_name, course_id_column_name):
    courseQry = count_unique_values(cursor, table_name, course_id_column_name)
    courseList = []
    print "generating course list"
    print datetime.datetime.now().time()
    for row in courseQry:
        courseList.append(row[0])
    userQry = count_unique_values(cursor, table_name, user_id_column_name)
    print "creating/overwriting course_combo"
    print datetime.datetime.now().time()
    try:
        add_column(cursor, table_name, "course_combo")
        index_column(cursor, table_name, "course_combo")
    except:
        simpleUpdate(cursor, table_name, "course_combo", "NULL")
    print "no. of unique users to update: " + str(len(userQry))
    print datetime.datetime.now().time()
    count = 1
    for row in userQry:
        query = "SELECT {course_id_column_name} FROM {table_name} WHERE {user_id_column_name} = ?".format(
            course_id_column_name=course_id_column_name,
            table_name=table_name,
            user_id_column_name=user_id_column_name
        )
        cursor.execute(query, (row[0],))
        subQry = cursor.fetchall()
        qryList = []
        for subRow in subQry:
            qryList.append(subRow[0])
        courseCombo = ""
        for course in courseList:
            if course in qryList:
                courseCombo += "1"
            else:
                courseCombo += "0"

        query = "UPDATE {table_name} SET course_combo = ? WHERE {user_id_column_name} = ?".format(
            table_name=table_name,
            user_id_column_name=user_id_column_name
        )
        cursor.execute(query, (courseCombo, row[0],))
    return courseList


def userKCheckTable(cursor, tableName, userVar, records='all'):
    """
    creates a temporary-use table called "userkcheck" that 
    holds unique course_combo values by userid
    records option allows 'all', 'True', or 'False'
    """
    try:
        cursor.execute("DROP TABLE userkcheck")
    except:
        pass
    cursor.execute("CREATE TABLE userkcheck (useridUKC TEXT, course_comboUKC TEXT)")
    if records == 'all':
        cursor.execute("INSERT INTO userkcheck SELECT DISTINCT " + userVar + ", course_combo FROM " + tableName)
    else:
        cursor.execute(
            "INSERT INTO userkcheck SELECT DISTINCT " + userVar + ", course_combo FROM " + tableName + " WHERE uniqUserFlag = '" + records + "'")
    try:
        index_column(cursor, "userkcheck", "useridUKC")
    except:
        pass
    try:
        index_column(cursor, "userkcheck", "course_comboUKC")
    except:
        pass


def courseUserQry(cursor, tableName, userVar, records='all'):
    """
    cursor: sqlite cursor object
    tableName: string, name of table
    userVar: string, name of userid variable
    records: string, 'all' 'True' or 'False'
    wrapper, creates a temp table of unique course combo records,
    returns a qry with the course combo and number of unique
    users as the result, option allows for just getting users with "unique" 
    (i.e. n<k) course combo values
    """
    userKCheckTable(cursor, tableName, userVar, records)
    cursor.execute("SELECT course_comboUKC, COUNT(useridUKC) FROM userkcheck GROUP BY course_comboUKC")
    qry = cursor.fetchall()
    return qry


def uniqUserCheck(cursor, tableName, userVar, k):
    """
    cursor: sqlite cursor object
    tableName: string, name of table
    userVar: string, name of userid variable
    k: minimum group size
    used to check if there are unique combos of courses
    by user
    """
    qry = courseUserQry(cursor, tableName, userVar)
    combos = 0
    count = 0
    uniqueList = []
    nonUniqueList = []
    for row in qry:
        combos += row[1]
        if row[1] < k:
            count += row[1]
            uniqueList.append(row[0])
        else:
            nonUniqueList.append(row[0])
    return float(count) / float(combos), uniqueList, nonUniqueList


def uniqUserFlag(cursor, tableName, uniqueList):
    """
    cursor: sqlite cursor object
    tableName: string, name of main table
    uniqueList: list, list of unique values of course_combo
    """
    try:
        add_column(cursor, tableName, "uniqUserFlag")
        index_column(cursor, tableName, "uniqUserFlag")
        simpleUpdate(cursor, tableName, "uniqUserFlag", "False")
    except:
        simpleUpdate(cursor, tableName, "uniqUserFlag", "False")
    for item in uniqueList:
        cursor.execute("UPDATE " + tableName + " SET uniqUserFlag = 'True' WHERE course_combo = '" + item + "'")


def shannonEntropy(itemList):
    """
    itemList: list of tuples (<<item>>, <<count>>) list of items to 
    determine entropy of
    """
    total = 0
    entropy = 0
    for i in itemList:
        total += i[1]
    for i in itemList:
        p_i = float(i[1]) / float(total)
        entropy += - p_i * math.log(p_i, 2)
    return entropy


def optimumDrop(cursor, tableName, userVar, k, nonUniqueList, nComb=1):
    """
    cursor: sqlite3 cursor object
    tableName: string, name of main table
    userVar: string, name of userid var
    k: int, minimum cell size
    nonUniqueList: list of course_combo values already cleared for k-anonymity
    nComb: int, number of courses to try to drop, default 1
    iteratively tries 'dropping' one course for all of the records
    that are flagged as having a unique combo of courses
    then measures the entropy of the resulting group, and
    returns the position in courseList of the course to drop, along with the 
    course_combo values that will benefit from the drop
    """
    qry = courseUserQry(cursor, tableName, userVar, 'True')
    if len(qry) == 0:
        return qry
    posLen = len(
        qry[0][0])  # assumes first variable in each tuple is the course combo, finds num of positions to change
    preList = qry[:]
    preEntropy = shannonEntropy(preList)
    postEntList = []
    preCount = 0
    for n in qry:
        preCount += n[1]
    print preCount
    # iterTemp = itertools.combinations(range(posLen),nComb)
    # dropCombos = list(iterTemp)
    for i in range(posLen):
        postList = []
        tmpList = qry[:]
        for j in tmpList:
            newString = ""
            for l in range(posLen):
                if l == i:
                    newString += "0"
                else:
                    newString += j[0][l]
            postList.append((newString, j[1]))
        try:
            cursor.execute("DROP TABLE coursedrop")
            cursor.execute("CREATE TABLE coursedrop (course_combo TEXT, Count INTEGER)")
        except:
            cursor.execute("CREATE TABLE coursedrop (course_combo TEXT, Count INTEGER)")
        cursor.executemany("INSERT INTO coursedrop VALUES (?,?)", postList)
        cursor.execute("SELECT course_combo, COUNT(*) FROM coursedrop GROUP BY course_combo")
        postQry = cursor.fetchall()
        postEntropy = shannonEntropy(postQry)
        postCount = 0
        for item in postQry:
            postCount += item[1]
        changeVals = []
        for m in postQry:
            mList = list(m[0])
            # print mList
            oldString = ""
            for slot in range(len(mList)):
                if slot == i:
                    oldString += '1'
                else:
                    oldString += mList[slot]
            if m[1] >= k:
                changeVals.append(oldString)
            elif (m[0] in nonUniqueList):
                changeVals.append(oldString)
        if len(changeVals) > 0:
            postEntList.append((i, preEntropy - postEntropy, changeVals))
    if len(postEntList) == 0:
        return []
    first = True
    for n in postEntList:
        if first:
            low = n
            first = False
        elif n[1] < low[1]:
            low = n
    return low


def courseDropper(cursor, tableName, courseVar, courseName, changeVals, courseDict={}):
    """
    courseName: string, name of course to be dropped
    changeVals: list of strings, values of course_combo to drop
    courseDict: dictionary of courses and running tally of rows dropped
    drops course record where course equals courseName
    AND uniqUserFlag = "True"
    """
    delCount = 0
    for val in changeVals:
        cursor.execute(
            "SELECT COUNT(*) FROM " + tableName + " WHERE (" + courseVar + " = '" + courseName + "' AND uniqUserFlag = 'True' AND course_combo = '" + val + "')")
        qry = cursor.fetchall()
        if (qry[0][0]):
            delCount += qry[0][0]
        else:
            return courseDict
    if courseName in courseDict.keys():
        courseDict[courseName] += delCount
    else:
        courseDict[courseName] = delCount
    # confirm = raw_input("Confirm you want to delete "+str(delCount)+" records associated with "+courseName+" (y/n): ")
    # if confirm == 'n':
    #    return
    # elif confirm == 'y':
    for val in changeVals:
        cursor.execute(
            "DELETE FROM " + tableName + " WHERE (" + courseVar + " = '" + courseName + "' AND uniqUserFlag = 'True' AND course_combo = '" + val + "')")
    # else:
    #    print "invalid choice, exiting function"
    return courseDict


#################
# Functions for censoring
# run after doing all generalizing
#################

def contCensor(cursor, tableName, varName1, varName2):
    """
    cursor: sqlite cursor object
    tableName: string, name of table
    varName1: string, name of variable containing country names
    varName2: string, name of variable containing continent names
    th = int, k, minimum group size
    similar to contSwap, only does it for all rows with a "False" export_flag
    """
    cursor.execute('UPDATE ' + tableName + ' SET ' + varName1 + '_DI = ' + varName2 + ' WHERE export_flag = "False"')


def censor(cursor, tableName, varName, value=""):
    """
    cursor: sqlite cursor object
    tableName: string, name of table
    varName: string, name of variable to censor
    sets value of given variable to specified value
    default blank, for rows with export_flag = 'False'
    """
    cursor.execute("UPDATE " + tableName + " SET " + varName + " = '" + value + "' WHERE export_flag = 'False'")


######################
# Misc. Helper functions
######################

def dataUpdate(cursor, tableName, varName, catMap, newVar=False, newVarName=''):
    """
    cursor: sqlite3 cursor object
    tableName: string, name of table in db of cursor
    varName: name of variable to update values
    catMap: dict, mapping of old values to new
    newVar: bool, optional, flag for if updated variable is different from key variable
    newVarName: if newVar==True, must provide name of target variable
    """
    try:
        index_column(cursor, tableName, varName)
    except:
        pass
    count = 0
    if newVar:
        try:
            index_column(cursor, tableName, newVarName)
        except:
            pass
        for cat in catMap:
            # if count == 100:
            #    return
            # print count
            # count += 1
            cursor.execute('UPDATE ' + tableName + ' SET ' + newVarName + ' = "' + catMap[
                cat] + '" WHERE ' + varName + ' = "' + cat + '"')
    else:
        for cat in catMap:
            # if count == 100:
            #    return
            # print count
            # count += 1
            cursor.execute('UPDATE ' + tableName + ' SET ' + varName + ' = "' + catMap[
                cat] + '" WHERE ' + varName + ' = "' + cat + '"')


def colToList(queryResult):
    """
    queryResult: list of tuples of length 1
    returns a list of the values outside of a tuple
    """
    returnList = []
    for a in queryResult:
        returnList.append(a[0])
    return returnList


#######################
#
# Functions for exporting de-identified file
#
######################


def csvExport(cursor, tableName, outFileName):
    """
    cursor: sqlite3 cursor object
    tableName: string, name of table
    outFileName: name of file to write to
    asks user to specify columns in the database to export to a .csv file under the specified name in the cwd
    """
    varList = qiPicker(cursor, tableName)
    selectItems = ''
    headers = []
    for var in varList[:-1]:
        selectItems += str(var[1]) + ", "
        headers.append(str(var[1]))
    selectItems += str(varList[-1][1])
    headers.append(str(varList[-1][1]))
    with open(outFileName, "w") as csvOutFile:
        fileWriter = csv.writer(csvOutFile)
        fileWriter.writerow(headers)
        cursor.execute("SELECT " + selectItems + " FROM " + tableName + " WHERE kCheckFlag = 'True'")
        for row in cursor.fetchall():
            rowList = list(row)
            fileWriter.writerow(rowList)
