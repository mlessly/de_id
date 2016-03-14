# <nbformat>3.0</nbformat>

# <headingcell level=4>

# Import de-identification functions from datafly_v4.py

# <codecell>

from __future__ import unicode_literals

from decimal import *

import numpy as np
import pandas as pd

from de_id_functions import *


# <headingcell level=4>

# Additional functions not included in the de_id_functions.py file.

# <codecell>

def utilValues(cursor, table_name, column_name):
    """
    cursor: sqlite cursor object
    table_name: string, name of sqlite table
    column_name: string, name of variable to analyze

    takes values of an integer or float variable and returns the
    mean, standard deviation, and entropy
    """
    entQry = count_unique_values(cursor, table_name, column_name)
    entropy = shannonEntropy(entQry)

    cursor.execute("SELECT {column_name}  FROM {table_name}".format(column_name=column_name, table_name=table_name))
    values = cursor.fetchall()
    values = colToList(values)
    float_values = textToFloat(values)

    if len(float_values) == 0:
        print("No values could be converted to numbers")
        return

    qryArray = np.array(float_values)
    mean = qryArray.mean()
    sd = qryArray.std()

    return entropy, mean, sd


def binAvg(cursor, table_name, nominal_column_name, numeric_column_name):
    """
    cursor: sqlite cursor object
    table_name: string, name of sqlite table
    nominal_column_name: string, name of variable with nominal categories
    numeric_column_name: string, name of corresponding variable with numeric values
    For two columns, one a categorical string representation (generalization)
    of the numeric values in another column (for example column A 
    contains "10-15" and then column B contains the actual values 
    that are in that bin), will give a bin-level average of the true values
    in that bin. Designed as a tool to help improve the quality of a 
    binned (aka 'generalized') dataset. 
    """
    newVarName = nominal_column_name + "_avg"
    getcontext().prec = 2
    bins = count_unique_values(cursor, table_name, nominal_column_name)
    avgDic = {}
    for cat in bins:
        cursor.execute(
            "SELECT {numeric_column_name} FROM {table_name} WHERE {nominal_column_name} = ?".format(
                numeric_column_name=numeric_column_name,
                table_name=table_name,
                nominal_column_name=nominal_column_name
            ),
            (cat[0],)
        )
        qry = cursor.fetchall()
        qry = colToList(qry)
        qry2 = textToFloat(qry)
        if len(qry2) == 0:
            print "No values could be converted to numbers: " + str(cat[0])
            continue
        qryArray = np.array(qry2)
        mean = qryArray.mean()
        mean = Decimal(mean)
        mean = round(mean, 2)
        avgDic[cat[0]] = str(mean)
    try:
        add_column(cursor, table_name, newVarName)
        index_column(cursor, table_name, newVarName)
    except:
        print "column " + newVarName + " already exists, overwriting..."
        cursor.execute("UPDATE " + table_name + " SET " + newVarName + " = 'null'")
    dataUpdate(cursor, table_name, nominal_column_name, avgDic, True, newVarName)


def utilMatrix(cursor, tableName, variable_names):
    """
    cursor: sqlite cursor object
    tableName: string, name of sqlite table
    varList: list of utility variables, in format indigenous
    to this program, which is the format that results
    from the sqlite "Pragma table_info()" command.

    This function creates a Pandas dataframe/matrix of the entropy,
    mean, and standard deviation of the utility variables, 
    index is the variable name, and columns are the statistics
    """
    uMatrix = pd.DataFrame(columns=["Entropy", "Mean", "SD"], index=variable_names)
    for var in variable_names:
        ent, mean, sd = utilValues(cursor, tableName, var)
        uMatrix.ix[var] = [ent, mean, sd]
    return uMatrix


def textToFloat(txtList):
    """
    txtList: list of text values
    returns a list of float values, 
    skips values that cannot be converted
    """
    numList = []
    for i in txtList:
        try:
            if i == 'true':
                numList.append(1)
            elif i == 'false':
                numList.append(0)
            else:
                numList.append(float(i))
        except:
            pass
    return numList


def lDiversity(cursor, tableName, kkeyVar, senVar):
    """
    cursor: sqlite3 cursor object                                                                                                                                                                
    tableName: string, name of main table
    kkeyVar: string, name of variable that contains concatenation of all quasi-identifiers
    senVar: string, name of variable whose value you do not want disclosed
    Checks a dataset for "l-diversity", namely that in a k-anonymous block of records
    if the sensitive value is homogeonous, then you have effectively disclosed the 
    value of the sensitive record. Bluntly sets sensitive variable to blank if not l-diverse
    """
    qry = count_unique_values(cursor, tableName, kkeyVar)
    for i in qry:
        cursor.execute(
            'SELECT ' + senVar + ' FROM ' + tableName + ' WHERE ' + kkeyVar + ' = "' + i[0] + '" GROUP BY ' + senVar)
        qry2 = cursor.fetchall()
        if len(qry2) == 1:
            cursor.execute('UPDATE ' + tableName + ' SET ' + senVar + ' = " " WHERE ' + kkeyVar + ' = "' + i[0] + '"')


def optimumDrop2(cursor, tableName, userVar, k, nonUniqueList, nComb=1):
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
    posLen = len(qry[0][
        0])  # assumes first variable in each tuple is the course combo, finds num of positions to change
    preList = qry[:]
    preCombos = []
    for i in preList:
        preCombos.append(i[0])
    preEntropy = shannonEntropy(preList)
    postEntList = []
    preCount = 0
    for n in qry:
        preCount += n[1]
    print preCount
    iterTemp = itertools.combinations(range(posLen), nComb)
    dropCombos = []
    while True:
        try:
            dropCombos.append(iterTemp.next())
        except:
            break
    for i in dropCombos:
        # print "dropCombo:"
        # print i
        postList = []
        tmpList = qry[:]
        for j in tmpList:
            newString = ""
            for l in range(posLen):
                if l in i:
                    newString += "0"
                else:
                    newString += j[0][l]
            postList.append((newString, j[1]))
        try:
            cursor.execute("DROP TABLE coursedrop")
            cursor.execute("CREATE TABLE coursedrop (course_combo text, Count integer)")
        except:
            cursor.execute("CREATE TABLE coursedrop (course_combo text, Count integer)")
        cursor.executemany("INSERT INTO coursedrop VALUES (?,?)", postList)
        cursor.execute("SELECT course_combo, COUNT(*) FROM coursedrop GROUP BY course_combo")
        postQry = cursor.fetchall()
        postEntropy = shannonEntropy(postQry)
        postCount = 0
        for item in postQry:
            postCount += item[1]
        changeVals = []
        for k in range(len(i)):
            oldSpots = []
            iterTemp = itertools.combinations(i, k + 1)
            while True:
                try:
                    oldSpots.append(iterTemp.next())
                except:
                    break
            for l in oldSpots:
                for m in postQry:
                    mList = list(m[0])
                    for n in l:
                        mList[n] = '1'
                    oldString = ''
                    for p in mList:
                        oldString += p
                    if m[1] >= k and oldString in preCombos:
                        changeVals.append(oldString)
                    elif (m[0] in nonUniqueList) and oldString in preCombos:
                        changeVals.append(oldString)
        # print "Length of ChangeVals: "+str(len(changeVals))
        if len(changeVals) > 0:
            postEntList.append((i, preEntropy - postEntropy, changeVals))
    if len(postEntList) == 0:
        return []
    first = True
    low = (99, 99, [])
    for n in postEntList:
        if n[1] < low[1] and n[1] > 0.0:
            low = n
    return low


def userKanon2(cursor, tableName, userVar, courseVar, k):
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
    while value != 0.0 and dropNum != 16:
        print "DropNum: " + str(dropNum)
        print "non-anon value: " + str(value)
        courseTup = optimumDrop2(cursor, tableName, userVar, k, nonUniqueList, dropNum)
        # print "courseTup returned from OptimumDrop:"
        if len(courseTup) == 0 or len(courseTup[2]) == 0:
            dropNum += 1
            print "no more changes can be made, trying " + str(dropNum) + " courses at a time"
            return courseDrops
        # print courseTup[:2]
        courseNums = courseTup[0]
        # print "courseNums:"
        # print courseNums
        changeVals = courseTup[2]
        print "length of changeVals"
        print len(changeVals)
        for i in courseNums:
            courseName = courseList[i]
            print "dropping courseName:"
            print courseName
            courseDrops = courseDropper2(cursor, tableName, courseVar, courseName, changeVals, courseDrops)
        courseList = courseComboUpdate(cursor, tableName, userVar, courseVar)
        value, uniqueList, nonUniqueList = uniqUserCheck(cursor, tableName, userVar, k)
        uniqUserFlag(cursor, tableName, uniqueList)
    return courseDrops


def courseDropper2(cursor, tableName, courseVar, courseName, changeVals, courseDict={}):
    """                                                                                                                                                                                          
    courseName: string, name of course to be dropped                                                                                                                                             
    changeVals: list of strings, values of course_combo to drop                                                                                                                                  
    courseDict: dictionary of courses and running tally of rows dropped                                                                                                                          
    drops course record where course equals courseName                                                                                                                                           
    AND uniqUserFlag = "True"                                                                                                                                                                    
    """
    delCount = 0
    # print "len of changeVals: "+str(len(changeVals))
    for val in changeVals:
        cursor.execute(
            "SELECT COUNT(*) FROM " + tableName + " WHERE (" + courseVar + " = '" + courseName + "' AND uniqUserFlag = 'True' AND course_combo = '" + val + "')")
        qry = cursor.fetchall()
        # print "changeVal qry length:"+str(len(qry))
        if (qry[0][0]): delCount += qry[0][0]
    print "delCount: " + str(delCount)
    if delCount == 0:
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


def kAnonIter(cursor, tableName, k, outFile):
    """                                                                                                                                                                                          
    cursor: sqlite cursor object                                                                                                                                                                 
    tableName: string, name of table                                                                                                                                                             
    k: minimum group size                                                                                                                                                                        
    wrapper function, gets list of variables from user input,                                                                                                                                    
    updates kkey, checks for k-anonymity                                                                                                                                                         
    """
    coreVarList = qiPicker(cursor, tableName)
    optVarList = qiPicker(cursor, tableName)
    iterVarList = coreVarList
    addList = []
    kkeyUpdate(cursor, tableName, iterVarList)
    varIndex = 0
    a, b = isTableKanonymous(cursor, tableName, k)
    results = [('core', b)]
    for var in optVarList:
        iterVarList.append(optVarList[varIndex])
        print iterVarList
        addList.append(optVarList[varIndex])
        print addList
        results.append((addList,))
        kkeyUpdate(cursor, tableName, iterVarList)
        a, b = isTableKanonymous(cursor, tableName, k)
        varIndex += 1
        results[varIndex] += (b,)
    outFile.write(str(results))
    return results


# <headingcell level=4>

# Name the file containing the data,
# name the database,
# and name commonly-used database variables.
# NOTE: make updates here if file specification changes

# <codecell>

file = 'private_data/input.csv'
table = 'source'
pristine_table_name = "original"
user_id_column_name = 'user_id'
course_id_column_name = 'course_id'
country_code_column_name = 'cc_by_ip'
k = 5

exported_file_path = "private_data/HMXPC13_DI_binned_061714.csv"

# <codecell>

# choose a name for the database and then connect to it
db_name = 'private_data/kaPC_1-17-4-17-14-3.db'
cursor = dbOpen(db_name)

# <headingcell level=4>

# Load data into SQLite database

# <codecell>

sourceLoad(cursor, file, table)

# <headingcell level=4>

# Load data into another table to make comparisons to the original data

# <codecell>

sourceLoad(cursor, file, pristine_table_name)

# <headingcell level=4>

# Drop the timestamp from the date fields.

# <codecell>

# TODO Re-activate when we are ingesting data as dates instead of numerical timestamps
# dateSplit(cursor, table, "start_time")
# dateSplit(cursor, table, "last_event")

# <codecell>

cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
cursor.fetchall()

# <headingcell level=4>

# Load column names into a variable called varList

# <codecell>

cursor.execute("Pragma table_info(" + table + ")")
varList = cursor.fetchall()
varList

# <headingcell level=4>

# Add indices

# <codecell>

index_column(cursor, table, course_id_column_name)
index_column(cursor, table, user_id_column_name)

# <codecell>

cursor.execute("CREATE INDEX " + course_id_column_name + "_idx2 ON original (" + course_id_column_name + ")")
cursor.execute("CREATE INDEX " + user_id_column_name + "_idx2 ON original (" + user_id_column_name + ")")

# <headingcell level=4>

# Get initial count of records loaded

# <codecell>

cursor.execute("SELECT COUNT(*) FROM {}".format(table))
cursor.fetchall()

# <headingcell level=4>

# Map country codes to country names, load table of country name to continent mappings

# <codecell>

# TODO Reactivate if we determine this to be useful
# countryNamer(cursor, table, country_code_column_name)
# contImport(cursor, table, "country_continent", country_code_column_name + "_cname")

# <headingcell level=4>

# Delete staff

# <codecell>

for table_name in (table, pristine_table_name,):
    cursor.execute("DELETE FROM {table_name} WHERE roles IN  ('instructor', 'staff')".format(table_name=table_name))

# <headingcell level=4>

# Generate anonymous userIDs, choose prefix that will describe the data release, here 'MHxPC13' refers to MITx/HarvardX Person-Course AY2013

# <codecell>

idGen(cursor, table, user_id_column_name, "MHxPC13")

# <headingcell level=4>

# Get initial entropy reading

# <codecell>

add_column(cursor, table, 'entropy')
index_column(cursor, table, 'entropy')
kkeyUpdate(cursor, table, varList, 'entropy')

# <codecell>

result = count_unique_values(cursor, table, 'entropy')

# <codecell>

beginEntropy = shannonEntropy(result)
beginEntropy

# <headingcell level=4>

# Create utility Matrix (both for unmodified dataset and current dataset)

# <codecell>

utilVars = (
    'viewed', 'explored', 'certified', 'grade', 'nevents', 'ndays_act', 'nplay_video', 'nchapters', 'nforum_posts',)

# <codecell>

preUmatrix = utilMatrix(cursor, pristine_table_name, utilVars)

# <codecell>

preUmatrix

# <codecell>

uMatrix = utilMatrix(cursor, table, utilVars)

# <codecell>

uMatrix

# <codecell>

uMatrix - preUmatrix
# removed rows for user k-anonymity

# <headingcell level=4>

# Establish user-wise k-anonymity (the removal of registrations that uniquely identify someone based on combination of courses registered for)

# <codecell>

courseDrops = userKanon2(cursor, table, user_id_column_name, course_id_column_name, k)

# <codecell>

for course in courseDrops.keys():
    print "Dropped " + str(courseDrops[course]) + " rows for course " + course

# <codecell>

cursor.execute("SELECT COUNT(*) FROM {table_name} WHERE uniqUserFlag = 'True'".format(table_name=table))
result = cursor.fetchall()
print "Deleted " + str(result[0][0]) + " additional records for users with unique combinations of courses.\n"
cursor.execute("DELETE FROM {table_name} WHERE uniqUserFlag = 'True'".format(table_name=table))

# <codecell>

kkeyUpdate(cursor, table, varList[:26], 'entropy')

# <codecell>

result = count_unique_values(cursor, table, 'entropy')
tmpEntropy = shannonEntropy(result)

# <codecell>

tmpEntropy

# <codecell>

entChg = 2 ** beginEntropy - 2 ** tmpEntropy

# <codecell>

entChg
# This one after User-K-Anonymity

# <headingcell level=4>

# Replace country names with continent names

# <codecell>

country_threshold = 5000
contSwap(cursor, table, "cc_by_ip", "continent", country_threshold)

# <headingcell level=4>

# Make gender variable that treats NA and missing as same

# <codecell>

try:
    # TODO Clean this field on ingestion
    add_column(cursor, table, "gender_DI")
    index_column(cursor, table, "gender_DI")
    simpleUpdate(cursor, table, "gender_DI", "NULL")
    cursor.execute("UPDATE " + table + " SET gender_DI = gender")
    cursor.execute("UPDATE " + table + " SET gender_DI = '' WHERE gender_DI = 'NA'")
except:
    cursor.execute("UPDATE " + table + " SET gender_DI = gender")
    cursor.execute("UPDATE " + table + " SET gender_DI = '' WHERE gender_DI = 'NA'")

# <headingcell level=4>

# Get k-anonymity reading

# <codecell>

# TODO Create this earlier
add_column(cursor, table, 'kkey')
status, value = kAnonWrap(cursor, table, k)
print "Percent of records that will need to be deleted to be k-anonymous: " + str(value) + "\n"
# outFile.write( "Percent of records that will need to be deleted to be k-anonymous: "+str(value)+"\n")

# <headingcell level=4>

# Check k-anonymity for records with some null values

# <codecell>

print "checking k-anonymity for records with some null values"
print datetime.datetime.now().time()
iterKcheck(cursor, table, k)


# <codecell>

def eduClean(cursor, tableName, loeVar):
    try:
        add_column(cursor, tableName, loeVar + "_DI")
        index_column(cursor, tableName, loeVar + "_DI")
    except:
        simpleUpdate(cursor, tableName, loeVar + "_DI", "NULL")
    ed_dict = {'': '', 'NA': 'NA', 'a': 'Secondary', 'b': "Bachelor's", 'el': 'Less than Secondary',
        'hs': 'Secondary', 'jhs': 'Less than Secondary', 'learn': '', 'm': "Master's", 'none': '',
        'other': '', 'p': 'Doctorate', 'p_oth': 'Doctorate', 'p_se': 'Doctorate'}
    qry = count_unique_values(cursor, tableName, loeVar)
    for row in qry:
        if row[0] in ed_dict.keys():
            cursor.execute(
                'UPDATE ' + tableName + ' SET ' + loeVar + '_DI = "' + ed_dict[row[0]] + '" WHERE ' + loeVar + ' = "' +
                row[0] + '"')


# <codecell>

eduClean(cursor, table, "LoE")

# <codecell>

count_unique_values(cursor, table, "LoE")

# <codecell>

count_unique_values(cursor, table, "LoE_DI")

# <codecell>

# TODO Set ignorable values to NULL. Update other code to exclude NULL/None values.
# change 0 values to text in order to exclude them from the binning procedure
cursor.execute("UPDATE {} SET nforum_posts = 'zero' WHERE nforum_posts = '0'".format(table))

# <headingcell level=4>

# The Tailfinder function can help to group a long tail of one variable into a text field
# (see more documentation in the de_id_functions.py file)

# <codecell>

tailFinder(cursor, table, "nforum_posts", 5)

# <codecell>

# TODO Where is the column supposed to be created?
numBinner(cursor, table, "nforum_posts_DI")

# <codecell>

binAvg(cursor, table, "nforum_posts_DI", "nforum_posts")

# <codecell>

count_unique_values(cursor, table, "nforum_posts_DI_avg")

# <codecell>

tailFinder(cursor, table, "YoB", 50)

# <codecell>

numBinner(cursor, table, "YoB_DI", bw=2)

# <codecell>

count_unique_values(cursor, table, "YoB_DI")

# <codecell>

kAnonWrap(cursor, table, k)

# <codecell>

lDiversity(cursor, table, "kkey", "grade")

# <headingcell level=4>

# Needed an incomplete flag for internally inconsistent records. This is described more in the documentation with the data release.

# <codecell>

add_column(cursor, table, "incomplete_flag")

# <codecell>

index_column(cursor, table, "incomplete_flag")

# <codecell>

cursor.execute("SELECT COUNT(*) FROM {} WHERE nevents = '' AND nchapters != ''".format(table))
result = cursor.fetchall()
print result
cursor.execute("SELECT COUNT(*) FROM {} WHERE nevents = '' AND nforum_posts != '0'".format(table))
result = cursor.fetchall()
print result
cursor.execute("SELECT COUNT(*) FROM {} WHERE nevents = '' AND ndays_act != ''".format(table))
result = cursor.fetchall()
print result

# <codecell>

cursor.execute("UPDATE {} SET incomplete_flag = '1' WHERE nevents = '' AND nchapters != ''".format(table))

# <codecell>

cursor.execute("UPDATE {} SET incomplete_flag = '1' WHERE nevents = '' AND nforum_posts != '0'".format(table))

# <codecell>

cursor.execute("UPDATE {} SET incomplete_flag = '1' WHERE nevents = '' AND ndays_act != ''".format(table))

# <codecell>

cursor.execute("SELECT * FROM {} WHERE incomplete_flag = '1'".format(table))

# <codecell>

result = cursor.fetchall()

# <codecell>

len(result)

# <codecell>

cursor.execute("Pragma table_info({})".format(table))
varList = cursor.fetchall()
varList

# <codecell>

kkeyList = []
kkeyList.append(varList[0])
kkeyList.append(varList[36])
kkeyList.append(varList[37])
kkeyList.append(varList[47])
kkeyList.append(varList[49])
kkeyList.append(varList[50])
kkeyList

# <codecell>

kkeyUpdate(cursor, table, kkeyList)

# <codecell>

cursor.execute("SELECT COUNT(*), kkey FROM {} GROUP BY kkey".format(table))
qry2 = cursor.fetchall()
# lessThanK = []
# badCount = 0
cursor.execute("UPDATE " + table + " SET kCheckFlag = 'False'")
for row in qry2:
    if row[0] >= 5:
        cursor.execute('UPDATE ' + table + ' SET kCheckFlag = "True" WHERE kkey = "' + row[1] + '"')

# <codecell>

count_unique_values(cursor, table, "kCheckFlag")

# <headingcell level=4>

# The fateful step where non-k-anonymous records are removed.

# <codecell>

cursor.execute("DELETE FROM {} WHERE kCheckFlag = 'False'".format(table))

# <headingcell level=4>

# Be careful to only export the columns you are ok with others seeing. Don't export IP address, original user_id, etc.

# <codecell>

csvExport(cursor, table, exported_file_path)

# <headingcell level=1>

# Stats on Original File

# <codecell>

# Commit the changes, and re-open the database.
dbClose(cursor)
cursor = dbOpen(db_name)

# <codecell>

cursor.execute("Pragma table_info({})".format(pristine_table_name))
cursor.fetchall()

# <codecell>
cursor.execute("SELECT COUNT(*) FROM {}".format(pristine_table_name))
total = cursor.fetchall()[0][0]
total

# <codecell>

view_qry = count_unique_values(cursor, pristine_table_name, "viewed")
view_dic = {}
for row in view_qry:
    view_dic[row[0]] = float(row[1]) / float(total)
view_dic

# <codecell>

exp_qry = count_unique_values(cursor, pristine_table_name, "explored")
exp_dic = {}
for row in exp_qry:
    exp_dic[row[0]] = float(row[1]) / float(total)
exp_dic

# <codecell>

cert_qry = count_unique_values(cursor, pristine_table_name, "certified")
cert_dic = {}
for row in cert_qry:
    cert_dic[row[0]] = float(row[1]) / float(total)
cert_dic

# <codecell>

gen_qry = count_unique_values(cursor, pristine_table_name, "gender")
gen_dic = {}
gen_total = total
for row in gen_qry:
    if row[0] == '' or row[0] == 'NA' or row[0] == 'o':
        gen_total -= row[1]
    else:
        gen_dic[row[0]] = float(row[1]) / float(gen_total)
gen_dic

# <codecell>

age_qry = count_unique_values(cursor, pristine_table_name, "YoB")
num = 0
denom = 0
for row in age_qry:
    try:
        age = 2013 - int(row[0])
    except:
        continue
    num += age * row[1]
    denom += row[1]
avg_age = float(num) / float(denom)
avg_age

# <headingcell level=2>

# Stats on De-identified file

# <codecell>

dbClose(cursor)
cursor = dbOpen(db_name)

# <codecell>

cursor.execute("SELECT COUNT(*) FROM {}".format(table))
total = cursor.fetchall()[0][0]
total

# <codecell>

view_qry = count_unique_values(cursor, table, "viewed")
view_dic = {}
for row in view_qry:
    view_dic[row[0]] = float(row[1]) / float(total)
view_dic

# <codecell>

exp_qry = count_unique_values(cursor, table, "explored")
exp_dic = {}
for row in exp_qry:
    exp_dic[row[0]] = float(row[1]) / float(total)
exp_dic

# <codecell>

cert_qry = count_unique_values(cursor, table, "certified")
cert_dic = {}
for row in cert_qry:
    cert_dic[row[0]] = float(row[1]) / float(total)
cert_dic

# <codecell>

gen_qry = count_unique_values(cursor, table, "gender")
gen_dic = {}
gen_total = total
for row in gen_qry:
    if row[0] == '' or row[0] == 'NA' or row[0] == 'o':
        gen_total -= row[1]
    else:
        gen_dic[row[0]] = float(row[1]) / float(gen_total)
gen_dic

# <codecell>

age_qry = count_unique_values(cursor, table, "YoB")
num = 0
denom = 0
for row in age_qry:
    try:
        age = 2013 - int(row[0])
    except:
        continue
    num += age * row[1]
    denom += row[1]
avg_age = float(num) / float(denom)
avg_age

# <codecell>

cursor.execute("Pragma database_list")
cursor.fetchall()

# <codecell>

count_unique_values(cursor, table, "YoB")

# <codecell>

uMatrix - preUmatrix
# This one taken after K-Anonymous

# <headingcell level=4>

# Good to close the db between uses, it compacts the data and prevents an error if you leave a cursor dangling.

# <codecell>

dbClose(cursor, closeFlag=True)
