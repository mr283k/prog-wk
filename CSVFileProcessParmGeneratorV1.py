""""
Input: COBOL Copybook
Output: Process Params, History Params, DDLs, Manifest Files

These parameter files will be used process CSV/Text Files
========================================================================================
|Date(YYYY/MM/DD)|Change Description                                        |Change Tag |
========================================================================================
|2022/10/20      | Initial Version                                          |           |
----------------------------------------------------------------------------------------
========================================================================================
"""

import json
import os
from copy import deepcopy

with open('./CSVFileParmGenerator_Params.json') as aparam:
    dictParams = json.load(aparam)

environment = dictParams['environment']
ddlFor = dictParams['ddlFor']
parquetDDLs = dictParams["parquetDDLs"]
manifestFor = dictParams['manifestFor']
tblPrefixDict = dictParams["tblPrefixDict"]
dataBucket = dictParams['dataBucket'][environment]
companyInfo = dictParams['company']


tblProperties = """ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY '\\t' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://{0}/{1}/'
TBLPROPERTIES (  
  'areColumnsQuoted'='false',   
  'classification'='csv', 
  'columnsOrdered'='true', 
  'compressionType'='none',
  'delimiter'='\\t',   
  'typeOfData'='file',
  'skip.header.line.count'='1');
"""

# parquet table properties
parquettblproperties = """PARTITIONED BY({2} {3}) 
STORED AS PARQUET
LOCATION 's3://{0}/{1}/'
tblproperties ("parquet.compress"="SNAPPY");
"""

# manifest Record
manifestRec = """{
 "fileLocations": [{
  "WildcardURIs": [
   "@#!#@"
  ]
 }, {
  "URIPrefixes": [
   "s3://$%^%$/"
  ]
 }],
 "settings": {
  "stopOnFail": "true"
 }
}
"""


def checkDir(argDir):
    return argDir[:-1] if argDir[-1] == '/' else argDir


def manifestCreator(argDataBucket, argS3Dir, argCompany, argFile, argTgtDir, argManifestFor='current'):
    nameSuffix = 'CDC_GLBL.manifest' if argManifestFor == 'delta' else 'GLBL.manifest'
    wildCardStr = f'{argS3Dir}/{argManifestFor}/{argCompany}_{argFile}_*.tab'
    outManifestFile = f'{argCompany}_{argFile}_{nameSuffix}'

    outDirectory = f'{argTgtDir}/{argS3Dir.rsplit("/",1)[1]}'
    if not os.path.exists(outDirectory):
        os.mkdir(outDirectory)
    outPath = outDirectory + '/' + outManifestFile
    mrec = manifestRec.replace('$%^%$', argDataBucket).replace('@#!#@', wildCardStr)
    with open(outPath, 'w') as mf:
        mf.write(mrec)
    return ''


def ddlCreator(argDBStr, argTBLStr, argElementsList, argfileObj, argS3Dir, argDDLFor):
    argfileObj.write(f'CREATE DATABASE IF NOT EXISTS {argDBStr};\n\n')
    argfileObj.write(f"CREATE EXTERNAL TABLE {argDBStr}.{argTBLStr}" + "\n" + '(' + '\n')

    totalColumns = len(argElementsList)
    typeChange = {}
    for indx, each in enumerate(argElementsList, 1):
        if '-' in each['Element']:
            raise Exception(f"<ERR> {each['Element']} has HYPHENS :: Pls Check")
        if ' ' in each['Element']:
            raise Exception(f"<ERR> '{each['Element']}' Has SPACES :: Pls Check")
        elif each['DataType'] in ['text', 'null', 'specialtext']:
            dtype = f"varchar({each['ByteLength']})"
        elif each['DataType'] in ['numeric', 'stnumeric', 'slnumeric']:
            if each['ByteLength'] >= 18:
                print(f"<WARNING> {each['Element']}:{each['DataType']} length is {each['ByteLength']}")
            dtype = f"decimal({each['ByteLength']},{each['Scale']})" if each['Scale'] else 'bigint'
            typeChange[each['Element']] = "long" if dtype == 'bigint' else dtype
        elif each['DataType'] in ['scomp3', 'ucomp3']:
            if each['ByteLength'] >= 9:
                print(f"<WARNING> {each['Element']}:{each['DataType']} length is {each['ByteLength']}")
            dtype = f"decimal({each['ByteLength'] * 2},{each['Scale']})" if each['Scale'] else 'bigint'
            typeChange[each['Element']] = "long" if dtype == 'bigint' else dtype
        elif each['DataType'] in ['hex']:
            dtype = f"varchar({each['ByteLength'] * 2})"
        elif each['DataType'] in ['sbinary', 'ubinary']:
            if each['ByteLength'] >= 8:
                print(f"<INFO> {each['Element']}:{each['DataType']} length is {each['ByteLength']}")
                dtype = f"decimal({len(str(2**(each['ByteLength']*8)))})"
            else:
                dtype = 'bigint'
            typeChange[each['Element']] = "long" if dtype == 'bigint' else dtype
        else:
            print(f"<WARNING> {each['Element']} Data type not included :: Pls Check")
            dtype = 'string'

        if argDDLFor in parquetDDLs and each['Element'].upper() == "CYCLEDATE":
            parquetCol = each['Element']
            parquetDtype = dtype
            continue

        if indx < totalColumns:
            argfileObj.write(f"{each['Element']}  {dtype}," + '\n')
        else:
            argfileObj.write(f"{each['Element']}  {dtype}")

    argfileObj.write('\n)\n')
    if argDDLFor in parquetDDLs:
        argfileObj.write(parquettblproperties.format(argS3Dir, argDDLFor, parquetCol, parquetDtype) + '\n\n')
    else:
        argfileObj.write(tblProperties.format(argS3Dir, argDDLFor) + '\n\n')
    return typeChange


def json_creator(fieldList, cpName):
    """Creates json template for fields"""

    if cpName not in dictParams:
        print(f'<ERROR> Input file parameter type not recognized for {cpName}')
        raise Exception(f'<ERROR> Input file parameter type not recognized for {cpName}')

    paramInfo = dictParams[cpName]
    response = {
        "Elements": [
            {
                "Element": "CycleDate",
                "DataType": "text",
                "ByteLength": "8",
                "Scale": None,
                "Key": False
            },
            {
                "Element": "RunDate",
                "DataType": "text",
                "ByteLength": "20",
                "Scale": None,
                "Key": False
            }
        ]
    }
    for count in range(len(fieldList)):

        if fieldList[count][10]:
            errMsg = f'{fieldList[count]} Has Occurs for text file process. Please Check'
            raise Exception(errMsg)

        if fieldList[count][12] != 'NOT REDEFINED' or fieldList[count][6] == 'G':
            continue

        if fieldList[count][6] == 'X':
            scale = fieldList[count][5] if fieldList[count][5] else None
            if scale:
                raise Exception(f'<ERROR> Alphanumeric Field {fieldList[count]} has Scale. Pls Check')

            response['Elements'].append(
                {"Element": fieldList[count][1], "DataType": "text", "ByteLength": fieldList[count][3],
                 "Scale": scale, "Key": False})

        elif fieldList[count][6] == 'N':
            scale = fieldList[count][5] if fieldList[count][5] else None
            response['Elements'].append(
                {"Element": fieldList[count][1],
                 "DataType": "numeric",
                 "ByteLength": fieldList[count][3],
                 "Scale": fieldList[count][5] if fieldList[count][5] else None, "Key": False})

        elif fieldList[count][6] == 'P':
            if fieldList[count][7] == 'SP':
                response['Elements'].append(
                    {"Element": fieldList[count][1], "DataType": "scomp3", "ByteLength": fieldList[count][3],
                     "Scale": fieldList[count][5] if fieldList[count][5] else None, "Key": False})

            else:
                response['Elements'].append(
                    {"Element": fieldList[count][1], "DataType": "ucomp3", "ByteLength": fieldList[count][3],
                     "Scale": fieldList[count][5] if fieldList[count][5] else None, "Key": False})
        elif fieldList[count][6] == 'B':
            if fieldList[count][7] == 'SB':
                response['Elements'].append(
                    {"Element": fieldList[count][1], "DataType": "sbinary", "ByteLength": fieldList[count][3],
                     "Scale": fieldList[count][5] if fieldList[count][5] else None, "Key": False})

            else:
                response['Elements'].append(
                    {"Element": fieldList[count][1], "DataType": "ubinary", "ByteLength": fieldList[count][3],
                     "Scale": fieldList[count][5] if fieldList[count][5] else None, "Key": False})
        else:
            errMsg = f'{fieldList[count]} Has invalid picture clause. Please Check'
            raise Exception(errMsg)
    response['Elements'].append(
        {"Element": "UnKnown", "DataType": "text", "ByteLength": 2000, "Scale": None, "Key": False})
    return response, paramInfo


def len_calculator(inputDType, inputCType):
    """ Function to calculate DISPLAY length"""
    if '(' in inputDType:
        outputLength = int(inputDType[inputDType.index('(') + 1:inputDType.index(')')])
    else:
        outputLength = inputDType.count(inputCType)
    return outputLength


def details_extractor(inputField):
    """ Function to extract details of field """

    fieldLevel = int(inputField[0])
    fieldName = inputField[1]
    fieldPosition = 0
    fieldStorageLen = 0
    fieldDisplayLength = 0
    fieldScale = 0
    fieldPictureType = 'G'
    fieldSignStat = 'NS'
    fieldRedefined = 'NR'
    fieldRedefines = 'NRS'
    fieldOccurs = 0
    fieldRedefinedVariable = ''
    fieldDependingOn = None
    fieldRedefined_field = "NOT REDEFINED"
    if 'PIC' in inputField:
        fieldDataType = inputField[inputField.index('PIC') + 1]
        if 'X' in fieldDataType:
            fieldPictureType = 'X'
            fieldDisplayLength = len_calculator(fieldDataType, 'X')
            fieldStorageLen = fieldDisplayLength
            fieldSignStat = 'NS'
        elif 'A' in fieldDataType:
            fieldPictureType = 'A'
            fieldDisplayLength = len_calculator(fieldDataType, 'A')
            fieldStorageLen = fieldDisplayLength
            fieldSignStat = 'NS'
        elif '9' in fieldDataType:
            signFlag = 'N'
            signPos = 'T'
            if fieldDataType[0] == 'S':
                signFlag = 'Y'
                fieldDataType = fieldDataType[1:]
                if 'LEADING' in inputField:
                    signPos = 'L'
            fieldDataTypeParts = fieldDataType.split('V', 1)
            fieldDisplayLength = len_calculator(fieldDataTypeParts[0], '9')
            if len(fieldDataTypeParts) == 2:
                fieldScale = len_calculator(fieldDataTypeParts[1], '9')
            fieldDisplayLength += fieldScale
            if 'COMPUTATIONAL'  in inputField[inputField.index('PIC')+1:] or 'COMP' in inputField[inputField.index('PIC')+1:] :
                fieldPictureType = 'B'
                if 1 <= fieldDisplayLength <= 4:
                    fieldStorageLen = 2
                elif 5 <= fieldDisplayLength <= 9:
                    fieldStorageLen = 4
                else:
                    fieldStorageLen = 8
                if signFlag == 'Y':
                    fieldSignStat = 'SB'
            elif 'COMPUTATIONAL-3'  in inputField[inputField.index('PIC')+1:] or 'COMP-3' in inputField[inputField.index('PIC')+1:] :
                fieldStorageLen = (fieldDisplayLength // 2) + 1
                fieldPictureType = 'P'
                if signFlag == 'Y':
                    fieldSignStat = 'SP'
            else:
                fieldStorageLen = fieldDisplayLength
                fieldPictureType = 'N'
                if signFlag == 'Y':
                    if signPos == 'L':
                        fieldSignStat = 'SL'
                    else:
                        fieldSignStat = 'ST'
    else:
        groupUsageClauseFlags = map(lambda ix: True if ix in inputField else False,
                                    ['COMPUTATIONAL', 'COMPUTATIONAL-3', 'COMP', 'COMP-3'])
        if any(groupUsageClauseFlags):
            raise Exception(f'{eachFile}::Group::{field[1]} Has Group level Usage Clause-Check and Update')

    if 'COMPUTATIONAL-1' in inputField or 'COMP-1' in inputField:
        fieldPictureType = 'B1'
        fieldStorageLen = 4
        fieldDisplayLength = 16
        fieldSignStat = 'SB'
    elif 'COMPUTATIONAL-2' in inputField or 'COMP-2' in inputField:
        fieldPictureType = 'B2'
        fieldStorageLen = 8
        fieldDisplayLength = 32
        fieldSignStat = 'SB'
    if 'OCCURS' in inputField:
        fieldOccurs = int(inputField[inputField.index('TIMES') - 1])
        fieldDependingOn = inputField[inputField.index('DEPENDING') + 2] if 'DEPENDING' in inputField else None
    if 'REDEFINES' in inputField:
        fieldRedefines = 'RS'
        fieldRedefinedVariable = inputField[inputField.index('REDEFINES') + 1]
        fieldRedefined_field = "REDEFINED"

    outputField = [fieldLevel, fieldName, fieldPosition, fieldStorageLen, fieldDisplayLength, fieldScale,
                   fieldPictureType, fieldSignStat, fieldRedefined, fieldRedefines, fieldOccurs, fieldRedefinedVariable,
                   fieldRedefined_field, fieldDependingOn]
    return outputField


def start_position_calculator(fieldsList):
    flag = True
    fieldCount = 1
    while flag:
        siblingGroupCount = fieldCount - 1
        if fieldCount == len(fieldsList):
            break
        if not fieldsList[fieldCount][11]:  # not redefined
            if fieldsList[fieldCount][0] < fieldsList[fieldCount - 1][0]:
                if fieldsList[fieldCount][0] == 1:
                    fieldsList[fieldCount][2] = 0
                    fieldCount += 1
                    continue
                while fieldsList[fieldCount][0] != fieldsList[siblingGroupCount][0]:
                    siblingGroupCount -= 1

                fieldsList[fieldCount][2] = fieldsList[siblingGroupCount][2] + fieldsList[siblingGroupCount][3] * \
                                            fieldsList[siblingGroupCount][10]
            else:
                if fieldsList[fieldCount][0] == 1:
                    fieldsList[fieldCount][2] = 0
                    fieldCount += 1
                    continue
                if fieldsList[fieldCount - 1][6] == 'G':
                    fieldsList[fieldCount][2] = fieldsList[fieldCount - 1][2]
                    fieldCount += 1
                    continue
                while fieldsList[siblingGroupCount][11]:
                    siblingGroupCount -= 1
                fieldsList[fieldCount][2] = fieldsList[siblingGroupCount][2] + fieldsList[siblingGroupCount][3] * \
                                            fieldsList[siblingGroupCount][10]
        else:

            while fieldsList[siblingGroupCount][1] != fieldsList[fieldCount][11]:
                siblingGroupCount -= 1
            fieldsList[fieldCount][2] = fieldsList[siblingGroupCount][2]
        fieldCount += 1

    return fieldsList


def checkFieldName(inDetail,fieldNames=None, occursLvl=None):
    lvl = inDetail[0]
    if occursLvl and (lvl <= occursLvl[-1]):
        occursLvl.pop()
    inkey = occursLvl[-1] if occursLvl else 'default'
    if inDetail[1] in fieldNames[inkey]:
        errTrlrField = fieldNames['default'][0].replace('_', '-')
        raise Exception(f"{errTrlrField}::{inDetail[1].replace('_','-')} Has duplicates. Please check")
    if inDetail[10]:
        occursLvl.append(lvl)
        fieldNames[lvl] = [inDetail[1]]
    else:
        fieldNames[inkey].append(inDetail[1])


"""----------------------------------------------------------------------------------------------- """
"""                                             MAIN LOGIC                                         """
"""----------------------------------------------------------------------------------------------- """

file_count = 0

copyBookDirectory = 'H:/EBCIDIC2ASCII/Mainframe Parsing/MainDetails/TextFileProcess/InCopyBooks'
jsonFilesDirectory = 'H:/EBCIDIC2ASCII/Mainframe Parsing/MainDetails/TextFileProcess/OutJson'
manifestDirPath = 'H:/EBCIDIC2ASCII/Mainframe Parsing/MainDetails/TextFileProcess/OutManifest'
ddlDirPath = 'H:/EBCIDIC2ASCII/Mainframe Parsing/MainDetails/TextFileProcess/OutDDLs'
replaceHyphenInColumn = True
replaceChar = '_' if replaceHyphenInColumn else '-'
createDB = True

copyBookDirectory = checkDir(copyBookDirectory)
jsonFilesDirectory = checkDir(jsonFilesDirectory)
manifestDirPath = checkDir(manifestDirPath)
ddlDirPath = checkDir(ddlDirPath)


for rootDir, folders, fileNames in os.walk(copyBookDirectory):
    roorDir = rootDir + ('' if rootDir[-1] == '/' else '/')
    for eachFile in fileNames:
        file_count += 1
        print('-' * 65)
        print(f'{file_count} Processing: {rootDir}/{eachFile}')
        prevLevel = 0
        prevFldisNotGroup = False
        prevLine = ''
        with open(rootDir + '/' + eachFile) as copyBookLayout:
            """"
                Reads every file in the copy book folder and creates a list with each element line for processing
            """
            newLine = str()
            loadedLayout = list()
            for layout_line in copyBookLayout:
                tempLine = layout_line[6:72].strip()
                if tempLine == '':
                    continue
                if tempLine[0] != '*':
                    if tempLine.strip()[-1] != '.':
                        newLine = newLine.strip() + ' ' + tempLine
                    else:
                        finalLine = newLine.strip() + ' ' + tempLine[:-1]
                        if finalLine.count(' PIC ') >= 2 or finalLine.count(' PICTURE ') >= 2:
                            raise Exception(f'{eachFile}:: FIELDS:1: {finalLine} not in Cobol Standard')

                        if (finalLine.count(' PIC ') > 0 or finalLine.count(' PICTURE ') > 0) \
                                and finalLine.strip()[0:2] == '88':
                            raise Exception(f'{eachFile}:: FIELDS:2: {finalLine} not in Cobol Standard')

                        if finalLine.strip()[0:2] != '88':
                            bkpLine = finalLine.strip()
                            details = "".join(finalLine.strip().split(' VALUE ', 1)[0]).split()
                            finalLine = details[:]
                            if ('PIC' in finalLine or 'PICTURE' in finalLine) \
                                    and int(finalLine[0]) > prevLevel \
                                    and prevFldisNotGroup:
                                raise Exception(f'{eachFile}:: FIELDS:3: {prevLine} not in Cobol Standard')

                            if len(finalLine) > 2 and finalLine[0].isnumeric and finalLine[2].isnumeric():
                                raise Exception(f'{eachFile}:: FIELDS:4: {bkpLine} not in Cobol Standard')

                            if len(finalLine) > 2 and 'OCCURS' in finalLine[2:] and 'TIMES' not in finalLine[2:]:
                                raise Exception(f'{eachFile}:: FIELDS:5: {bkpLine} not in Cobol Standard')

                            if len(finalLine) > 2 \
                                    and 'DEPENDING' in finalLine[2:] \
                                    and ('OCCURS' not in finalLine[2:] or 'ON' not in finalLine[2:]):
                                raise Exception(f'{eachFile}:: FIELDS:6: {bkpLine} not in Cobol Standard')

                            if len(finalLine) > 1 and int(finalLine[0]) == 2 and finalLine[1][:4] != 'TRLR':
                                raise Exception(f'{eachFile}:: FIELDS:7: {bkpLine} '
                                                f'not in Standard Format for Processing')

                            if len(finalLine) > 2 and 'OCCURS' in finalLine[2:] and 'DEPENDINGON' in finalLine[2:]:
                                raise Exception(f'{eachFile}:: FIELDS:5: {bkpLine} not in Cobol Standard')

                            if len(finalLine) > 2 \
                                    and 'OCCURS' not in finalLine[2:] \
                                    and 'REDEFINES' not in finalLine[2:] \
                                    and 'PIC' not in finalLine \
                                    and 'PICTURE' not in finalLine:
                                errMsg = f'{eachFile}:: FIELDS:8: {bkpLine} not in Cobol Standard. Check PIC clause'
                                raise Exception(errMsg)

                            if 'OCCURS' in finalLine[2:]:
                                tline = bkpLine.split("OCCURS", 1)[0]
                                if len(tline.strip().split()) > 2 \
                                        and 'REDEFINES' not in finalLine[2:] \
                                        and 'PIC' not in finalLine \
                                        and 'PICTURE' not in finalLine:
                                    errMsg = f'{eachFile}:: FIELDS:9: {bkpLine} not in Cobol Standard. Check PIC clause'
                                    raise Exception(errMsg)

                                if len(tline.strip().split()) > 4 \
                                        and 'REDEFINES' in finalLine[2:] \
                                        and 'PIC' not in finalLine \
                                        and 'PICTURE' not in finalLine:
                                    errMsg = f'{eachFile}:: FIELDS:10: {bkpLine} not in Cobol Standard. Check PIC clause'
                                    raise Exception(errMsg)

                            if 'REDEFINES' in finalLine[2:] \
                                    and 'OCCURS' not in finalLine[2:] \
                                    and len(finalLine) > 4 \
                                    and 'PIC' not in finalLine \
                                    and 'PICTURE' not in finalLine:
                                errMsg = f'{eachFile}:: FIELDS:11: {bkpLine} not in Cobol Standard. Check PIC clause'
                                raise Exception(errMsg)

                            if prevLevel == 1 and int(finalLine[0]) != 2:
                                raise Exception(f'{eachFile}:: {bkpLine}: 02 Level field is missed')

                            loadedLayout.append(finalLine)
                            prevLevel = int(finalLine[0])
                            prevFldisNotGroup = True if 'PIC' in finalLine or 'PICTURE' in finalLine else False
                            prevLine = bkpLine
                        newLine = ''
        copyBookLayout.close()

        """ Extracts the cobol copybook field details and stores the details in list """
        layout = []
        for field in loadedLayout:
            fieldLength = len(field)
            if fieldLength >= 2:
                if 'PICTURE' in field:
                    print(eachFile + ':: PIC ::' + field[1] + ' In Different Format')
                    field[field.index('PICTURE')] = 'PIC'
                outDetails = details_extractor(field)
                if replaceHyphenInColumn:
                    outDetails[1] = outDetails[1].replace('-', replaceChar)
                    outDetails[11] = outDetails[11].replace('-', replaceChar)
                    outDetails[13] = outDetails[13].replace('-', replaceChar) if outDetails[13] else outDetails[13]
                layout.append(outDetails)
        loadedLayout = deepcopy(layout)

        """
        (lvl,name,pos,storage-len,disp-len,scale,picture,signstat,redefined,redefines,occurs,redefined var,Redefined,
                                                                                                           Depending On)
        (0  ,1   ,2  ,3          ,4       ,5    ,6      ,7       ,8        ,9        ,10    ,11           ,12      ,13)
        """

        """Add Redefines of GROUP to Field Level"""
        count = 0
        while count < len(loadedLayout):
            if loadedLayout[count][6] == 'G' and loadedLayout[count][12] == "REDEFINED":
                base = count
                count += 1
                while count < len(loadedLayout) and int(loadedLayout[base][0]) < int(loadedLayout[count][0]):
                    loadedLayout[count][12] = "REDEFINED"
                    count += 1
            else:
                count += 1

        """ Checks field name uniqueness"""
        for field in loadedLayout:
            if field[12] == "REDEFINED":
                continue
            if int(field[0]) == 1:
                checkFieldName.__defaults__ = ({'default': []}, [])
            checkFieldName(field)

        """ calculates Group lengths """
        groupCheck = 'N'
        if loadedLayout[0][6] == 'G':
            groupCheck = 'Y'

        while groupCheck == 'Y':
            groupCheck = 'N'
            groupLength = 0
            groupLevel = 0
            subGroupLevel = 0
            groupIndex = -1
            countInGroup = 0
            layout = deepcopy(loadedLayout)
            loadedLayout = []
            slenList = []
            for tempField in layout:
                loadedLayout.append(tempField)
                currentLevel = int(tempField[0])
                slenList.append(tempField[:4])
                if currentLevel <= groupLevel and groupLength > 0:
                    loadedLayout[groupIndex][3] = groupLength
                    loadedLayout[groupIndex][4] = groupLength
                    slenList[groupIndex] = loadedLayout[groupIndex][:4]
                    groupLength = 0
                    groupLevel = 0
                    subGroupLevel = 0
                    groupIndex = -1
                if tempField[11] == '' and tempField[3] == 0 and tempField[6] == 'G':
                    groupLevel = currentLevel
                    groupIndex = layout.index(tempField)
                    groupLength = 0
                    countInGroup = 0
                    subGroupLevel = 0
                    groupCheck = 'Y'
                    continue
                if currentLevel > groupLevel and tempField[11] == '' and groupIndex >= 0:
                    countInGroup += 1
                    if countInGroup == 1:
                        subGroupLevel = currentLevel
                    if currentLevel == subGroupLevel:
                        groupLength += tempField[3]
                if tempField[11] != '' and tempField[3] == 0 and tempField[6] == 'G':
                    prevSlength = 0
                    for t_idx in range(len(slenList) - 2, -1, -1):
                        tfd = slenList[t_idx]
                        if tfd[0] == tempField[0] and tfd[1].find(tempField[11]) == 0:
                            prevSlength = tfd[3]
                            break
                    if prevSlength == 0:
                        groupCheck = 'Y'
                        continue
                    tempField[3] = prevSlength
                    tempField[4] = prevSlength
                    slenList[-1][3] = prevSlength
                    loadedLayout[-1] = list(tempField)
            if groupLength > 0:
                loadedLayout[groupIndex][3] = groupLength
                loadedLayout[groupIndex][4] = groupLength
                slenList[groupIndex] = loadedLayout[groupIndex][:4]

        """ Calculates starting position of fields """
        outputLayout = start_position_calculator(loadedLayout)
        fileName = eachFile.rsplit('.', 1)[0]
        out_json, DDLInfo = json_creator(outputLayout, fileName)
        # print(json.dumps(out_json, indent=2))

        s3Dir = checkDir(DDLInfo['s3Dir'])
        s3BaseFolder, s3FileFolder = s3Dir.rsplit('/', 1)
        cName = companyInfo[s3BaseFolder].upper()
        fName = fileName.split("_", 1)[0].upper()

        # Building target param names
        historyParmName = f'{cName.lower()}_history_params_{fName}.json'
        normalParmName = f'{cName.lower()}_processing_params_{fName}.json'

        # Building normal processing params
        normalParams = {
            "Company": cName,
            "FileName": fName,
            "FileExtention": "tab",
            "ProcessSource": True,
            "Delimiter": ",",
            "QuoteCharacter": '\"',
            "Header": False,
            "Elements": [each['Element'] for each in out_json["Elements"]],
            "CycleDateInfo": {
                "CNTLFile": True,
                "Default": {
                    "Pattern": f'GlobeLife_DataExtract_{fName}_([0-9]'+'{8}'+')\\..*',
                    "MatchIdx": 1,
                    "Format": "%Y%m%d"
                }
            },
            "PreviousFolder": f"raw/{s3Dir}/previous/",
            "HistoryProcess": {
                "GlueJobName": "gl-cdm-lake-mt-process",
                "HistoryParams": f"config/Data-Pipelines/{s3BaseFolder}/{historyParmName}"
            }
        }

        with open(f'{jsonFilesDirectory}/{normalParmName}', 'w') as nParam:
            nParam.write(json.dumps(normalParams, indent=2))

        dbName = DDLInfo['dbName']
        typeCast = {}
        for each in ddlFor:
            tblName = f"{tblPrefixDict[each]}_{DDLInfo.get('tblName', fName.lower())}"
            ddlFile = f'{ddlDirPath}/{environment.upper()}_{cName}_{fName}_{each.upper()}.txt'
            ddlFileObj = open(ddlFile, 'w')
            typeCast = ddlCreator(
                dbName,
                tblName,
                out_json["Elements"],
                ddlFileObj,
                f'{dataBucket}/raw/{s3BaseFolder}/{s3FileFolder}',
                each
            )

        # building history params
        historyTblName = f"{tblPrefixDict['history']}_{DDLInfo.get('tblName', fName.lower())}"
        historyParams = {
            "Company": cName,
            "FileName": fName,
            "Header": True,
            "Delimiter": "\t",
            "HistoryFolder": f"raw/{s3Dir}/history",
            "AthenaTable": f'{dbName}.{historyTblName}',
            "TypeCast": typeCast
        }
        with open(f'{jsonFilesDirectory}/{historyParmName}', 'w') as hParam:
            hParam.write(json.dumps(historyParams, indent=2))

        # Creating Manifest
        manifestCreator(dataBucket, f'raw/{s3BaseFolder}/{s3FileFolder}', cName, fName, manifestDirPath, manifestFor)
