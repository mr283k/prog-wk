""""
Parameter file generation for cobol copy book data data extraction
Occurs: No
Depending On : No
Redefines : No
========================================================================================
|Date(YYYY/MM/DD)|Change Description                                        |Change Tag |
========================================================================================
|2021/01/01      | Initial version                                          |           |
----------------------------------------------------------------------------------------
|2021/01/06      | Added in param trailer id duplicate check logic          | C20210106 |
|                | separated cobol initial params to different location     |           |
|                | Added period '.' check in initial params key             |           |
========================================================================================
"""

import json
import os


def groups_splitter(fieldList, level):
    """Recursive function for inner groups  and data extraction for Elements"""
    global count, maxcount, replaceChar, prevBucket, prevFolder

    elements = []
    while count < maxcount:
        elementValue = {}
        if fieldList[count][0] < level or (fieldList[count][0] == level and elements):
            break

        if fieldList[count][6] == 'G' and fieldList[count][12] == 'NOT REDEFINED':
            low = count
            temp = fieldList[count]
            tempList = []
            count += 1
            groupReturnedParams = groups_splitter(fieldList, int(fieldList[count - 1][0]))

            if temp[12] == 'NOT REDEFINED':
                if int(temp[0]) == 1:
                    return groupReturnedParams
                if int(temp[0]) == 2:
                    groupReturnedParams.append(
                        {"Element": "UnKnown", "DataType": "text", "ByteLength": 1000, "Scale": None, "Key": False})
                    try:
                        indexOfTRLR = temp[1][4:].index(replaceChar)
                        elements.append({temp[1][4:4+indexOfTRLR]: {
                            "PreviousBucket": prevBucket,
                            "PreviousFolder": prevFolder + temp[1][4:4+indexOfTRLR],
                            "Elements": groupReturnedParams
                            # "DependingOnList": [each[13] for each in fieldList[low:count] if each[13]]
                            }
                        })
                    except:
                        raise Exception(temp[1] + ":: 02 Level doesn't have TRLR - in it.Please correct and try again.")
                elif int(temp[0] > 2):
                    if temp[10] >= 1:
                        print("<ERROR> Copy book has occurs")
                        raise Exception("<ERROR> Copy book has occurs")
                    else:
                        elements.extend(groupReturnedParams)
            count -= 1
        else:
            if fieldList[count][12] == 'NOT REDEFINED':
                if fieldList[count][6] == 'X':
                    elementValue.update(
                        {"Element": fieldList[count][1], "DataType": "text", "ByteLength": fieldList[count][3],
                         "Scale": fieldList[count][5] if fieldList[count][5] else None, "Key": False})
                elif fieldList[count][6] == 'N':
                    elementValue.update(
                        {"Element": fieldList[count][1], "DataType": "numeric", "ByteLength": fieldList[count][3],
                         "Scale": fieldList[count][5] if fieldList[count][5] else None, "Key": False})
                else:
                    print(f"<ERROR> Invalid Datatype = {fieldList[count][6]}")
                    raise Exception(f"<ERROR> Invalid Picture Type = {fieldList[count][6]}")

                if fieldList[count][10] >= 1:
                    print("<ERROR> Copy book has occurs")
                    raise Exception("<ERROR> Copy book has occurs")
                else:
                    elements.append(elementValue)

        count += 1

    return elements


def json_creator(fieldList, fileName, dispWarning=True):
    """Creates json template for fields"""

    global count, maxcount, File_Name, replaceChar, replaceHyphenInColumn, prevBucket, prevFolder, parmFile
    global s3BaseFolder
    recordsList = [each for each in range(len(fieldList)) if fieldList[each][0] == 1]
    recordTypes = {}

    with open('./COBOLParse_Params.json') as fparam:
        fileparams = json.load(fparam)
    cpName = fileName[:-4]
    if cpName in fileparams:
        paramtype = fileparams[cpName]
    else:  # input type unknown
        raise Exception('Input file parameter type not recognized; aborting execution:', fileName)

    parmFile = paramtype[0]
    s3FileFolder = paramtype[2]
    s3BaseFolder = paramtype[1]
    prevFolder = f"raw/{s3BaseFolder}/{s3FileFolder}/previous/{s3FileFolder}-"

    configPath = 'H:/EBCIDIC2ASCII/Mainframe Parsing/MainDetails/JsonFiles/InCobolParams/'                   # C20210106
    with open(configPath + '/' + parmFile) as f:
        iProcessParams = json.loads(f.read())
    File_Name = iProcessParams["FileName"]
    company = iProcessParams["Company"]
    frquency = iProcessParams["Frequency"]
    Addspaces = iProcessParams["AddLeadingSpaces"]
    Removespaces = iProcessParams["RemoveLeadingCharacters"]
    DataEtract = iProcessParams["DataExtractionInfo"]
    listTIDs = sorted(list(map(lambda inE: DataEtract[inE]['TrailerID'], DataEtract.keys())))                # C20210106
    setTIDs = list(set(listTIDs))                                                                            # C20210106
    if listTIDs != sorted(setTIDs):                                                                          # C20210106
        print(f'DataExtractionInfo TIDs: {listTIDs}')                                                        # C20210106
        raise Exception(f"<ERROR>DataExtractionInfo TIDS have '{len(listTIDs) - len(setTIDs)}' duplicates")  # C20210106

    filtTIDs = list(filter(lambda inE: '.' in inE, DataEtract.keys()))                                       # C20210106
    if filtTIDs:                                                                                             # C20210106
        print(f'DataExtractionInfo Keys have periods: {filtTIDs}')                                           # C20210106
        raise Exception(f"<ERROR>DataExtractionInfo Keys have periods: {filtTIDs}")                          # C20210106

    outJson = {
        "Company": company,
        "FileName": File_Name,
        "Frequency": frquency,
        "AddLeadingSpaces": Addspaces,
        "RemoveLeadingCharacters": Removespaces,
        "DataExtractionInfo": DataEtract
    }

    count = 0

    if len(recordsList) == 1:
        maxcount = len(fieldList) - 1
        recordTypes.update(groups_splitter(fieldList, 1)[0])

    for i in range(len(recordsList)):
        start = recordsList[i]
        stop = len(fieldList) if i == len(recordsList) - 1 else recordsList[i + 1]
        count = 0
        maxcount = stop - start - 1
        recordTypes.update(groups_splitter(fieldList[start + 1:stop], 1)[0])
    processedTIDs = sorted(list(recordTypes.keys()))
    if listTIDs != processedTIDs:
        print(f"<ERROR> TIDS are not matching {listTIDs} != {processedTIDs}")
        print('ParmTIDs - CpyTIDs: ', set(listTIDs) - set(processedTIDs))
        print('CpyTIDs - ParmTIDs: ', set(processedTIDs) - set(listTIDs))
        raise Exception(f"<ERROR> TIDS are not matching {listTIDs} != {processedTIDs}")

    outJson.update({"RecordTypes": recordTypes})
    return outJson


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
        # print(f"{errTrlrField}::{inDetail[1].replace('_','-')} Has duplicates")                             
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

copyBookDirectory = 'H:/EBCIDIC2ASCII/Mainframe Parsing/MainDetails/JsonFiles/InCopyBooks'                    
jsonFilesDirectory = 'H:/EBCIDIC2ASCII/Mainframe Parsing/MainDetails/JsonFiles/JsonFiles'                     
replaceHyphenInColumn = True
replaceChar = '_' if replaceHyphenInColumn else '-'
prevBucket = 'tmk-cdm-data'
prevFolder = ''
printWarning = False
for rootDir, folders, fileNames in os.walk(copyBookDirectory):
    roorDir = rootDir + ('' if rootDir[-1] == '/' else '/')                                                   
    for eachFile in fileNames:
        if eachFile.rsplit('.', 1)[1] in ('json', 'JSON'):
            continue
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
                        if finalLine.count(' PIC ') >= 2:
                            raise Exception(f'{eachFile}:: FIELDS:1: {finalLine} not in Cobol Standard')      

                        if finalLine.count(' PIC ') > 0 and finalLine.strip()[0:2] == '88':
                            raise Exception(f'{eachFile}:: FIELDS:2: {finalLine} not in Cobol Standard')      

                        if finalLine.strip()[0:2] != '88':
                            bkpLine = finalLine.strip()
                            details = "".join(finalLine.strip().split(' VALUE ', 1)[0]).split()
                            finalLine = details[:]
                            if 'PIC' in finalLine and int(finalLine[0]) > prevLevel and prevFldisNotGroup:
                                raise Exception(f'{eachFile}:: FIELDS:3: {prevLine} not in Cobol Standard')   

                            if len(finalLine) > 2 and finalLine[0].isnumeric and finalLine[2].isnumeric():
                                raise Exception(f'{eachFile}:: FIELDS:4: {bkpLine} not in Cobol Standard')    

                            if len(finalLine) > 2 and 'OCCURS' in finalLine[2:] and 'TIMES' not in finalLine[2:]:
                                raise Exception(f'{eachFile}:: FIELDS:5: {bkpLine} not in Cobol Standard')    

                            if len(finalLine) > 2 and 'DEPENDING' in finalLine[2:] and 'OCCURS' not in finalLine[2:]:
                                raise Exception(f'{eachFile}:: FIELDS:6: {bkpLine} not in Cobol Standard')    

                            if len(finalLine) > 1 and int(finalLine[0]) == 2 and finalLine[1][:4] != 'TRLR':
                                raise Exception(f'{eachFile}:: FIELDS:7: {bkpLine} '                          
                                                f'not in Standard Format for Processing')                     

                            if prevLevel == 1 and int(finalLine[0]) != 2:
                                raise Exception(f'{eachFile}:: {bkpLine}: 02 Level field is missed')          

                            loadedLayout.append(finalLine)
                            prevLevel = int(finalLine[0])
                            prevFldisNotGroup = True if 'PIC' in finalLine else False
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
        loadedLayout = layout.copy()

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
            layout = loadedLayout.copy()
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
        out_json = json_creator(outputLayout, eachFile, dispWarning=printWarning)
        with open(f'{jsonFilesDirectory}/{s3BaseFolder.split("/", 1)[0]}/{parmFile}', 'w+') as jsonfile:     # C20210106
            print(jsonfile.name, "created")
            json.dump(out_json, jsonfile, indent=2)
