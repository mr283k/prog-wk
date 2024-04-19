""""
Parameter file generation without Redefines
Occurs: Yes
Depending On : yes
Redefines : No
========================================================================================
|Date(YYYY/MM/DD)|Change Description                                        |Change Tag |
========================================================================================
|2020/09/11      | Added new logic to check field uniqueness in each trailer| UPR000001 |
----------------------------------------------------------------------------------------
|2020/10/28      | Changed input and output folder paths to bring similarity| UPR000002 |
|                | among other automation scripts                           |           |
----------------------------------------------------------------------------------------
|2021/07/22      | Added new logic to check cycleDate length, spaces in MRID| UPR000003 |
|                | and LENGHT['MRID'] is equal to length of all MRID fields |           |
----------------------------------------------------------------------------------------
|2022/09/01      | Added new logic to check scale for numeric fields and    | UPR000004 |
|                | populated numeric data type for such fields              |           |
|                | updated shallow copy to deep copy                        |           |
|                | updated logic to process copybook even if splitting      |           |
|                | params not available                                     |           |
|                | Added new logic to check PIC clause for non group fields |           |
|                | Added logic to handle special trailer prefix             |           |
----------------------------------------------------------------------------------------
|2022/09/26      | Added logic to check duplicate MRID fields in splitting  | UPR000005 |
|                | parameters                                               |           |
|                | Added warning message if splitting params are            |           |
|                | not available                                            |           |
----------------------------------------------------------------------------------------
|2022/12/23      | Added logic to populate RecordPreface even though master,| UPR000006 |
|                | default in split params doesnt have MRID fields          |           |                                   |           |
========================================================================================
"""

import json
import os
from copy import deepcopy                                                                                    # UPR000004


def groups_splitter(fieldList, level):
    """Recursive function for inner groups  and data extraction for Elements"""
    global count, maxcount, prevFolder

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
                        {"Element": "UnKnown", "DataType": "hex", "ByteLength": 1000, "Scale": None, "Key": False})
                    try:
                        dependingOnList = []
                        for each in fieldList[low:count]:
                            if each[13] and each[13] not in dependingOnList:
                                dependingOnList.append(each[13])

                        indexOfTRLR = temp[1][4:].index(replaceChar)
                        elements.append({temp[1][4:4+indexOfTRLR]: {
                            "PreviousBucket": prevBucket,
                            "PreviousFolder": prevFolder + temp[1][4:4+indexOfTRLR],
                            "Elements": groupReturnedParams,
                            "DependingOnList": dependingOnList}
                        })
                    except:
                        raise Exception(temp[1] + ":: 02 Level doesn't have TRLR - in it.Please correct and try again.")
                elif int(temp[0] > 2):
                    if temp[10] >= 1:
                        tempList.extend(groupReturnedParams)
                        tempList.append(
                            {"times": temp[10], "dependingOn": temp[13]} if temp[13] else {"times": temp[10]})
                        elements.append(tempList)
                    else:
                        elements.extend(groupReturnedParams)
            count -= 1
        else:
            if fieldList[count][12] == 'NOT REDEFINED':
                if fieldList[count][6] == 'X':
                    scale = fieldList[count][5] if fieldList[count][5] else None
                    if scale:
                        raise Exception(f'<ERROR> Alphanumeric Field {fieldList[count]} has Scale. Pls Check')

                    elementValue.update(
                        {"Element": fieldList[count][1], "DataType": "text", "ByteLength": fieldList[count][3],
                         "Scale": scale, "Key": False})

                if fieldList[count][6] == 'N':
                    scale = fieldList[count][5] if fieldList[count][5] else None
                    if scale:
                        print(f"<DEBUG> {fieldList[count][1]} has scale for numeric datatype")
                    elementValue.update(
                        {"Element": fieldList[count][1],
                         "DataType": "numeric" if scale else ("text" if numericWithoutScaleAsText else "numeric"),
                         "ByteLength": fieldList[count][3],
                         "Scale": fieldList[count][5] if fieldList[count][5] else None, "Key": False})

                elif fieldList[count][6] == 'P':
                    if fieldList[count][7] == 'SP':
                        elementValue.update(
                            {"Element": fieldList[count][1], "DataType": "scomp3", "ByteLength": fieldList[count][3],
                             "Scale": fieldList[count][5] if fieldList[count][5] else None, "Key": False})

                    else:
                        elementValue.update(
                            {"Element": fieldList[count][1], "DataType": "ucomp3", "ByteLength": fieldList[count][3],
                             "Scale": fieldList[count][5] if fieldList[count][5] else None, "Key": False})

                elif fieldList[count][6] == 'B':
                    if fieldList[count][7] == 'SB':
                        elementValue.update(
                            {"Element": fieldList[count][1], "DataType": "sbinary", "ByteLength": fieldList[count][3],
                             "Scale": fieldList[count][5] if fieldList[count][5] else None, "Key": False})

                    else:
                        elementValue.update(
                            {"Element": fieldList[count][1], "DataType": "ubinary", "ByteLength": fieldList[count][3],
                             "Scale": fieldList[count][5] if fieldList[count][5] else None, "Key": False})
                if fieldList[count][10] >= 1:
                    elements.append([elementValue,
                                     {"times": fieldList[count][10], "dependingOn": fieldList[count][13]} if
                                     fieldList[count][13] else {"times": fieldList[count][10]}])
                else:
                    elements.append(elementValue)

        count += 1

    return elements


def build_preface(argSplitParmFile):

    splitParmFullPath = f'H:/EBCIDIC2ASCII/Mainframe Parsing/MainDetails/config/{argSplitParmFile}'
    if not os.path.isfile(splitParmFullPath):
        print(f'<WARNING> Split param not available: {splitParmFullPath}')                                   # UPR000005
        return {}, []

    with open(splitParmFullPath) as f:
        splittingParams = json.loads(f.read())

    response = {
        "Company": splittingParams["Company"],
        "FileName": splittingParams["FileName"]
    }

    if splittingParams["PrefaceInfo"]["LENGTH_CYCLEDATE"] != 8:                                              # UPR000003
        raise Exception("LENGTH_CYCLEDATE should be equal to 8: Update Splitting Parameters")                # UPR000003

    LENGTH_MRID = splittingParams["PrefaceInfo"]["LENGTH_MRID"]                                              # UPR000003
    totalMRIDLenList = []                                                                                    # UPR000003
    recTemplatesMRIDLen = {}
    for key, value in splittingParams["RecordTemplates"].items():                                            # UPR000003
        if type(value) is dict and value.get("MRID"):                                                        # UPR000003
            totalMridLength = 0                                                                              # UPR000003
            mridFields = []                                                                                  # UPR000005
            for eachEntry in value.get("MRID"):                                                              # UPR000003

                mridFieldName = eachEntry["Field"].replace('-', '_').replace(' ', '').upper()                # UPR000005
                if mridFieldName in mridFields:                                                              # UPR000005
                    errMsg = f"<ERROR>{argSplitParmFile}:{key} has SPACES in <{eachEntry['Field']} field>"   # UPR000005
                    raise Exception(errMsg)                                                                  # UPR000005
                else:                                                                                        # UPR000005
                    mridFields.append(mridFieldName)                                                         # UPR000005

                if ' ' in eachEntry["Field"]:                                                                # UPR000003
                    errMsg = f"<ERROR>{argSplitParmFile}:{key} has SPACES in <{eachEntry['Field']} field>"   # UPR000003
                    print(errMsg)                                                                            # UPR000003
                    # raise Exception(errMsg)                                                                # UPR000003

                if eachEntry['Conversion'] == 'text':                                                        # UPR000003
                    totalMridLength = totalMridLength + eachEntry['Length']                                  # UPR000003
                elif eachEntry['Conversion'] in ('scomp3', 'ucomp3', 'hex'):                                 # UPR000003
                    totalMridLength = totalMridLength + (eachEntry['Length'] * 2)                            # UPR000003
                elif eachEntry['Value']:                                                                     # UPR000003
                    totalMridLength = totalMridLength + len(str(eachEntry['Value']))                         # UPR000003
                else:                                                                                        # UPR000003
                    errMsg = f"<ERROR> {argSplitParmFile}:{key} has invalid type <{eachEntry['Conversion']}>"  # UPR000003
                    raise Exception(errMsg)
            recTemplatesMRIDLen[key] = totalMridLength
            totalMRIDLenList.append(totalMridLength)                                                         # UPR000003

    if LENGTH_MRID != max(totalMRIDLenList):                                                                 # UPR000003
        raise Exception(f"<ERROR>{argSplitParmFile}:{key} Maximum of MRID fields length:"
                        f"{totalMRIDLenList} is not matching with 'LENGTH_MRID: {LENGTH_MRID}'")             # UPR000003

    if 'MasterRecord' in recTemplatesMRIDLen and 'DefaultRecord' in recTemplatesMRIDLen:                     # UPR000004
        raise Exception(f"<ERROR>{argSplitParmFile}: MasterRecord and DefaultRecord has MRID")               # UPR000004

    # changes to handle special trailer prefix UPR000004
    specialTrailerIds = []
    specialMRID = {}
    addCommonPrefix = False
    for recTemplate in recTemplatesMRIDLen:
        templateInfo = splittingParams["RecordTemplates"][recTemplate]
        if recTemplate in ('MasterRecord', 'DefaultRecord'):
            specialTrlrID = 'Main'
            addCommonPrefix = True
        else:
            specialTrlrID = templateInfo["Type"]["Value"] \
                if templateInfo.get("Type") and templateInfo.get("Type", {}).get('Value') else templateInfo["ID"]

        specialTrailerIds.append(specialTrlrID)

        if recTemplate == 'HeaderRecord':
            recTemplatesMRIDLen[recTemplate] = LENGTH_MRID
            specialMRID[specialTrlrID] = [
                {
                    "Element": "MRID",
                    "DataType": "text",
                    "ByteLength": recTemplatesMRIDLen[recTemplate],
                    "Scale": None,
                    "Key": False
                }
            ]
        else:
            specialMRID[specialTrlrID] = []
            for fields in templateInfo["MRID"]:
                if '-' in fields["Field"]:
                    print(f"<WARNING> Splitting Params: MRID Field {fields['Field']} of {recTemplate} has hyphens")

                specialMRID[specialTrlrID].append({
                    "Element": fields["Field"]
                    if fields["Field"] == 'MRID' else 'MRID'+replaceChar+fields["Field"].replace('-', replaceChar),
                    "DataType": "text",
                    "ByteLength": fields["Length"] * 2
                    if "comp3" in fields["Conversion"] or "hex" in fields["Conversion"] else fields["Length"],
                    "Scale": None,
                    "Key": False
                })

        if LENGTH_MRID > recTemplatesMRIDLen[recTemplate]:
            specialMRID[specialTrlrID].append(
                {
                    "Element": "MRID_FILLER",
                    "DataType": "text",
                    "ByteLength": LENGTH_MRID - recTemplatesMRIDLen[recTemplate],
                    "Scale": None,
                    "Key": False
                }
            )
    if addCommonPrefix:
        specialTrailerIds.remove("Main")

    if len(specialTrailerIds) > 0:
        response["SpecialTrailerIds"] = deepcopy(specialTrailerIds)
        response['SpecialTrailerPreface'] = {}

    prefacePrefix = [
        {
            "Element": "remLen",
            "DataType": splittingParams["PrefaceInfo"]["TYPE_READREM"],
            "ByteLength": splittingParams["PrefaceInfo"]["LENGTH_READREM"],
            "Scale": None,
            "Key": False
        },
        {
            "Element": "CycleDate",
            "DataType": splittingParams["PrefaceInfo"]["TYPE_CYCLEDATE"],
            "ByteLength": splittingParams["PrefaceInfo"]["LENGTH_CYCLEDATE"],
            "Scale": None,
            "Key": False
        },
        {
            "Element": "RecType",
            "DataType": splittingParams["PrefaceInfo"]["TYPE_RECTYPE"],
            "ByteLength": splittingParams["PrefaceInfo"]["LENGTH_RECTYPE"],
            "Scale": None,
            "Key": False
        }
    ]

    prefaceSuffix = []
    if splittingParams.get("SeparateNonUniqueRecords", True) is False:
        prefaceSuffix.extend([
            {
                "Element": "MRUniqueSequence",
                "DataType": splittingParams["PrefaceInfo"]["TYPE_MRUNIQUESEQUENCE"],
                "ByteLength": splittingParams["PrefaceInfo"]["LENGTH_MRUNIQUESEQUENCE"],
                "Scale": None,
                "Key": False
            }
        ])

    prefaceSuffix.extend([
        {
            "Element": "MRRecNum",
            "DataType": splittingParams["PrefaceInfo"]["TYPE_MRRECNUMBER"],
            "ByteLength": splittingParams["PrefaceInfo"]["LENGTH_MRRECNUMBER"],
            "Scale": None,
            "Key": False
        },
        {
            "Element": "RecNum",
            "DataType": splittingParams["PrefaceInfo"]["TYPE_RECNUMBER"],
            "ByteLength": splittingParams["PrefaceInfo"]["LENGTH_RECNUMBER"],
            "Scale": None,
            "Key": False
        },
        {
            "Element": "SeqNum",
            "DataType": splittingParams["PrefaceInfo"]["TYPE_SEQNUMBER"],
            "ByteLength": splittingParams["PrefaceInfo"]["LENGTH_SEQNUMBER"],
            "Scale": None,
            "Key": False},
        {
            "Element": "StartBytePos",
            "DataType": splittingParams["PrefaceInfo"]["TYPE_STARTBYTEPOS"],
            "ByteLength": splittingParams["PrefaceInfo"]["LENGTH_STARTBYTEPOS"],
            "Scale": None,
            "Key": False
        }
        ])

    for specialTrlrID in specialTrailerIds:
        response['SpecialTrailerPreface'][specialTrlrID] = []
        response['SpecialTrailerPreface'][specialTrlrID].extend(deepcopy(prefacePrefix))
        response['SpecialTrailerPreface'][specialTrlrID].extend(deepcopy(specialMRID[specialTrlrID]))
        response['SpecialTrailerPreface'][specialTrlrID].extend(deepcopy(prefaceSuffix))

    if addCommonPrefix:
        response['RecordPreface'] = []
        response['RecordPreface'].extend(deepcopy(prefacePrefix))
        response['RecordPreface'].extend(deepcopy(specialMRID['Main']))
        response['RecordPreface'].extend(deepcopy(prefaceSuffix))

    listIDs = list(filter(lambda x: x,
                          map(lambda rt: splittingParams["RecordTemplates"][rt].get("ID")
                          if splittingParams["RecordTemplates"][rt] else None, splittingParams["RecordTemplates"].keys())
                          )
                   )
    return deepcopy(response), listIDs


def json_creator(fieldList, fileName):
    """Creates json template for fields"""

    global count, maxcount, prevFolder
    recordsList = [each for each in range(len(fieldList)) if fieldList[each][0] == 1]
    recordTypes = {}

    with open('./CopyBookfileparameter_params.json') as fparam:
        fileparams = json.load(fparam)
    cpName = fileName[:-4]
    if cpName in fileparams:
        paramtype = fileparams[cpName]
        parmFile = paramtype[0]
        s3FileFolder = paramtype[1]
        s3BaseFolder = paramtype[0].split('/', 1)[0]
        prevFolder = f"raw/{s3BaseFolder}/{s3FileFolder}/previous/{s3FileFolder}-"
        finalResp, listIDs = build_preface(parmFile)
    else:  # input type unknown
        print(f'<ERROR> Input file parameter type not recognized for {fileName}. Ignoring Prefix')
        finalResp, listIDs = {}, []
        prevFolder = ''
        parmFile = f'{cpName}.json'

    count = 0

    for i in range(len(recordsList)):
        start = recordsList[i]
        stop = len(fieldList) if i == len(recordsList) - 1 else recordsList[i + 1]
        count = 0
        maxcount = stop - start - 1
        recordTypes.update(groups_splitter(fieldList[start + 1:stop], 1)[0])

    if 'SC' in listIDs:
        recordTypes["SC"] = {
            "PreviousBucket": prevBucket,
            "PreviousFolder": prevFolder + 'SC',
            "Elements": [
                {"Element": "SCBTRIDENT", "DataType": "text", "ByteLength": 2, "Scale": None, "Key": False},
                {"Element": "UnKnown", "DataType": "null", "ByteLength": 4000, "Scale": None, "Key": False}
            ],
            "DependingOnList": []
        }
    if 'SN' in listIDs:
        recordTypes['SN'] = {
            "PreviousBucket": prevBucket,
            "PreviousFolder": prevFolder + 'SN',
            "Elements": [
                {"Element": "SNBTRIDENT", "DataType": "text", "ByteLength": 2, "Scale": None, "Key": False},
                {"Element": "UnKnown", "DataType": "null", "ByteLength": 4000, "Scale": None, "Key": False}
            ],
            "DependingOnList": []
        }
    finalResp.update({"RecordTypes": recordTypes})
    return finalResp, parmFile


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
            raise Exception(f'{eachFile}::Group::{field[1]} Has Group level Usage Clause-Check and Update')  # UPR000001

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


def checkFieldName(inDetail,fieldNames=None, occursLvl=None):                                                # UPR000001
    lvl = inDetail[0]                                                                                        # UPR000001
    if occursLvl and (lvl <= occursLvl[-1]):                                                                 # UPR000001
        occursLvl.pop()                                                                                      # UPR000001
    inkey = occursLvl[-1] if occursLvl else 'default'                                                        # UPR000001
    # print('++', inDetail, 'Key:', inkey, 'occursLvl:', occursLvl, 'len:', len(fieldNames[inkey]))          # UPR000001
    if inDetail[1] in fieldNames[inkey]:                                                                     # UPR000001
        errTrlrField = fieldNames['default'][0].replace('_', '-')                                            # UPR000001
        # print(f"{errTrlrField}::{inDetail[1].replace('_','-')} Has duplicates")                            # UPR000001
        raise Exception(f"{errTrlrField}::{inDetail[1].replace('_','-')} Has duplicates. Please check")      # UPR000001
    if inDetail[10]:                                                                                         # UPR000001
        occursLvl.append(lvl)                                                                                # UPR000001
        fieldNames[lvl] = [inDetail[1]]                                                                      # UPR000001
    else:                                                                                                    # UPR000001
        fieldNames[inkey].append(inDetail[1])                                                                # UPR000001


"""----------------------------------------------------------------------------------------------- """
"""                                             MAIN LOGIC                                         """
"""----------------------------------------------------------------------------------------------- """

file_count = 0

copyBookDirectory = 'H:/EBCIDIC2ASCII/Mainframe Parsing/MainDetails/JsonFiles/InCopyBooks'                   # UPR000002
jsonFilesDirectory = 'H:/EBCIDIC2ASCII/Mainframe Parsing/MainDetails/JsonFiles/JsonFiles'                    # UPR000002
replaceHyphenInColumn = True
numericWithoutScaleAsText = True
replaceChar = '_' if replaceHyphenInColumn else '-'
prevBucket = 'tmk-cdm-data'

for rootDir, folders, fileNames in os.walk(copyBookDirectory):
    roorDir = rootDir + ('' if rootDir[-1] == '/' else '/')                                                  # UPR000001
    for eachFile in fileNames:                                                                               # UPR000001
        file_count += 1
        print('-' * 65)
        print(f'{file_count} Processing: {rootDir}/{eachFile}')                                              # UPR000002
        prevLevel = 0
        prevFldisNotGroup = False
        prevLine = ''
        with open(rootDir + '/' + eachFile) as copyBookLayout:                                               # UPR000001
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
                            raise Exception(f'{eachFile}:: FIELDS:1: {finalLine} not in Cobol Standard')     # UPR000001

                        if (finalLine.count(' PIC ') > 0 or finalLine.count(' PICTURE ') > 0) \
                                and finalLine.strip()[0:2] == '88':
                            raise Exception(f'{eachFile}:: FIELDS:2: {finalLine} not in Cobol Standard')     # UPR000001

                        if finalLine.strip()[0:2] != '88':
                            bkpLine = finalLine.strip()
                            details = "".join(finalLine.strip().split(' VALUE ', 1)[0]).split()
                            finalLine = details[:]
                            if ('PIC' in finalLine or 'PICTURE' in finalLine) \
                                    and int(finalLine[0]) > prevLevel \
                                    and prevFldisNotGroup:
                                raise Exception(f'{eachFile}:: FIELDS:3: {prevLine} not in Cobol Standard')  # UPR000001

                            if len(finalLine) > 2 and finalLine[0].isnumeric and finalLine[2].isnumeric():
                                raise Exception(f'{eachFile}:: FIELDS:4: {bkpLine} not in Cobol Standard')   # UPR000001

                            if len(finalLine) > 2 and 'OCCURS' in finalLine[2:] and 'TIMES' not in finalLine[2:]:
                                raise Exception(f'{eachFile}:: FIELDS:5: {bkpLine} not in Cobol Standard')   # UPR000001

                            if len(finalLine) > 2 \
                                    and 'DEPENDING' in finalLine[2:] \
                                    and ('OCCURS' not in finalLine[2:] or 'ON' not in finalLine[2:]):
                                raise Exception(f'{eachFile}:: FIELDS:6: {bkpLine} not in Cobol Standard')   # UPR000001

                            if len(finalLine) > 1 and int(finalLine[0]) == 2 and finalLine[1][:4] != 'TRLR':
                                raise Exception(f'{eachFile}:: FIELDS:7: {bkpLine} '                         # UPR000001
                                                f'not in Standard Format for Processing')                    # UPR000001

                            if len(finalLine) > 2 and 'OCCURS' in finalLine[2:] and 'DEPENDINGON' in finalLine[2:]:
                                raise Exception(f'{eachFile}:: FIELDS:5: {bkpLine} not in Cobol Standard')   # UPR000004

                            if len(finalLine) > 2 \
                                    and 'OCCURS' not in finalLine[2:] \
                                    and 'REDEFINES' not in finalLine[2:] \
                                    and 'PIC' not in finalLine \
                                    and 'PICTURE' not in finalLine:
                                errMsg = f'{eachFile}:: FIELDS:8: {bkpLine} not in Cobol Standard. Check PIC clause'
                                raise Exception(errMsg)                                                      # UPR000004

                            if 'OCCURS' in finalLine[2:]:
                                tline = bkpLine.split("OCCURS", 1)[0]
                                if len(tline.strip().split()) > 2 \
                                        and 'REDEFINES' not in finalLine[2:] \
                                        and 'PIC' not in finalLine \
                                        and 'PICTURE' not in finalLine:
                                    errMsg = f'{eachFile}:: FIELDS:9: {bkpLine} not in Cobol Standard. Check PIC clause'
                                    raise Exception(errMsg)                                                 # UPR000004

                                if len(tline.strip().split()) > 4 \
                                        and 'REDEFINES' in finalLine[2:] \
                                        and 'PIC' not in finalLine \
                                        and 'PICTURE' not in finalLine:
                                    errMsg = f'{eachFile}:: FIELDS:10: {bkpLine} not in Cobol Standard. Check PIC clause'
                                    raise Exception(errMsg)                                                 # UPR000004

                            if 'REDEFINES' in finalLine[2:] \
                                    and 'OCCURS' not in finalLine[2:] \
                                    and len(finalLine) > 4 \
                                    and 'PIC' not in finalLine \
                                    and 'PICTURE' not in finalLine:
                                errMsg = f'{eachFile}:: FIELDS:11: {bkpLine} not in Cobol Standard. Check PIC clause'
                                raise Exception(errMsg)                                                      # UPR000004

                            if prevLevel == 1 and int(finalLine[0]) != 2:
                                raise Exception(f'{eachFile}:: {bkpLine}: 02 Level field is missed')         # UPR000001

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
                    print(eachFile + ':: PIC ::' + field[1] + ' In Different Format')                        # UPR000001
                    field[field.index('PICTURE')] = 'PIC'
                outDetails = details_extractor(field)
                if replaceHyphenInColumn:
                    outDetails[1] = outDetails[1].replace('-', replaceChar)
                    outDetails[11] = outDetails[11].replace('-', replaceChar)
                    outDetails[13] = outDetails[13].replace('-', replaceChar) if outDetails[13] else outDetails[13]
                layout.append(outDetails)
        loadedLayout = deepcopy(layout)                                                                       #UPR000004

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

        """ Checks field name uniqueness"""                                                                  # UPR000001
        for field in loadedLayout:                                                                           # UPR000001
            if field[12] == "REDEFINED":                                                                     # UPR000001
                continue                                                                                     # UPR000001
            if int(field[0]) == 1:                                                                           # UPR000001
                checkFieldName.__defaults__ = ({'default': []}, [])                                          # UPR000001
            checkFieldName(field)                                                                            # UPR000001

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
            layout = deepcopy(loadedLayout)                                                                   #UPR000004
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
        out_json, parmFile = json_creator(outputLayout, eachFile)                                 # UPR000001
        with open(f'{jsonFilesDirectory}/{parmFile.replace("splitting", "processing")}', 'w+') as jsonfile:
            print(jsonfile.name, "created")
            json.dump(out_json, jsonfile, indent=2)
