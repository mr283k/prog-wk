""""
Parameter file generation without Redefines
Occurs: Yes
Depending On : yes
Redefines : No
Changes done : Made the MRID with individual fields instead of combined ID
"""
import json
import re
import os


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
            if 'COMPUTATIONAL' in inputField[inputField.index('PIC') + 1:] or 'COMP' in inputField[
                                                                                        inputField.index('PIC') + 1:]:
                fieldPictureType = 'B'
                if 1 <= fieldDisplayLength <= 4:
                    fieldStorageLen = 2
                elif 5 <= fieldDisplayLength <= 9:
                    fieldStorageLen = 4
                else:
                    fieldStorageLen = 8
                if signFlag == 'Y':
                    fieldSignStat = 'SB'
            elif 'COMPUTATIONAL-3' in inputField[inputField.index('PIC') + 1:] or 'COMP-3' in inputField[
                                                                                              inputField.index(
                                                                                                      'PIC') + 1:]:
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
            raise Exception(eachFile.name + ':: Group ::' + field[1] + ' Has Group level Usage Clause-Check and Update')

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


"""----------------------------------------------------------------------------------------------- """
"""                                             MAIN LOGIC                                         """
"""----------------------------------------------------------------------------------------------- """

file_count = 0

copyBookDirectory = 'H:/EBCIDIC2ASCII/Mainframe Parsing/MainDetails/Upd_CopyBooks'
jsonFilesDirectory = 'H:/EBCIDIC2ASCII/Mainframe Parsing/MainDetails/JsonFiles'

for rootDir, folders, fileNames in os.walk(copyBookDirectory):
    if folders:
        continue

    for eachFile in os.scandir(rootDir + '\.'):
        file_count += 1
        # print('-' * 65)
        print(f'{file_count} Processing:{eachFile.name}')
        prevLevel = 0
        prevFldisNotGroup = False
        prevLine = ''
        with open(rootDir + '/' + eachFile.name) as copyBookLayout:
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
                            raise Exception(eachFile.name + ':: FIELDS :1: ' + finalLine + ' not in Cobol Standard')

                        if finalLine.count(' PIC ') > 0 and finalLine.strip()[0:2] == '88':
                            raise Exception(eachFile.name + ':: FIELDS :2: ' + finalLine + ' not in Cobol Standard')

                        if finalLine.strip()[0:2] != '88':
                            bkpLine = finalLine.strip()
                            details = "".join(finalLine.strip().split(' VALUE ', 1)[0]).split()
                            finalLine = details[:]
                            if 'PIC' in finalLine and int(finalLine[0]) > prevLevel and prevFldisNotGroup:
                                raise Exception(eachFile.name + ':: FIELDS :3: ' + prevLine + ' not in Cobol Standard')

                            if len(finalLine) > 2 and finalLine[0].isnumeric and finalLine[2].isnumeric():
                                raise Exception(eachFile.name + ':: FIELDS :4: ' + bkpLine + ' not in Cobol Standard')

                            if len(finalLine) > 2 and 'OCCURS' in finalLine[2:] and 'TIMES' not in finalLine[2:]:
                                raise Exception(eachFile.name + ':: FIELDS :5: ' + bkpLine + ' not in Cobol Standard')

                            if len(finalLine) > 2 and 'DEPENDING' in finalLine[2:] and 'OCCURS' not in finalLine[2:]:
                                raise Exception(eachFile.name + ':: FIELDS :6: ' + bkpLine + ' not in Cobol Standard')

                            if len(finalLine) > 1 and int(finalLine[0]) == 2 and finalLine[1][:4] != 'TRLR':
                                raise Exception(
                                    eachFile.name + ':: FIELDS :4: ' + bkpLine + ' not in Standard Format for Processing')

                            if prevLevel == 1 and int(finalLine[0]) != 2:
                                raise Exception(
                                    eachFile.name + ':: 02 Level field is missed to identify trailer type: ' + bkpLine)

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
                    print(eachFile.name + ':: PIC ::' + field[1] + ' In Different Format')
                    field[field.index('PICTURE')] = 'PIC'
                outDetails = details_extractor(field)
                layout.append(outDetails)
        loadedLayout = layout.copy()
        del layout

        """
        (level,name,pos,storage-len,display-len,scale,picture,sign stat,redefined,redefines,occurs,redefined var,
                                                                                                Redefined, DependingOn)
        (0    ,1   ,2  ,3          ,4          ,5    ,6      ,7        ,8        ,9        ,10  ,11    ,12      ,13)
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

        layout = []
        for eachField in loadedLayout:
            layout.append(eachField)
            layout[-1].append(eachField[1].replace('-', '_'))
        loadedLayout = layout.copy()
        del layout

        # for kkk in loadedLayout:
        #     print(kkk)

        with open('./CopyBookfileparameter_params.json') as fparam:
            fileparams = json.load(fparam)
        cpName = eachFile.name[:-4]
        if cpName in fileparams:
            paramtype = fileparams[cpName]
        else:  # input type unknown
            raise Exception('Input file parameter type not recognized; aborting execution:', cpName)

        jsonFilePath = f"{jsonFilesDirectory}/{paramtype[0].replace('splitting', 'processing')}"

        jsonTxt = ''
        with open(jsonFilePath) as jparam:
            for inRec in jparam:
                if re.search(r'(\ *"Element"\ *:\ *")(.*)(",)$', inRec):
                    jsonTxt += inRec.replace('-', '_')
                else:
                    jsonTxt += inRec

        for eachField in loadedLayout:
            if eachField[6] == 'G' or eachField[12] == 'REDEFINED':
                continue
            jsonTxt = jsonTxt.replace(f'"{eachField[1]}"', f'"{eachField[14]}"')
            jsonTxt = jsonTxt.replace(f"'{eachField[1]}'", f"'{eachField[14]}'")

        jsonFilePath = f"{jsonFilesDirectory}_Upd/{paramtype[0].replace('splitting', 'processing')}"
        with open(jsonFilePath, 'w+') as jparam:
            jparam.write(jsonTxt)
