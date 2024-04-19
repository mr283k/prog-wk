"""
Extracts db and corresponding table names
input: Processing parameter file
output: file with DB and table names
"""

# No need to change Athena DDL Parameters

import json

ISLOCAL = False
s3fsys = None

with open ('./AthenaDDL_Params.json') as aparam:                                                             # C20210108
    athenaParams = json.load(aparam)                                                                         # C20210108

ddlEnv = athenaParams.get('ddlEnv')                                                                          # C20210108
ddlFor = athenaParams.get('ddlFor')                                                                          # C20210108
tblPrefixDict = athenaParams["tblPrefixDict"]                                                                # C20210108
dataBucket = athenaParams["dataBucket"]                                                                      # C20210108
splDBs = athenaParams["splDBs"]                                                                              # C20210108
splTrlrs = athenaParams["splTrlrs"]                                                                          # C20210108
parquetDDLs = athenaParams["parquetDDLs"]                                                                    # C20211029
# splfields whose length should be adjusted as those fields length depends on other fields
splFields = athenaParams["splFields"]                                                                        # C20210108


def ddlCreator(mRecType, paramsElements, prefaceHdr, fileObj, s3Details):
    global createDB
    elemFields = list()
    subFileCount = 1
    for each in prefaceHdr+paramsElements:
        if type(each) is dict:
            if each.get('SourceFields', None):
                for srcField in each['SourceFields']:
                    elemFields.append(srcField)
            elemFields.append(each)
        elif type(each) is list:

            s3SubDetails = [s3Details[0], s3Details[1] + '-' + str(subFileCount)]

            if prefaceHdr[-1]["Element"][:prefaceHdr[-1]["Element"].find('_')] == 'subIndex':
                idxField = "subIndex_" + str(
                    int(prefaceHdr[-1]["Element"][prefaceHdr[-1]["Element"].find('_') + 1:]) + 1)
            else:
                idxField = "subIndex_1"
            indxElement = {"Element": idxField, "DataType": "ubinary", "ByteLength": 4, "Scale": None, "Key": False}
            ddlCreator(mRecType, each[:-1], prefaceHdr + [indxElement], fileObj, s3SubDetails)
            subFileCount += 1
        else:
            raise Exception('Invalid Format in Json Parameter file')

    s3PathDetails = s3Details[1].split('/')
    fileKey = f"{s3PathDetails[1]}/{s3PathDetails[2]}"
    splFile = True if fileKey in splFields else False
    dbName = splDBs.get(fileKey, '_'.join(['dl'] + s3PathDetails[1:3]).replace('-', '_'))
    repTrlr = splTrlrs.get(fileKey, splTrlrs['default'])

    refinedPrefix = 'refined_' if s3PathDetails[0] == 'refined' else ''                                      # C20210305
    # Added refined in table name for refined process files tagID: C20210305
    tblName = f'{tblPrefixDict["current"]}_{refinedPrefix}{s3PathDetails[-1].replace(mRecType, repTrlr[mRecType])}' \
        if mRecType in repTrlr else f'{tblPrefixDict["current"]}_{refinedPrefix}{s3PathDetails[-1]}'
    curr_tblName = tblName.replace('-', '_')

    tblName = f'{tblPrefixDict["previous"]}_{refinedPrefix}{s3PathDetails[-1].replace(mRecType, repTrlr[mRecType])}' \
        if mRecType in repTrlr else f'{tblPrefixDict["previous"]}_{refinedPrefix}{s3PathDetails[-1]}'
    prev_tblName = tblName.replace('-', '_')
    fileObj.write(f'{dbName}.{curr_tblName}\t{dbName}.{prev_tblName}\n')


def mainProcess(ppath, infile, outFileObj):
    with open(ppath + infile) as pf:
        params = json.load(pf)

    for recType in params["RecordTypes"]:
        prefaceHeader = params['SpecialTrailerPreface'][recType][1:] \
            if recType in params.get("SpecialTrailerIds", []) else params['RecordPreface'][1:]

        dataBucketFolder = params['RecordTypes'][recType]['PreviousFolder']
        dataBucketFolder = dataBucketFolder.replace('/previous/', f'/{ddlFor}/')
        dataBucketDetails = [dataBucket[ddlEnv], dataBucketFolder]
        ddlCreator(recType.strip(), params['RecordTypes'][recType]['Elements'],
                   prefaceHeader, outFileObj, dataBucketDetails)


if __name__ == '__main__':
    import io
    import timeit
    script_start = timeit.default_timer()

    ISLOCAL = True
    s3fsys = io

    if ddlFor not in tblPrefixDict:
        raise Exception(f'<ERR> Invalid DDL For Entry: {ddlFor}')

    paramDirPath = 'H:/EBCIDIC2ASCII/Mainframe Parsing/MainDetails/DDL/InParams/'
    ddlDirPath = 'H:/EBCIDIC2ASCII/Mainframe Parsing/MainDetails/DDL/DDL/'

    outFile = open(ddlDirPath + 'db_table_info.txt', 'w+')
    import os

    listParamsCheck = ['processing_params']
    noNeed = ('processing_params_ADPPREMIUM.json',
              'processing_params_UACFUCFOPLANCODES.json',
              'processing_params_DAILYADPPREMIUM.json')
    totalFiles = 0
    processFiles = 0
    for rootDir, folders, fileNames in os.walk(paramDirPath):
        roorDir = rootDir + ('' if rootDir[-1] == '/' else '/')
        for eachFile in fileNames:
            totalFiles += 1
            listFileInfo = eachFile.rsplit('_', 1)
            if listFileInfo[0] not in listParamsCheck or eachFile in noNeed:
                continue
            processFiles += 1
            createDB = True
            try:
                # print('+++++++++++++++ Processing: ' + eachFile + ' ++++++++++++++++')
                mainProcess(roorDir, eachFile, outFile)
            except Exception as e:
                raise e
    print(f'Total: {totalFiles}, Processed: {processFiles}')

    script_end = timeit.default_timer()
    total_time = script_end - script_start
    mins, secs = divmod(total_time, 60)
    hours, mins = divmod(mins, 60)
    print('### Execution Time:' + str(hours) + ' Hrs ' + str(mins) + ' Mns ' + str(secs) + ' Secs')
