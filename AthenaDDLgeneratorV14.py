"""
Generates the DDL
input: Processing parameter file
output: DDL statement
=========================================================================================
|Date(YYYY/MM/DD)      |Change Description                                    |Change Tag|
=========================================================================================
|2021/01/08            |Updated the script to handle cobol parsing params     |C20210108 |
|                      |Parameterized hard coded values                       |          |
-----------------------------------------------------------------------------------------
|2021/03/05            |Updated script to handle refined process parameters   |C20210305 |
-----------------------------------------------------------------------------------------
|2021/09/14            |Updated script to handle spaces in column names       |C20210914 |
-----------------------------------------------------------------------------------------
|2021/10/29            |Updated script for     parquet tables DDL creation    |C20211029 |
-----------------------------------------------------------------------------------------
|2022/07/04            |Updated script for to include new data type           |C20220704 |
=========================================================================================
"""

import json

ISLOCAL = False
s3fsys = None

with open ('./AthenaDDL_Params.json') as aparam:                                                             # C20210108
    athenaParams = json.load(aparam)                                                                         # C20210108

ddlEnv = athenaParams.get('ddlEnv')                                                                          # C20210108
ddlFor = athenaParams.get('ddlFor')                                                                          # C20210108
tblPrefixDict = athenaParams["tblPrefixDict"]                                                                # C20210108
dataBucket = athenaParams["dataBucket"]                                                                      # C20210108
maskedFiles = athenaParams["maskedFiles"]                                                                    # C20210108
splDBs = athenaParams["splDBs"]                                                                              # C20210108
splTrlrs = athenaParams["splTrlrs"]                                                                          # C20210108
parquetDDLs = athenaParams["parquetDDLs"]                                                                    # C20211029
# splfields whose length should be adjusted as those fields length depends on other fields
splFields = athenaParams["splFields"]                                                                        # C20210108

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

# parquet table properties added for request <C20211029>
parquettblproperties = """PARTITIONED BY({2} {3}) 
STORED AS PARQUET
LOCATION 's3://{0}/{1}/'
tblproperties ("parquet.compress"="SNAPPY");
"""


def ddlCreator(mRecType, paramsElements, prefaceHdr, fileObj, s3Details, maskFlag, cobolFlag):               # C20210108
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
            if cobolFlag:                                                                                    # C20210108
                raise Exception(f'<ERROR> {mRecType} has "OCCURS". Pls Check')                               # C20210108
            s3SubDetails = [s3Details[0], s3Details[1] + '-' + str(subFileCount)]

            if prefaceHdr[-1]["Element"][:prefaceHdr[-1]["Element"].find('_')] == 'subIndex':
                idxField = "subIndex_" + str(
                    int(prefaceHdr[-1]["Element"][prefaceHdr[-1]["Element"].find('_') + 1:]) + 1)
            else:
                idxField = "subIndex_1"
            indxElement = {"Element": idxField, "DataType": "ubinary", "ByteLength": 4, "Scale": None, "Key": False}
            ddlCreator(mRecType, each[:-1], prefaceHdr + [indxElement], fileObj, s3SubDetails, maskFlag,
                       cobolFlag)                                                                            # C20210108
            subFileCount += 1
        else:
            raise Exception('Invalid Format in Json Parameter file')

    s3PathDetails = s3Details[1].split('/')
    fileKey = f"{s3PathDetails[1]}/{s3PathDetails[2].rsplit('-masked', 1)[0] if maskFlag else s3PathDetails[2]}"
    splFile = True if fileKey in splFields else False
    dbName = splDBs.get(fileKey, '_'.join(['dl'] + s3PathDetails[1:3]).replace('-', '_'))
    repTrlr = splTrlrs.get(fileKey, splTrlrs['default'])

    refinedPrefix = 'refined_' if s3PathDetails[0] == 'refined' else ''                                      # C20210305
    # Added refined in table name for refined process files tagID: C20210305
    tblName = f'{tblPrefixDict[ddlFor]}_{refinedPrefix}{s3PathDetails[-1].replace(mRecType, repTrlr[mRecType])}' \
        if mRecType in repTrlr else f'{tblPrefixDict[ddlFor]}_{refinedPrefix}{s3PathDetails[-1]}'
    tblName = tblName.replace('-', '_')

    if createDB:
        createDB = False
        fileObj.write(f'CREATE DATABASE IF NOT EXISTS {dbName};\n\n')

    fileObj.write(f"CREATE EXTERNAL TABLE {dbName}.{tblName}" + "\n" + '(' + '\n')
    totalColumns = len(elemFields)
    for indx, each in enumerate(elemFields, 1):
        if '-' in each['Element']:
            raise Exception(f"<ERR> {each['Element']} has HYPHENS :: Pls Check")
        if ' ' in each['Element']:                                                                           # C20210914
            raise Exception(f"<ERR> '{each['Element']}' Has SPACES :: Pls Check")                            # C20210914

        if splFile and each['Element'] in splFields[fileKey]:
            print(f"<INFO> {each['Element']} is a special field in {fileKey} and {splFields[fileKey]}")
            dtype = f"varchar({splFields[fileKey][each['Element']]})"

        elif each['DataType'] in ['text', 'null', 'specialtext']:                                            # C20220704
            dtype = f"varchar({each['ByteLength']})"

        elif each['DataType'] in ['numeric', 'stnumeric', 'slnumeric']:
            if each['ByteLength'] >= 18:
                print(f"<WARNING> {each['Element']}:{each['DataType']} length is {each['ByteLength']}")
            dtype = f"decimal({each['ByteLength']},{each['Scale']})" if each['Scale'] else 'bigint'
            # dtype = 'string' if ddlFor in parquetDDLs else dtype                                           # C20211029

        elif each['DataType'] in ['scomp3', 'ucomp3']:
            if each['ByteLength'] >= 9:
                print(f"<WARNING> {each['Element']}:{each['DataType']} length is {each['ByteLength']}")
            dtype = f"decimal({each['ByteLength'] * 2},{each['Scale']})" if each['Scale'] else 'bigint'
            # dtype = 'string' if ddlFor in parquetDDLs else dtype                                           # C20211029

        elif each['DataType'] in ['hex']:
            dtype = f"varchar({each['ByteLength'] * 2})"

        elif each['DataType'] in ['sbinary', 'ubinary']:
            if each['ByteLength'] >= 8:
                print(f"<INFO> {each['Element']}:{each['DataType']} length is {each['ByteLength']}")
                dtype = f"decimal({len(str(2**(each['ByteLength']*8)))})"
            else:
                dtype = 'bigint'
            # dtype = 'string' if ddlFor in parquetDDLs else dtype                                           # C20211029

        elif each['DataType'] in ['dateAIL', 'dateAIL_r', 'dateAIL_Comp3_YearMonth',
                                  'dateAIL_Text_YYMMDD', 'dateAIL_Text_MMDDYY', 'dateAIL_Text_MM/DD/YY',
                                  'dateLNL_Text_YYMMDD', 'dateLNL_Text_YY', 'dateLNL_Comp3_YearQtr',
                                  'Comp3_Date_YYYDDD1800', 'Comp3_Date_YYYMMDD', 'Comp3_Year_YYY1800']:
            dtype = 'string'

        elif each['DataType'] in ['quotedtext']:
            dtype = f"nvarchar({each['ByteLength'] + 2})"

        else:
            print(f"<WARNING> {each['Element']} Data type not included :: Pls Check")                        # C20220704
            dtype = 'string'

        if ddlFor in parquetDDLs and each['Element'].upper() == "CYCLEDATE":                                 # C20211029
            parquetCol = each['Element']                                                                     # C20211029
            parquetDtype = dtype                                                                             # C20211029
            continue                                                                                         # C20211029

        if indx < totalColumns:
            fileObj.write(f"{each['Element']}   {dtype}," + '\n')
        else:
            fileObj.write(f"{each['Element']}   {dtype}")
    if ddlFor == 'delta':
        fileObj.write(',\ndatalake_operation  string,\n')
        fileObj.write('datalake_timestamp  string')
    fileObj.write('\n)\n')
    # Added logic to parquet file properties addition  for change <C20211029>
    if ddlFor in parquetDDLs:
        fileObj.write(parquettblproperties.format(s3Details[0], s3Details[1], parquetCol, parquetDtype) + '\n\n')
    else:
        fileObj.write(tblProperties.format(s3Details[0], s3Details[1]) + '\n\n')


def processingParams(ppath, infile, masked=False):                                                           # C20220209
    with open(ppath + infile) as pf:
        params = json.load(pf)
    inFileSplit = infile.rsplit('_', 1)
    cobolfile = inFileSplit[0] == 'cobol_parsing_params'                                                     # C20220209
    parmFileName = params["FileName"] + ( '_MASKED' if masked else '')
    outFileName = f'{ddlEnv.upper()}_{params["Company"]}_{parmFileName}_{ddlFor.replace("/", "_").upper()}.txt'
    ddlWriter = open(ddlDirPath + outFileName, 'w+')

    for recType in params["RecordTypes"]:
        if cobolfile:                                                                                        # C20210108
            prefaceHeader = []                                                                               # C20210108
        elif infile == 'internal_processing_params_DELSTAT.json':                                            # C20220209
            prefaceHeader = []                                                                               # C20220209
        else:                                                                                                # C20210108
            prefaceHeader = params['SpecialTrailerPreface'][recType][1:]\
                if recType in params.get("SpecialTrailerIds", []) else params['RecordPreface'][1:]

        dataBucketFolder = params['RecordTypes'][recType]['PreviousFolder']
        if masked:
            dataBucketFolder = dataBucketFolder.replace(maskedFiles[infile][0], maskedFiles[infile][1], 1)

        dataBucketFolder = dataBucketFolder.replace('/previous/', f'/{ddlFor}/')
        dataBucketDetails = [dataBucket[ddlEnv], dataBucketFolder]
        ddlCreator(recType.strip(), params['RecordTypes'][recType]['Elements'],
                   prefaceHeader, ddlWriter, dataBucketDetails, masked, cobolfile)                           # C20210108


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
    import os

    listParamsCheck = ['processing_params', 'cobol_parsing_params']                                          # C20210108
    for rootDir, folders, fileNames in os.walk(paramDirPath):
        roorDir = rootDir + ('' if rootDir[-1] == '/' else '/')
        for eachFile in fileNames:
            listFileInfo = eachFile.rsplit('_', 1)                                                           # C20210108
            if not ('.json' in eachFile and listFileInfo[0] in listParamsCheck):                             # C20210108
                if eachFile != 'internal_processing_params_DELSTAT.json':                                    # C20220209
                    continue

            createDB = True
            try:
                print('+++++++++++++++ Processing: ' + eachFile + ' ++++++++++++++++')
                processingParams(paramDirPath, eachFile)                                                    # C20220209
                if eachFile in maskedFiles and ddlEnv in ('dev', 'test'):
                    print('+++++++++++++++ Masking: ' + eachFile + ' ++++++++++++++++')
                    createDB = True
                    processingParams(paramDirPath, eachFile,masked=True)
            except Exception as e:
                raise e

    script_end = timeit.default_timer()
    total_time = script_end - script_start
    mins, secs = divmod(total_time, 60)
    hours, mins = divmod(mins, 60)
    print('### Execution Time:' + str(hours) + ' Hrs ' + str(mins) + ' Mns ' + str(secs) + ' Secs')


