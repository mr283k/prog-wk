"""
Generates the CDC Parameter files
input: Processing parameter file and processed files
output: CDC Parameter files
"""

import json
import os

outDict = {
"Version": "3.38",
"KMSURL": "https://qxg7sgv2aj.execute-api.us-west-2.amazonaws.com/Dev/test-lambda?filename=",
}
splFiles = {'GLMASTER': 'CFOmaster', 'ALISMASTER': 'alis-master'}
with open('./AthenaDDL_Params.json') as aparam:
    athenaParams = json.load(aparam)

ddlEnv = athenaParams.get('ddlEnv')
ddlFor = athenaParams.get('ddlFor')
tblPrefixDict = athenaParams["tblPrefixDict"]
splDBs = athenaParams["splDBs"]
splTrlrs = athenaParams["splTrlrs"]
splFields = athenaParams["splFields"]


def cdcParamCreator(rtype, hdrElements, paramsElements, cpath, mrDtls):
    print(f'rtype:<{rtype}>, cpath:<{cpath}>, mrDtls:<{mrDtls}>')
    originl_rectype = rtype
    rtype = rtype.strip()
    cpathInfo = cpath.split('/')
    fileKey = f'{cpathInfo[1]}/{cpathInfo[2]}'
    repTrlr = splTrlrs.get(fileKey, splTrlrs['default'])

    refinedPrefix = 'refined_' if cpathInfo[0] == 'refined' else ''
    tblName = f'{tblPrefixDict[ddlFor]}_{refinedPrefix}{cpathInfo[-1].replace(rtype, repTrlr[rtype])}' \
        if rtype in repTrlr else f'{tblPrefixDict[ddlFor]}_{refinedPrefix}{cpathInfo[-1]}'
    tblName = tblName.replace('-', '_')
    dbname = splDBs.get(fileKey, '_'.join(['dl'] + cpathInfo[1:3]).replace('-', '_'))
    print(f'AthenaTable: <{dbname}.{tblName}>')

    global triggerFolder
    global castFileds
    triggerFolder[cpath] = {
        'RecordType': originl_rectype,
        'FieldDelimiter': "\t",
        "CompoundKeyList": mrDtls,
        "ExcludeColList": ["CycleDate", "RecType", "MRRecNum", "RecNum", "StartBytePos"]
    }
    subFileCount = 1
    elemFields = []
    for each in hdrElements + paramsElements:
        if type(each) is dict:
            # continue
            if each.get('SourceFields', None):
                for srcField in each['SourceFields']:
                    elemFields.append(srcField)
            elemFields.append(each)
        elif type(each) is list:
            if mrDtls[-1][:mrDtls[-1].find('_')] == 'subIndex':
                idxField = "subIndex_" + str(int(mrDtls[-1][mrDtls[-1].find('_') + 1:]) + 1)
            else:
                idxField = "subIndex_1"
            indxElement = {"Element": idxField, "DataType": "ubinary", "ByteLength": 4, "Scale": None, "Key": False}
            cdcParamCreator(originl_rectype, hdrElements + [indxElement], each[:-1],
                            cpath + '-' + str(subFileCount), mrDtls+[idxField])
            subFileCount += 1
        else:
            raise Exception('Invalid Format in Json Parameter file')

    splFile = True if fileKey in splFields else False
    typeChange = {}
    for indx, each in enumerate(elemFields, 1):
        if '-' in each['Element']:
            raise Exception(f"<ERR> {each['Element']} has HYPHENS :: Pls Check")
        if ' ' in each['Element']:
            raise Exception(f"<ERR> '{each['Element']}' Has SPACES :: Pls Check")

        if splFile and each['Element'] in splFields[fileKey]:
            print(f"<INFO> {each['Element']} is a special field in {fileKey} and {splFields[fileKey]}")
            dtype = f"varchar({splFields[fileKey][each['Element']]})"

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

        elif each['DataType'] in ['dateAIL', 'dateAIL_r', 'dateAIL_Comp3_YearMonth',
                                  'dateAIL_Text_YYMMDD', 'dateAIL_Text_MMDDYY', 'dateAIL_Text_MM/DD/YY',
                                  'dateLNL_Text_YYMMDD', 'dateLNL_Text_YY', 'dateLNL_Comp3_YearQtr',
                                  'Comp3_Date_YYYDDD1800', 'Comp3_Date_YYYMMDD', 'Comp3_Year_YYY1800']:
            dtype = 'string'

        elif each['DataType'] in ['quotedtext']:
            dtype = f"nvarchar({each['ByteLength'] + 2})"

        else:
            print(f"<WARNING> {each['Element']} has no Length Component :: Pls Check")
            dtype = 'string'
    triggerFolder[cpath]['TypeCast'] = typeChange
    triggerFolder[cpath]["AthenaTable"] = f'{dbname}.{tblName}'
    castFileds += len(typeChange)


def mainProcessor(ppath, infile):
    global triggerFolder
    with open(ppath + infile) as pf:
        params = json.load(pf)

    mridFileds = list(filter(lambda x: x, map(lambda inD: inD['Element'] if "MRID" in inD['Element'] else None,
                                              params['RecordPreface']))) + ['SeqNum']
    for recType in params["RecordTypes"]:
        currentFilePath = params["RecordTypes"][recType]["PreviousFolder"].replace('/previous/', '/current/', 1)
        if recType in params.get('SpecialTrailerPreface', []):
            splMridFlds = list(filter(lambda x: x, map(lambda inD: inD['Element'] if "MRID" in inD['Element'] else None,
                                                      params['SpecialTrailerPreface'][recType])))
            splMridFlds += ['SeqNum']
            prefaceHeader = params['SpecialTrailerPreface'][recType][1:]
            cdcParamCreator(recType, prefaceHeader, params['RecordTypes'][recType]['Elements'],
                            currentFilePath, splMridFlds)
        else:
            prefaceHeader = params['RecordPreface'][1:]
            cdcParamCreator(recType, prefaceHeader, params['RecordTypes'][recType]['Elements'],
                            currentFilePath, mridFileds)

    outDict['TriggerFolder'] = triggerFolder
    outPath = f"{manifestDirPath}ChangeDataCapture-{currentFilePath.split('/', 2)[1]}-" \
              f"{splFiles.get(params['FileName'], params['FileName'].lower())}.conf"
    print('outPath:', outPath)
    outWriter = open(outPath, 'w+')
    json.dump(outDict, outWriter, indent='\t')


if __name__ == '__main__':
    import timeit
    script_start = timeit.default_timer()

    global triggerFolder
    global castFileds

    paramDirPath = 'H:/EBCIDIC2ASCII/Mainframe Parsing/MainDetails/CDC/InParams/'
    manifestDirPath = 'H:/EBCIDIC2ASCII/Mainframe Parsing/MainDetails/CDC/CDCParams/'
    paramFilesList = list(filter(lambda f: 'json' in f.rsplit('.', 1), os.listdir(paramDirPath)))
    for f in paramFilesList:
        try:
            print('+++++++++++++++ Processing ' + f + ' ++++++++++++++++')
            triggerFolder = {}
            castFileds = 0
            mainProcessor(paramDirPath, f)
            print("Total Cast fields:", castFileds)
        except Exception as e:
            print(e)
            raise e
    script_end = timeit.default_timer()
    total_time = script_end - script_start
    mins, secs = divmod(total_time, 60)
    hours, mins = divmod(mins, 60)
    print('### Execution Time:' + str(hours) + ' Hrs ' + str(mins) + ' Mns ' + str(secs) + ' Secs')
