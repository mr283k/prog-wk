import os
import json
parmDir = '//MCKUserData2/users3/VENRXTHALAMANCHI/Citrix/Desktop/Check Details/config'
commonParam = f'{parmDir}/common/DataLake_Mainframe_Params.json'


with open(commonParam, 'r') as parmObj:
    commonParams = json.load(parmObj)

fileCount = 0
finalList = list()
splParamDict = dict()
splFiles = []
for rootDir, folders, files in os.walk(parmDir):
    splitFiles = filter(lambda x: 'splitting_params_' in x, files)
    for inFile in splitFiles:
        fileCount += 1
        with open(f'{rootDir}/{inFile}', 'r') as parmObj:
            splitParams = json.load(parmObj)
        if ('MaxRecCountForSeparation' in splitParams) or ('TrailerSeparationRecCount' in splitParams):
            with open(f'{rootDir}/{inFile.replace("splitting_params_", "Processing_params_")}', 'r') as parmObj:
                processParams = json.load(parmObj)
            splParamDict[inFile] = \
                processParams['RecordTypes'][list(processParams['RecordTypes'].keys())[0]]['PreviousFolder']
        for trlrID in splitParams.get('TrailerSeparationRecCount', []):
            finalList.append('\t'.join([splitParams['Company'], splitParams['FileName'], trlrID]))

for inFile, paramFile in commonParams.items():
    splitFileName = paramFile.rsplit('/', 1)[-1]
    if splitFileName in splParamDict:
        s3Info = splParamDict[splitFileName].split("/")
        splFiles.append(f'{inFile}\t{s3Info[1]}\t{s3Info[2]}')
if len(splParamDict) != len(splFiles):
    print(splParamDict)
    print(splFiles)
    print('<WARNING> Not matching')
print('============= Special File Details =================')
for inFile in splFiles:
    print(inFile)

print('============= Long Running Trailers Details =================')
for finalRec in finalList:
    print(finalRec)
