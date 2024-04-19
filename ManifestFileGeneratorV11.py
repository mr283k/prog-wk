"""
Generates the Manifest files
input: Processing parameter file
output: Manifest Files
========================================================================================
|Date(YYYY/MM/DD)|Change Description                                        |Change Tag |
========================================================================================
|2021/01/06      | Updated script to include logic to generate manifest     | C20210108 |
|                | files from cobol parsing params                          |           |
----------------------------------------------------------------------------------------
|2021/03/28      | Updated script to include logic to generate masked       | C20210328 |
|                | manifest files for masked trailers only                  |           |
----------------------------------------------------------------------------------------
|2022/02/09      | Removed file name check before processing the param file | C20220209 |
----------------------------------------------------------------------------------------
========================================================================================
"""

import json
import os

fileAliasNamesDict = {
    "AMF": "AGENTMASTER"
}

with open('./ManifestFiles_params.json') as mparam:
    manifestParams = json.load(mparam)

strEnv = manifestParams.pop('Environment')
manifestFor = manifestParams.pop('ManifestFor')
maskedFileDetails = manifestParams.copy()

if strEnv == 'prod':
    maskedFileDetails = {}

dataBucket = {'dev': 'tmk-cdm-data', 'test': 'tmk-cdm-test-data', 'prod': 'tmk-cdm-prd-data'}


def manifestCreator(rType, paramsElements, mrec, cpath, cname,
                    fileObj, nameSfx, removeFlag, pTrailers, mProcess):                                      # C20210328
    print('$rType:', rType)                                                                                  # C20210328
    print('$cpath:', cpath, '$cname:', cname)
    print('$fname:', fileObj.name)
    subFileCount = 1
    for each in paramsElements:
        if type(each) is dict:
            continue
        elif type(each) is list:
            subFilePath = fileObj.name.rsplit('_', nameSfx.count('_'))[0] + '_' + str(subFileCount) + nameSfx
            subRType = rType + '_' + str(subFileCount)                                                       # C20210328
            print('$subFilePath:', subFilePath)
            subFileObject = open(subFilePath, 'w+')
            splitIdx = -5 if removeFlag else -6
            subFileName = cname[:splitIdx] + f'_{subFileCount}' + cname[splitIdx:]
            manifestCreator(subRType, each[:-1], mrec, cpath + '-' + str(subFileCount), subFileName,
                            subFileObject, nameSfx, removeFlag, pTrailers, mProcess)                         # C20210328
            subFileCount += 1
        else:
            raise Exception('Invalid Format in Json Parameter file')
    if pTrailers == 'ALL' or rType in pTrailers or rType.replace('_', '-') in pTrailers:                     # C20210328
        fileObj.write(mrec.replace('@#!#@', cpath + cname, 1))                                               # C20210328
        fileObj.close()                                                                                      # C20210328
    else:                                                                                                    # C20210328
        fileObj.close()                                                                                      # C20210328
        print(f"<WARNING> Manifest file not required for '{rType}' & MaskedProcess: {mProcess}")             # C20210328
        os.remove(fileObj.name)                                                                              # C20210328


def processingParams(ppath, infile, maskedFile=False):
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

    with open(ppath + infile) as pf:
        params = json.load(pf)

    if manifestFor == 'delta':
        nameSuffix = '_CDC_GLBL.manifest'
    else:
        nameSuffix = '_GLBL.manifest'

    for recType in params["RecordTypes"]:
        cdmDataBucket = dataBucket[strEnv]
        currentFilePath = params["RecordTypes"][recType]["PreviousFolder"].replace('/previous/', f'/{manifestFor}/', 1)
        newType = recType.strip()
        if recType != newType:
            print(f'<UPD> Rectype Changed from "{recType}" to "{newType}"')
        currentFileName = '/' + params["Company"] + '_' + params["FileName"] + '_' + newType + '_*.tab'
        outManifestFile = params["Company"] + '_' + \
                          fileAliasNamesDict.get(params["FileName"], params["FileName"]) + '_' + newType + nameSuffix
        removeUnderscore = False
        tgtFolder = maskedFileDetails[infile]["UnmaskedTargetFolder"] \
            if infile in maskedFileDetails else currentFilePath.split('/')[2]                                # C20210328
        trailersToProcess = 'ALL'                                                                            # C20210328

        if manifestFor != 'current':
            tgtFolder = f'{tgtFolder}-{manifestFor}'
        if maskedFile:
            currentFilePath = currentFilePath.replace(maskedFileDetails[infile]["OriginalS3Folder"],
                                                      maskedFileDetails[infile]["MaskedS3Folder"], 1)        # C20210328
            tgtFolder = maskedFileDetails[infile]["MaskedTargetFolder"]                                      # C20210328
            if maskedFileDetails[infile]["BoolToRemove_"]:                                                   # C20210328
                currentFileName = currentFileName.replace('_*', '*', 1)
                removeUnderscore = True
            trailersToProcess = maskedFileDetails[infile].get("MaskedTrailers", 'ALL')                       # C20210328

        outDirectory = manifestDirPath + tgtFolder
        if not os.path.exists(outDirectory):
            os.mkdir(outDirectory)
        outPath = outDirectory + '/' + outManifestFile
        manifestWriter = open(outPath, 'w+')
        manifestRec = manifestRec.replace('$%^%$', cdmDataBucket)
        manifestCreator(newType, params['RecordTypes'][recType]['Elements'], manifestRec,
                        currentFilePath, currentFileName, manifestWriter, nameSuffix,
                        removeUnderscore, trailersToProcess, maskedFile)                                     # C20210328


if __name__ == '__main__':
    import timeit
    script_start = timeit.default_timer()

    paramDirPath = 'H:/EBCIDIC2ASCII/Mainframe Parsing/MainDetails/Manifest/InParams/'
    manifestDirPath = 'H:/EBCIDIC2ASCII/Mainframe Parsing/MainDetails/Manifest/ManifestFiles/'
    listParamsCheck = ['processing_params', 'cobol_parsing_params']                                          # C20210108
    # paramsList = list(filter(lambda inF: '.json' in inF and inF.rsplit('_', 1)[0] in listParamsCheck,      # C20210108
    #                          os.listdir(paramDirPath)))                                                    # C20210108
    paramsList = list(filter(lambda inF: '.json' in inF, os.listdir(paramDirPath)))                          # C20220209

    for f in paramsList:
        try:
            print('+++++++++++++++ Processing ' + f + ' ++++++++++++++++')
            processingParams(paramDirPath, f)
            if manifestFor == 'current' and f in maskedFileDetails:
                print('============= Processing for Masked file ==============')
                processingParams(paramDirPath, f, maskedFile=True)

        except Exception as e:
            print(e)
            raise e
    script_end = timeit.default_timer()
    total_time = script_end - script_start
    mins, secs = divmod(total_time, 60)
    hours, mins = divmod(mins, 60)
    print('### Execution Time:' + str(hours) + ' Hrs ' + str(mins) + ' Mns ' + str(secs) + ' Secs')
