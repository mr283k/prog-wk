import os
import json

inputDir = 'H:/EBCIDIC2ASCII/Mainframe Parsing/MainDetails/EOD/Input'
outputDir = 'H:/EBCIDIC2ASCII/Mainframe Parsing/MainDetails/EOD/output'

reProcessTrailers = ['GL_GLMASTER_13']

countDict = {}
totalCount = {}
outputDict = {}


def getOutputFile(argCompany, argFileName, argTID='', argKey=''):
    key = f'{argKey}{argCompany}{argFileName}{argTID}'
    if key not in outputDict:
        # print("print", key)
        outputDict[key] = open(f'{outputDir}/{argCompany}_{argFileName}{f"_{argTID}" if argTID else ""}_{argKey}.adhoc', 'w+')
    return outputDict[key]


for rootDir, subDir, inFiles in os.walk(inputDir+'/'):
    rootDir = rootDir if rootDir[-1] == '/' else f'{rootDir}/'
    for each in inFiles:
        print(f'Processing: {rootDir}{each}')
        cnt = 0
        header = []
        prevTabFile = ''
        prevTID = ''
        prevCompany = ''
        prevFileName = ''
        boolWrite = False
        tgtFileKey = each.rsplit('.', 1)[0].split('_', 1)[1]
        resEODFiles = {tgtFileKey: []}
        with open(f'{rootDir}{each}') as f:
            while True:
                inRec = f.readline().strip('\n')
                if inRec == '':
                    break
                cnt += 1
                if cnt == 1:
                    header = inRec.split('\t')
                    continue
                recInfo = dict(zip(header, inRec.split('\t')))

                if prevTID != recInfo['Trailer_ID'] and resEODFiles[tgtFileKey]:
                    getOutputFile(prevCompany, prevFileName, argTID=prevTID, argKey='EODFiles')\
                        .write(json.dumps(resEODFiles, indent=2))
                    resEODFiles = {tgtFileKey: []}

                # Company FileName TabFile Trailer_ID ChunkFlag ChunkIndex
                # FileSize TotalSize SizeDiff(%) PartialLoad IncludeFlag DuplicateFlag
                company = recInfo['Company']
                fileName = recInfo['FileName']
                currTID = recInfo['Trailer_ID']
                if recInfo['DuplicateFlag'].upper() == 'TRUE':
                    countKey = f'Duplicate_{company}_{fileName}'
                    countDict[countKey] = countDict.get(countKey, 0) + 1
                    continue

                if recInfo['PartialLoad'].upper() == 'TRUE':
                    countKey = f'Partial_{company}_{fileName}'
                    countDict[countKey] = countDict.get(countKey, 0) + 1
                    getOutputFile(company, fileName, argKey='PartialFiles')\
                        .write(recInfo['TabFile']+'\n')
                    continue

                if f'{company}_{fileName}_{currTID}' in reProcessTrailers:
                    getOutputFile(company, fileName, argTID=currTID, argKey='ProcessFiles') \
                        .write(recInfo['TabFile'].rsplit('.', 1)[0] + '.bin\n')

                countKey = f'EOD_{company}_{fileName}_{currTID}'
                countDict[countKey] = countDict.get(countKey, 0) + 1

                currFile = recInfo['TabFile'].rsplit('.', 1)[0]
                if recInfo['ChunkFlag'].upper() == 'Y':
                    currFile = currFile.rsplit('-', 1)[0]
                    if currFile == prevTabFile:
                        resEODFiles[fileName][-1].append(recInfo['TabFile'])
                    else:
                        resEODFiles[fileName].append([recInfo['TabFile']])
                else:
                    resEODFiles[fileName].append(recInfo['TabFile'])

                prevTabFile = currFile
                prevCompany = company
                prevFileName = fileName
                prevTID = currTID

        if resEODFiles[tgtFileKey]:
            getOutputFile(prevCompany, prevFileName, argTID=prevTID, argKey='EODFiles') \
                .write(json.dumps(resEODFiles, indent=2))
        totalCount[tgtFileKey] = cnt-1


for each in outputDict:
    outputDict[each].close()

print("================= Segregated Count Info ================")
print(json.dumps({x: countDict[x] for x in sorted(countDict)}, indent=2))

print("===================== Total Count Info =================")
print(json.dumps(totalCount, indent=2))

print("======================= Total Check ===================")
print("Total     Count:", sum(totalCount.values()))
print("Segrgated Total:", sum(countDict.values()))