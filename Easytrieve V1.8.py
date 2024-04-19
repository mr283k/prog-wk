import json
import logging
from datetime import datetime
from timeit import default_timer
from functools import reduce

global loopPosList
global procPosDict
global procCallDict
procPosDict ={}
procCallDict = dict()

def lineFormatter(inLine, sep):
    tline = list()
    sepCnt = inLine.count(sep)
    for cnt in range(sepCnt):
        pos = inLine.index(sep)
        tr = inLine[:pos]
        if cnt % 2 == 0:
            tline.append('|'.join(tr.split()))
        else:
            tline.append("'" + inLine[:pos + 1])
        inLine = inLine[pos + 1:]
    if inLine:
        tline.append('|'.join(inLine.split()))
    return '|'.join(tline)


def varCheck(infield):
    if infield[0] in ["'", '+', '-'] or infield[0].isnumeric():
        return False
    else:
        return True


def logicExtractor(inField, srcCode):
    logicLines = list()
    for srcLine in srcCode:
        if inField == '@RecordFilterLogic@':
            if '|GO|TO|JOB' in srcLine:
                logicLines.append(int(srcLine.split('|', 1)[0]))
            continue
        tline = srcLine.split('|')
        if 'CALL' in tline and 'USING' in tline and (inField in tline or inField+',' in tline):
            logicLines.append(int(srcLine.split('|', 1)[0]))
        elif inField in tline and 'IF' not in tline and 'DO' not in tline and (
                '|' + inField + '|=' in srcLine or '|TO|' + inField in srcLine):
            logicLines.append(int(srcLine.split('|', 1)[0]))
            tField = tline[3] if '=' in tline else tline[2]
            if varCheck(tField):
                tlines = logicExtractor(tField, srcCode)
                logicLines += tlines
        #else:
        #    print('Not Considered Lines:', srcLine)
    #print('%% logicLines:', logicLines)
    return logicLines

def detailsExtraction(lineNos, loopList):
    outLines = []
    for lineNo in lineNos:
        outLines.append(lineNo)
        linesWithLP = filter(lambda inL:inL,
                             map(lambda inLP: inLP if inLP[0] <= lineNo <= inLP[-1] else None, loopList))
        outLines += [inL for subL in linesWithLP for inL in subL]
    outLines = sorted(set(outLines))
    return outLines


def procCallProcsExtraction(prcLines, prcPDict=procPosDict):
    outPrcNames = []
    for lineNo in prcLines:
        procKeys = list(filter(lambda inP: inP,
                               map(lambda inPrc: inPrc if prcPDict[inPrc][0] <= lineNo <= prcPDict[inPrc][1] else None,
                                   prcPDict)))
        outPrcNames += procKeys
    return outPrcNames


def procDetailsExtraction(prcList, prcCallInPrcDict, prcCDict=procCallDict, prcPDict=procPosDict):
    outLines = []
    for inP in prcList:
        outLines += prcCDict[inP] + prcPDict[inP]
        if prcCallInPrcDict[inP]:
            subProcLines = procDetailsExtraction(prcCallInPrcDict[inP], prcCallInPrcDict)
            outLines += subProcLines
    return outLines


def mainProcesser(srcPgmDir, logicExtDetails):
    logger.info(f"=========== Processing: {logicExtDetails} ===========")
    dictKey = None
    jobStarted = False
    detailsDict = dict()
    rptWritingProcs = dict()
    loopPosDict = {'IF': [], 'DO': []}
    lineCnt = 0
    printerFiles = list()
    tline = ''
    rline = ''

    srcPgmName = logicExtDetails['SourceProgram']
    srcPgmPath = f"{srcPgmDir}/{srcPgmName}"

    with open(srcPgmPath, 'r') as f:
        for line in f:
            line = line[:71].strip()
            if line == '' or line[0] == '*':
                continue
            rline += ' ' + line
            if rline[-1] == '+':
                continue
            rline = rline.strip()
            if "'" not in rline:
                line = '|'.join(rline.split())
                rline = ''
            else:
                tline += rline
                rline = ''
                if tline.count("'") % 2 != 0:
                    continue
                line = lineFormatter(tline, "'")
                tline = ''
            tmpline = line.split('|')

            if tmpline[0] == 'JOB' or jobStarted:
                if lineCnt == 0:
                    jobStarted = True
                    dictKey = 'JOB'
                    detailsDict[dictKey] = list()
                    if 'START' in tmpline:
                        procCallDict[tmpline[tmpline.index('START') + 1]] = [1]
                    if 'FINISH' in tmpline:
                        procCallDict[tmpline[tmpline.index('FINISH') + 1]] = [1]

                if tmpline[0] == 'REPORT' and 'PRINTER' in tmpline:
                    dictKey = tmpline[tmpline.index('PRINTER') + 1]
                    rptWritingProcs[dictKey] = tmpline[tmpline.index('REPORT') + 1]
                    logger.info(f"[INFO] ##4 Not Considered: {line}")
                    continue

                if dictKey in printerFiles:
                    if tmpline[0] in ['TITLE', 'LINE']:
                        detailsDict[dictKey] += list(filter(lambda x: x, map(
                            lambda x: x.replace(',', '') if varCheck(x.replace(',', '')) else None, tmpline[2:])))
                        continue
                    elif tmpline[0] in ['SEQUENCE', 'HEADING']:
                        logger.info(f"[INFO] ##5 Not Considered: {line}")
                        continue

                lineCnt += 1

                if tmpline[0] in ('PERFORM', 'PRINT'):
                    procCallDict[tmpline[1]] = procCallDict[tmpline[1]].append(lineCnt) \
                        if tmpline[1] in procCallDict else [lineCnt]

                if 'PROC' in tmpline or 'PROC.' in tmpline:
                    procName = tmpline[0].split('.')[0]
                    procPosDict[procName] = [lineCnt]
                    dictKey = 'JOB'
                elif 'END-PROC' in tmpline or 'END-PROC.' in tmpline:
                    procPosDict[procName].append(lineCnt)

                if 'IF' in tmpline:
                    loopPosDict['IF'].insert(0, [lineCnt])
                elif 'ELSE' in tmpline or 'ELSE-IF' in tmpline:
                    loopPosDict['IF'][0].append(lineCnt)
                elif 'END-IF' in tmpline:
                    cnt = line.count('END-IF')
                    for k in range(cnt):
                        loopPosDict['IF'][0].append(lineCnt)
                        loopPosDict['IF'] = loopPosDict['IF'][1:] + loopPosDict['IF'][0:1]

                if 'DO' in tmpline:
                    loopPosDict['DO'].insert(0, [lineCnt])
                elif 'END-DO' in tmpline:
                    cnt = line.count('END-DO')
                    for k in range(cnt):
                        loopPosDict['DO'][0].append(lineCnt)
                        loopPosDict['DO'] = loopPosDict['DO'][1:] + loopPosDict['DO'][0:1]

                detailsDict[dictKey].append(str(lineCnt) + '|' + line)
                continue
            elif tmpline[0] == 'FILE':
                dictKey = tmpline[1]
                detailsDict[dictKey] = list()
                if 'PRINTER' in tmpline:
                    printerFiles.append(dictKey)
                logger.info(f"[INFO] ##2 Not Considered: {line}")
                continue
            elif len(tmpline) > 1 and tmpline[1] == 'W':
                dictKey = 'WORKING'
                if dictKey not in detailsDict:
                    detailsDict[dictKey] = list()
            if dictKey:
                detailsDict[dictKey].append(tmpline[0])
            else:
                logger.info(f"[INFO] ##3 Not Considered: {line}")
    f.close()

    loopPosList = []
    for inKey in loopPosDict:
        loopPosList += loopPosDict[inKey]
    loopPosList.sort()

    procCallInProcDetails = reduce(lambda res, src: res.update(src) or res,
                                   map(lambda inPrc: {inPrc: procCallProcsExtraction(procCallDict[inPrc])},
                                       procCallDict.keys()), {})

    finalProcCallDict = reduce(lambda res, src: res.update(src) or res,
                               map(lambda inPrc:
                                   {inPrc: procCallDict[inPrc] +
                                           procDetailsExtraction(procCallInProcDetails[inPrc], procCallInProcDetails)},
                                   procCallDict.keys()), {})

    finalProcDetails = reduce(lambda res, src: res.update(src) or res,
                         map(lambda x: {x: detailsExtraction(finalProcCallDict[x], loopPosList)}, finalProcCallDict),
                         {})

    print('loopPosList:', loopPosList)
    print('rptWritingProcs:', rptWritingProcs)
    print('procPosDict:', procPosDict)
    print('ProcCallDict:', procCallDict)
    print('procCallInProcDetails:', procCallInProcDetails)
    print('finalProcCallDict:', finalProcCallDict)
    print('finalProcDetails:', finalProcDetails)

    for kkk in detailsDict:
        print(f'=============={kkk}=================')
        for rrr in detailsDict[kkk]:
            print(rrr)

    for logicExtDD in logicExtDetails['OutputFiles']:
        print(f"-------------Getting details for {logicExtDD}-------------")
        outLogicDict = dict()
        outLogicDict['ReportSelection'] = finalProcDetails[
            rptWritingProcs[logicExtDD]][:] if logicExtDD in rptWritingProcs else []

        for field in ['@RecordFilterLogic@'] + detailsDict[logicExtDD]:
            logicLineNos = logicExtractor(field, detailsDict['JOB'])
            print(f"Fld: {field} LLine: {logicLineNos}")
            outLogicDict[field] = []
            if logicLineNos:
                logicLines = detailsExtraction(logicLineNos, loopPosList)
                procsWithLogicLines = procCallProcsExtraction(logicLines)
                print('##1 logicLines:', logicLines, 'prcWL:', procsWithLogicLines)
                for eachPrc in procsWithLogicLines:
                    logicLines += finalProcDetails[eachPrc]
                outLogicDict[field] += sorted(set(logicLines))
                print('##1 logicLines:', logicLines)
        print('----------------------------------------------------------')
        outFilePath = srcPgmDir.rsplit('/', 1)[0] + '/' + srcPgmName.rsplit('.', 1)[0] + '-' + logicExtDD + '.txt'
        outF = open(outFilePath, 'w+')
        for field in outLogicDict:
            print(f"{field} lines: {outLogicDict[field]}")
            outF.write('-' * 20 + ' ' + field + ' ' + '-' * 20 + '\n')
            if outLogicDict[field]:
                for inLine in outLogicDict[field]:
                    rptLine = detailsDict['JOB'][inLine - 1].split('|', 1)[1]
                    outF.write(rptLine.replace('|', ' ') + '\n')
            else:
                outF.write('Unable to find logic for this field' + '\n')
        outF.close()

# -----------------------------------------------------------------------------------------
#   Main Logic
# -----------------------------------------------------------------------------------------
script_start = default_timer()
try:
    with open('./EZT_LogicExtraction_Params.json') as fparam:
        eztparams = json.load(fparam)
except Exception as excp:
    print("Unable to load EZT_LogicExtraction_Params.json parameter file")
    raise excp

currdate = datetime.now().strftime("%Y%m%d-%H%M%S")
logger = logging.getLogger()
logFilePath = eztparams['SourceProgramDirectory'].rsplit('/', 1)[0] + '/' + 'EZT_Logic_Extraction-' + currdate + '.log'
logging.basicConfig(filename=logFilePath, level=logging.INFO)

for eachValue in eztparams['LogicExtractDetails']:
    try:
        mainProcesser(eztparams['SourceProgramDirectory'], eachValue)
    except Exception as excp:
        print(f"Unable to process {eachValue}")
        logger.info(f"[ERR] +++++ Script Failed while Processing {eachValue} +++++")
        raise excp

script_end = default_timer()
total_time = script_end - script_start
mins, secs = divmod(total_time, 60)
hours, mins = divmod(mins, 60)
print(f'### Execution Time: {hours} Hrs {mins} Mns {secs} Secs')
logger.info(f"[INFO] ### Execution Time: {hours} Hrs {mins} Mns {secs} Secs")
