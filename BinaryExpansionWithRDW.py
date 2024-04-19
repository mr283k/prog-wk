import codecs
import json
mfpath='H:/EBCIDIC2ASCII/Mainframe Parsing/MainDetails/DataFiles/'
filename = 'PUNNB00.K.NEW.BUS.SUPP.BIN'
totalBytesRead = 0
totalRecs = 0
checkCnt = 1
finalDict = {}
with open(mfpath + filename, 'rb') as file:
    while True:
        if totalRecs > checkCnt * 500000:
            print('## totalRecs:', totalRecs)
            checkCnt += 1
        RDWBytes = file.read(4)
        rdw = int.from_bytes(RDWBytes[:2], 'big', signed=True)

        if RDWBytes == '' or rdw <= 0:
            break
        recID = codecs.decode(file.read(rdw-4)[9:13], 'cp500')
        # print('recID:', recID, 'RDW:', rdw, 'totalBytesRead:', totalBytesRead)
        if recID not in finalDict:
            finalDict[recID] = totalBytesRead
        totalBytesRead += rdw
        totalRecs += 1

print('='*60)
print('## totalRecs:', totalRecs)
print(json.dumps(finalDict, indent=2))
print('='*60)