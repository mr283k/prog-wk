import os

inDir = 'H:/EBCIDIC2ASCII/Mainframe Parsing/MainDetails/ExcelsToText/DDLs_In/'
outDir = 'H:/EBCIDIC2ASCII/Mainframe Parsing/MainDetails/ExcelsToText/DDLs_Out/'
targetDBInfo = {'LNL': 'dl_lnl_mainframe', 'AIL': 'dl_ail_mainframe',
                'GL': 'dl_gl_mainframe', 'NIL': 'dl_nil_mainframe'}
processed = 0
for root, subDir, files in os.walk(inDir):
    root = f'{root}/' if root[-1] != '/' else root
    for ef in files:
        targetDB = None
        if '_CURRENT.txt' in ef:
            processed += 1
            outF = f'{outDir}{ef}'
            print(f'{root}{ef}')
            targetDB = targetDBInfo.get(ef.split('_')[1])
            if not targetDB:
                continue

            with open(outF, 'w') as outFile:
                with open(f'{root}{ef}', 'r') as inF:
                    dbName = ''
                    finalRec = ''
                    while dbName == '':
                        rec = inF.readline()
                        print(rec)
                        if 'CREATE DATABASE IF NOT EXISTS' in rec:
                            dbName = rec.split('\n')[0].strip().split()[-1].rsplit(';', 1)[0].strip()
                            print(dbName)
                        finalRec += rec
                    finalRec += inF.read()
                    outFile.write(finalRec.replace(dbName, targetDB))
        else:
            continue
print('Total Processed: ', processed)