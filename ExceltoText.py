import pandas as pd
import numpy as np

pd.set_option('display.max_columns', None)

inDir = 'H:\EBCIDIC2ASCII\Mainframe Parsing\MainDetails\ExcelsToText\input'
outOldDir = 'H:\EBCIDIC2ASCII\Mainframe Parsing\MainDetails\ExcelsToText\out_old'
outNewDir = 'H:\EBCIDIC2ASCII\Mainframe Parsing\MainDetails\ExcelsToText\out_new'
f1 = f'{inDir}/FILE AND FIELD INFORMATION FOR LOOMIS SYSTEM v2.xlsx'
f2 = f'{inDir}/FILE AND FIELD INFORMATION FOR LOOMIS SYSTEM v3 20220701.xlsx'

oldXL = pd.read_excel(f1, engine='openpyxl', sheet_name=None)
newXL = pd.read_excel(f2, engine='openpyxl', sheet_name=None)
oldSheets = oldXL.keys()
newSheets = newXL.keys()
diff = list(set(oldSheets).symmetric_difference(set(newSheets)))
print(len(oldSheets), oldSheets)
print(len(newSheets), newSheets)
print(set(oldSheets).symmetric_difference(set(newSheets)))
cnt = 0

for eachSheet in oldSheets:
    cnt += 1
    print(f'{cnt} >> processing: {eachSheet}')
    dF = oldXL[eachSheet]
    dF.to_csv(f'{outOldDir}/{eachSheet}.txt', index=False, sep='|')

for eachSheet in newSheets:
    cnt += 1
    print(f'{cnt} ## processing: {eachSheet}')
    dF = newXL[eachSheet]
    dF.to_csv(f'{outNewDir}/{eachSheet}.txt', index=False, sep='|')

