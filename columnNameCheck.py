import os
import re
dirpath = "H:/EBCIDIC2ASCII/Mainframe Parsing/MainDetails/JsonFiles"

regex = re.compile('-')
fcnt = 0
for inParm in os.scandir(dirpath):
    fcnt += 1
    with open(inParm.path, 'r') as jfile:
        print(f"{fcnt} Checking file: {inParm.name}")
        for i in jfile:
            tKey = i.replace('\t', ' ').strip()[:17]
            if tKey == '"PreviousBucket":' or tKey == '"PreviousFolder":':
                continue

            if (regex.search(i) != None):
                print("ColumnName is not accepted:", i[:-1])

#
# with open(dirpath+'processing_params_AGBM.json', 'r')as jfile:
#     for i in jfile:
#         if (regex.search(i) != None):
#             print("MRID ColumnName is not accepted.", i)
# #
