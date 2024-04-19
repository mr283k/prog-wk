import os
import json


def dirPathCheck(argPath):
    argPath = argPath if argPath[-1] == '/' else argPath + '/'
    return argPath


in_dir = '//MCKUserData2/users3/VENRXTHALAMANCHI/Citrix/Desktop/Check Details/1 Old/'
total_files = 0
for root_dir, folders, files in os.walk(in_dir):
    root_dir = dirPathCheck(root_dir)
    for infile in files:
        total_files += 1
        rec = f'{total_files}:{infile}'
        # if 'loomis_history_params_' in infile:
        #     with open(f'{root_dir}{infile}') as f:
        #         inJsonParams = json.loads(f.read())
        #     tbleName = inJsonParams['AthenaTable'].split('.', 1)[-1].replace('hist_', '')
        #     currDir = inJsonParams['HistoryFolder'].replace('/history', '/current')
        #     rec = rec + f':glb_ff_{tbleName}:{currDir}'
        print(rec)

print('total_files:', total_files)
