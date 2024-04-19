import os
import json


def dirPathCheck(argPath):
    argPath = argPath if argPath[-1] == '/' else argPath + '/'
    return argPath

"""
formats json files with indent2 and writes to the new folder
"""

in_dir = 'H:/EBCIDIC2ASCII/Mainframe Parsing/MainDetails/JsonFiles/InJson'
out_dir = 'H:/EBCIDIC2ASCII/Mainframe Parsing/MainDetails/JsonFiles/OutJson'

total_files = 0
out_dir = dirPathCheck(out_dir)
for root_dir, folders, files in os.walk(in_dir):
    root_dir = dirPathCheck(root_dir)
    tgt_folder = root_dir.replace(in_dir, '').replace('/', '').replace('\\', '')
    tgt_dir = f'{out_dir}{tgt_folder}' if tgt_folder else out_dir[:-1]
    if not os.path.exists(tgt_dir):
        os.mkdir(tgt_dir)
    for infile in files:
        total_files += 1
        print(f'Processing: {root_dir}{infile}')
        with open(f'{root_dir}{infile}') as f:
            inJsonParams = json.loads(f.read())

        with open(f'{tgt_dir}/{infile}', 'w+') as outf:
            json.dump(inJsonParams, outf, indent=2)

print('total_files:', total_files)
