import os

in_dir = '//MCKUserData2/users3/VENRXTHALAMANCHI/Citrix/Desktop/Check Details/1 Old'
out_dir = '//MCKUserData2/users3/VENRXTHALAMANCHI/Citrix/Desktop/Check Details/1 New'

total_files = 0

for root_dir, folders, files in os.walk(in_dir):
    root_dir = root_dir if root_dir[-1] == '/' else root_dir + '/'
    for infile in files:
        total_files += 1
        with open(root_dir + infile, 'r') as inf:
            data = inf.read()

        with open(f'{out_dir}/{infile}', 'w') as outf:
            outf.write(data.upper())


print('total_files:', total_files)
