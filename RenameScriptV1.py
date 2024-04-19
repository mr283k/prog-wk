import os

in_dir = '//MCKUserData2/users3/VENRXTHALAMANCHI/Citrix/Desktop/Check Details/1 Old'

orig_string = 'PROD_'
# orig_string = 'DEV_'
# new_string = 'DEV_'
# new_string = 'PROD_'
new_string = 'TEST_'
total_files = 0

for root_dir, folders, files in os.walk(in_dir):
    root_dir = root_dir if root_dir[-1] == '/' else root_dir + '/'
    for infile in files:
        total_files += 1
        os.rename(root_dir + infile, root_dir + infile.replace(orig_string, new_string, 1))

print('total_files:', total_files)
