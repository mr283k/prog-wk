import os
"""
compares files between two folders and displays missing files
"""

in_dir1 = '//MCKUserData2/users3/VENRXTHALAMANCHI/Citrix/Desktop/Check Details/1 Old/'
in_dir2 = '//MCKUserData2/users3/VENRXTHALAMANCHI/Citrix/Desktop/Check Details/2 New/'

inList1 = []
for root_dir, folders, files in os.walk(in_dir1):
    root_dir = root_dir if root_dir[-1] == '/' else root_dir + '/'
    for infile in files:
        inList1.append(infile)

inList2 = []
for root_dir, folders, files in os.walk(in_dir2):
    root_dir = root_dir if root_dir[-1] == '/' else root_dir + '/'
    for infile in files:
        inList2.append(infile)

print(f'{in_dir1} has {len(inList1)} files and total after removing duplicates {len(set(inList1))}')
print(f'{in_dir2} has {len(inList2)} files and total after removing duplicates {len(set(inList2))}')

print('====== Difference ======')
difCnt = 0
for df in set(inList1).symmetric_difference(set(inList2)):
    difCnt += 1
    print(df)
print('========================')
print('Total Different Files Count:', difCnt)
