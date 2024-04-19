import os

if __name__ == '__main__':
    import timeit
    script_start = timeit.default_timer()

    cpyDirPath = 'H:/EBCIDIC2ASCII/Mainframe Parsing/MainDetails/DDL/CpyBooks/'
    cpyList = list(os.listdir(cpyDirPath))
    outWriter = open(cpyDirPath + '1Out.txt', 'w+')
    print(cpyList)
    for inf in cpyList:
        outList = []
        trlrCnt = 0
        if inf[-4:] not in ('.txt', '.TXT'):
            print(f"File NOt processed: {inf}")
            continue
        with open(cpyDirPath + inf) as pf:
            for inrec in pf:
                inrec = inrec[6:72].strip()
                if inrec != '' and inrec[0] != '*' and ' PIC ' not in inrec:
                    if inrec.split()[0] in ('01', '02'):
                        trec = inrec[2:].strip()
                        fldName= trec.split()[0]
                        if fldName[:4] == 'TRLR':
                           trlrCnt += 1
                           indexOfTRLR = fldName[4:].index('-')
                           outWriter.write(f"{inf[:-4]}\t{fldName[4:4+indexOfTRLR]}\n")

    script_end = timeit.default_timer()
    total_time = script_end - script_start
    mins, secs = divmod(total_time, 60)
    hours, mins = divmod(mins, 60)
    print('### Execution Time:' + str(hours) + ' Hrs ' + str(mins) + ' Mns ' + str(secs) + ' Secs')
