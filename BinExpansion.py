import codecs
mfpath='H:/EBCIDIC2ASCII/Mainframe Parsing/MainDetails/DataFiles/'
filename = 'EXTLIFECN.BIN'
file4 = open('H:/EBCIDIC2ASCII/Mainframe Parsing/MainDetails/AnalysisOutput/output.txt', 'w+')
with open(mfpath + filename,'rb') as file:
    k = 0
    data=file.read(100000)
    print('Record:', k, 'RecLen:', len(data))
    t_data = codecs.decode(data, 'cp500')
    byte_cnt = -1
    h_data = ''
    for bb in data:
            byte_cnt += 1
            vv = hex(ord(chr(bb)))[2:]
            if len(vv) == 1:
                vv = '0' + vv
            h_data += vv
            w_rec = str(byte_cnt + 1) + '~' + vv + '~' + str(t_data[byte_cnt].encode('UTF-8'))[2:-1]
            file4.write(w_rec + '\n')

file.close()
file4.close()