import s3fs
from datetime import datetime
s3fsys = s3fs.S3FileSystem(anon=False)


def checkpath(argpath):
    if argpath[-1]=='/':
        return argpath
    else:
        return argpath + '/'


def write_output(arg_total_size, arg_prev_total_size, arg_info, arg_obj):
    sizediff = ((arg_total_size - arg_prev_total_size) * 100 / arg_total_size) if arg_total_size else 0
    partial_load = False if arg_prev_total_size == 0 else (True if sizediff < -10 else False)
    for each_rec in arg_info:
        arg_obj.write('\t'.join(each_rec[:-2]) + '\t' + str(arg_total_size) + '\t'
                     + str(sizediff) + '\t' + str(partial_load) + '\t' + each_rec[-2] + '\t' + each_rec[-1] + '\n')
    return partial_load


def tabfiles_handler(argBucket, argPath, argDate, argObj):
    prev_filekey = ""
    prev_run_date = 0
    total_size = 0
    prev_totalsize = 0
    info_dict = {}
    for current_dir, sub_dirs, files in s3fsys.walk(argBucket + argPath):
        if checkpath(current_dir).rsplit('/',2)[-2] != 'processed':
            continue
        for eachfile in files:
            if not eachfile or eachfile[-3:] != 'tab':
                continue
            fs = s3fsys.size(checkpath(current_dir) + eachfile)
            filekey = eachfile.rsplit(".", 1)[0]
            chunkflag = 'Y' if filekey.count('-') == 2 else 'N'
            chunk_idx = '0'
            if chunkflag == 'Y':
                filekey, chunk_idx = filekey.rsplit('-', 1)
            if filekey != prev_filekey and prev_filekey:
                bool_new_trailer_id = '_'.join(prev_filekey.split('_')[2:-1]) != '_'.join(filekey.split('_')[2:-1])
                bool_partial_load = write_output(total_size, prev_totalsize, info_dict[prev_filekey], argObj)
                if not bool_partial_load:
                    prev_totalsize = total_size
                    prev_run_date = int(prev_filekey.rsplit('_', 1)[1].split('-', 1)[0])
                if bool_new_trailer_id:
                    prev_run_date = prev_totalsize = 0

                total_size = 0
                info_dict = {}

            file_info = filekey.split("_", 2)
            tr_id, time_info = file_info[2].rsplit("_", 1)
            runDate = int(time_info.split('-', 1)[0])
            duplicate_flag = runDate == prev_run_date
            total_size += fs
            if filekey in info_dict:
                info_dict[filekey]\
                    .append([file_info[0], file_info[1], eachfile, tr_id, chunkflag,
                             chunk_idx, str(fs), str(runDate<argDate), str(duplicate_flag)])
            else:
                info_dict[filekey] = [
                    [file_info[0], file_info[1], eachfile, tr_id, chunkflag, chunk_idx,
                     str(fs), str(runDate<argDate), str(duplicate_flag)]]
            prev_filekey = filekey

    if info_dict:
        prev_totalsize = write_output(total_size, prev_totalsize, info_dict[prev_filekey], argObj)


# ---------------------------------------------------------
#  Main logic
# ---------------------------------------------------------

strBucketName = 'tmk-cdm-landing'
s3ScanDict = {
    "/tmk/ail-mainframe/pmfmast/processed/": ["5/19/2020", '%m/%d/%Y', "AIL_AILPOLICYMASTER"],
    "/tmk/ail-mainframe/nil-pmfmast/processed/": ["5/19/2020", '%m/%d/%Y', "NIL_NILPOLICYMASTER"],
    "/tmk/lnl-mainframe/alis-master/processed/": ["5/19/2020", '%m/%d/%Y', "LNL_ALISMASTER"],
    "/tmk/gl-mainframe/cfo-mastr/processed/": ["5/19/2020", '%m/%d/%Y', "GL_GLMASTER"],
    "/tmk/gl-mainframe/fundfile/processed/": ["5/19/2099", '%m/%d/%Y', "GL_FUNDFILE"],
    "/tmk/gl-mainframe/cfu-sulofundfile/processed/": ["5/19/2099", '%m/%d/%Y', "GL_CFUFUNDFILE"]
}
outDir = "D:/Users/venrxthalamanchi/2_FileExtract/"

for inPath, inInfo in s3ScanDict.items():
    print(f'<{datetime.now()}> Processing: {strBucketName}{inPath}')
    file_path = f"{checkpath(outDir)}{inInfo[2]}.txt"
    dateBefore = datetime.strptime(inInfo[0], inInfo[1]).strftime('%Y%m%d')
    print('dateBefore:', dateBefore)
    fileObject = open(file_path, 'w+')
    fileObject.write('\t'.join(['Company', 'FileName', 'TabFile', 'Trailer_ID', 'ChunkFlag', 'ChunkIndex',
                               'FileSize', 'TotalSize', 'SizeDiff(%)', 'PartialLoad', 'IncludeFlag',
                                'DuplicateFlag']) + '\n')
    tabfiles_handler(strBucketName, inPath, int(dateBefore), fileObject)
    fileObject.close()
    print(f'<{datetime.now()}> Completed: {strBucketName}{inPath}')
