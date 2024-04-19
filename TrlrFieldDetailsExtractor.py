"""
Generates the file with all trailers and field counts
input: Processing parameter file
output: report with field and trailer counts
"""

import json
import os


def extract_details(argelements):
    fcount = 0
    for each in argelements:
        if type(each) is dict:
            fcount += len(each.get('SourceFields', [])) + 1
        elif type(each) is list:
            sub_file_field_cnt = extract_details(each[:-1])
            fcount += sub_file_field_cnt
        else:
            raise Exception('Invalid Format in Json Parameter file')
    return fcount


class DetailsExtraction:

    special_params = ('processing_params_ADPPREMIUM.json', 'processing_params_DAILYADPPREMIUM.json')

    def __init__(self, indir, outdir):
        self.inDir = indir + ('/' if indir[-1] != '/' else '')
        self.outDir = open(f"{outdir}{'/' if outdir[-1] != '/' else ''}Details_Report.txt", 'w')
        self.outDir.write('Company\tFileName/TotalFiles\tDetails\tTrailerID/TrailerCount\tNoOfFields\n')
        self.totalTrlrs = 0
        self.totalFields = 0
        self.TotalFiles = 0

    def extract_param_files(self):
        for rootDir, folders, inFiles in os.walk(self.inDir):
            for inParam in inFiles:
                if 'processing_params' in inParam:
                    yield f"{rootDir}{'/' if rootDir[-1] != '/' else ''}{inParam}"

    def write_details(self, infile):
        trlrs_count = 0
        trlrs_field_count = 0
        self.TotalFiles += 1
        with open(infile) as pf:
            params = json.load(pf)

        if infile.rsplit('/', 1)[-1] in DetailsExtraction.special_params:
            trlrs_count += 1
            field_count = len(params['Elements'])
            self.outDir.write(f"{params['Company']}\t{params['FileName']}\t\t{params['TrailerID']}\t{field_count}\n")
            trlrs_field_count += field_count
        else:
            for recType in params["RecordTypes"]:
                trlrs_count += 1
                field_count = extract_details(params['RecordTypes'][recType]['Elements']) - 1
                self.outDir.write(f"{params['Company']}\t{params['FileName']}\t\t{recType}\t{field_count}\n")
                trlrs_field_count += field_count

        self.outDir.write(f"{params['Company']}\t{params['FileName']}\tTotalTrailers\t"
                          f"{trlrs_count}\t{trlrs_field_count}\n")

        self.totalTrlrs += trlrs_count
        self.totalFields += trlrs_field_count

    def terminate(self):
        self.outDir.write(f"All\t{self.TotalFiles}\tOverallTrailers\t{self.totalTrlrs}\t{self.totalFields}")
        self.outDir.close()


if __name__ == '__main__':
    import timeit
    script_start = timeit.default_timer()

    paramDirPath = 'H:/EBCIDIC2ASCII/Mainframe Parsing/MainDetails/DDL/InParams/'
    ddlDirPath = 'H:/EBCIDIC2ASCII/Mainframe Parsing/MainDetails/DDL/DDL/'
    mainObj = DetailsExtraction(paramDirPath, ddlDirPath)
    inParams = mainObj.extract_param_files()
    for inF in inParams:
        mainObj.write_details(inF)
    mainObj.terminate()

    script_end = timeit.default_timer()
    total_time = script_end - script_start
    mins, secs = divmod(total_time, 60)
    hours, mins = divmod(mins, 60)
    print('### Execution Time:' + str(hours) + ' Hrs ' + str(mins) + ' Mns ' + str(secs) + ' Secs')
