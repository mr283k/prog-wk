from datetime import datetime
import boto3
import time
import logging

argAthena = boto3.client('athena', region_name='us-west-2')
acctID = boto3.client("sts").get_caller_identity()["Account"]
argAthenaS3 = f's3://aws-athena-query-results-{acctID}-us-west-2/jumpbox'

currDate = datetime.now().strftime("%Y%m%d-%H%M%S")
tableFileFullPath = 'D:/Users/venrxthalamanchi/5_Automation/input/AthenaTableDetails_GL.txt'
inSuffix = tableFileFullPath.rsplit('/', 1)[1].replace('AthenaTableDetails_', '', 1).rsplit('.', 1)[0]
outFileFullPath = f'D:/Users/venrxthalamanchi/5_Automation/result/AthenaDiffCheck_Result_{inSuffix}_{currDate}.txt'
logFilePath = f'D:/Users/venrxthalamanchi/5_Automation/result/AthenaDiffCheck_{inSuffix}_{currDate}.log'

logger = logging.getLogger()
logging.basicConfig(filename=logFilePath, level=logging.INFO)
processCount = 0
sqlQuery = "SELECT COUNT(*) FROM (SELECT * FROM {0} EXCEPT SELECT * FROM {1})"


def queryExecutor(argSql):
    logger.info(f'Starting: {datetime.now().strftime("%Y%m%d-%H%M%S")}')
    startResp = argAthena.start_query_execution(
        QueryString=argSql,
        ResultConfiguration={'OutputLocation': argAthenaS3})
    qid = startResp['QueryExecutionId']

    argCount = 1
    queryStatus = False
    while argCount <= 4:
        time.sleep(4 ** argCount)
        logger.info(f'Checking{argCount}: {datetime.now().strftime("%Y%m%d-%H%M%S")}')
        getQueryResp = argAthena.get_query_execution(QueryExecutionId=qid)
        execStatus = getQueryResp['QueryExecution']['Status']['State']

        logger.info(f'Status: {datetime.now().strftime("%Y%m%d-%H%M%S")} ==> {execStatus}')
        if execStatus in ('FAILED', 'CANCELLED'):
            logger.info(f"{getQueryResp['QueryExecution']['Status']['StateChangeReason']}")
            logger.info(f'{getQueryResp}')
            break
        elif execStatus == 'SUCCEEDED':
            logger.info('Query execution completed successfully')
            queryStatus = True
            break
        argCount += 1

    return qid, queryStatus


def getResult(qid):
    response = argAthena.get_query_results(
        QueryExecutionId=qid,
        MaxResults=123
    )
    return int(response['ResultSet']['Rows'][1]['Data'][0]['VarCharValue'])


with open(outFileFullPath, 'w') as o:
    o.write('CurrDB\tCurrTable\tPrevDB\tPrevTable\tCurrVSPrevQuery\tCurrVSPrev\tPrevVSCurrQuery\tPrevVSCurr\tComments\n')
    with open(tableFileFullPath, 'r') as f:
        while True:
            rec = f.readline().replace('\n', '')
            if rec == '':
                break
            processCount += 1
            print(f'{datetime.now().strftime("%Y%m%d-%H%M%S")} Processing: {processCount}')
            currDBTbl, prevDBTbl = rec.split('\t', 1)
            currDB, currTbl = currDBTbl.split('.', 1)
            prevDB, prevTbl = prevDBTbl.split('.', 1)
            logger.info(f'Processing: {processCount}')
            cVSpSql = sqlQuery.format(currDBTbl, prevDBTbl)
            qid, status = queryExecutor(cVSpSql)
            if not status:
                o.write(f'{currDB}\t{currTbl}\t{prevDB}\t{prevTbl}\t{cVSpSql}\tNA\tNA\tNA\tFailed\n')
                continue
            currVsPrev = getResult(qid)

            pVScSql = sqlQuery.format(prevDBTbl, currDBTbl)
            qid, status = queryExecutor(pVScSql)
            if not status:
                o.write(f'{currDB}\t{currTbl}\t{prevDB}\t{prevTbl}\t{cVSpSql}\t{currVsPrev}\t{pVScSql}\tNA\tFailed\n')
                continue

            prevVsCurr = getResult(qid)
            comments = "Matching" if currVsPrev == 0 and prevVsCurr == 0 else "Not-Matching"
            o.write(f'{currDB}\t{currTbl}\t{prevDB}\t{prevTbl}\t'
                    f'{cVSpSql}\t{currVsPrev}\t{pVScSql}\t{prevVsCurr}\t{comments}\n')
