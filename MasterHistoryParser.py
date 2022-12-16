import datetime
from pathlib import Path
import sys
import csv
import os
import json
     
############################################################################################################################
def typesFoundList(inputFile,fileNum):
    suffixIndexTxt = str.find(inputFile.name,".txt")
    suffixIndexLog = str.find(inputFile.name,".log")
    suffixIndexJson = str.find(inputFile.name,".json")
    if(suffixIndexTxt==-1 and suffixIndexLog==-1 and suffixIndexJson==-1):
        return None
    if(not str.find(inputFile.name,".xlsx")==-1):
        return
    if(not str.find(inputFile.name,".csv")==-1):
        return
    print('FILE:\t{0}__{1}'.format(inputFile,fileNum))
    #########################
    #open input file
    try:
        fp = open(inputFile, 'r')
    except IOError: 
           print ("Error: File does not appear to exist.")
           return
    #########################
    #input all lines
    lines = fp.readlines()
    [allData,typesFound,notParsedRecordTypes] = logTOcsvLines(lines)
    fp.close()  #typesFoundList ##GOOD
    return [allData,typesFound,notParsedRecordTypes]    #typesFoundList     #GOOD

def createCSVfile(outputDir,EventFileType,headers,inputDir,fileNum): 
    theDir = str.split(inputDir,'\\')
    #create output files and open it
    FileName = '{0}_{1}__{2}.csv'.format(theDir[len(theDir)-1],EventFileType,fileNum)    
    print(FileName)
    File = open(os.path.join(outputDir,FileName),"w+")
    writerFile = csv.DictWriter(File, fieldnames=headers,lineterminator='\n')
    writerFile.writeheader()
    return [writerFile,File]    #createCSVfile   ##GOOD

def logTOcsvLines(lines):  
    i=0
    typesFound = list()
    allData = dict()
    notParsedRecordTypes = list()
    for line in lines:
        i=i+1
        lineJson = json.loads(line)
        linRes = lineAnalizeHistory(lineJson,i)
        if(linRes==None):
            if lineJson['RecordType'] not in notParsedRecordTypes:
                notParsedRecordTypes.append(lineJson['RecordType'])
            continue
        [message_type,TLine] = linRes
        if message_type not in typesFound:
            typesFound.append(message_type)
            allData[message_type] = []
        allData[message_type].append(TLine)
    return [allData,typesFound,notParsedRecordTypes]    #logTOcsvLines      #GOOD


def directoryMainUnitedFiles(argv):

    if(len(argv)<2):
        print("Input file Directory:")
        inputDir = input()
    else:
        inputDir = argv[1]
    
    EventFilesTypesDone = dict()
    EventFiles = dict()
    if(os.path.isdir(inputDir)):
        
        outputDir = inputDir + '\\'+'Parsed'
        if not (os.path.isdir(inputDir + '\\'+'Parsed')):
           os.mkdir(outputDir)

    fileNum = 0
    entries = Path(inputDir)
    for entry in entries.iterdir():
        if(os.path.isdir(entry)):
            continue
        Data = typesFoundList(entry,fileNum)
        if Data==None:
            continue
        [allData,typesFound,notParsedRecordTypes] = Data
        fileNum = fileNum+1
        for EventFileType in typesFound:
            headers = list()
            for dType in allData[EventFileType]:
                for k in dType:
                    if k not in headers:
                        headers.append(k)
            if not EventFileType in EventFilesTypesDone:
                EventFilesTypesDone[EventFileType] = {'headers':headers}
            else:
                s = set(EventFilesTypesDone[EventFileType]['headers']).difference(headers)
                p = set(headers).difference(EventFilesTypesDone[EventFileType]['headers'])
                headers = EventFilesTypesDone[EventFileType]['headers']
                if len(s)>0 or len(p)>0:
                    for h in s:
                        headers.append(h)
                    for h in p:
                        headers.append(h)
                EventFilesTypesDone[EventFileType]['headers'] = list(set(headers))
            EventFiles[EventFileType] =createCSVfile(outputDir,EventFileType,sorted(headers),inputDir,fileNum)
        
        count = 0
        for d in allData:
            if d not in notParsedRecordTypes:               
                EventFiles[d][0].writerows(allData[d])
            count = count+1
     #########################
    #closing all files
    for f in EventFiles:
        EventFiles[f][1].close()       #directoryMainUnitedFiles   ##GOOD
   

def main():
    directoryMainUnitedFiles(sys.argv)  #GOOD

RECORD_TYPES_DONE = ['RobotMalfunction',#
                'SMSEvent',#
                'StationMessage',
                'MasterState',
                'RobotFailingMessage',#
                'CleaningEvent',
                'UserMessage',
                'RobotSuccessfulMessage',
                'CommunicationCycle',
                'DisablerState','SchedulerState','DisablerEvent']#

#''

def lineAnalizeHistory(lineJson,i):
    #if i==174:
    #    print('t')

    RecordType = lineJson['RecordType']

    if RecordType not in RECORD_TYPES_DONE:
        print('UNKNOWN: {0}\n'.format(RecordType))
        return None

    AssetId = lineJson['AssetId']
    Timestamp = lineJson['Timestamp']
    res = {'AssetId':AssetId,'Timestamp':Timestamp}

    Data = lineJson['Data']
    res = createDict(Data,res)

    return [RecordType,res]     #lineAnalizeHistory     #GOOD

def createDict(Data,res):
    for dJson in Data:
        if type(Data[dJson])==dict:
            for d in Data[dJson]:
                indexTag = '{0}.{1}'.format(dJson,d)
                if not Data[dJson][d]==None:
                    if type(Data[dJson][d])==dict:
                        res[indexTag] = Data[dJson][d]
                    else:
                        res[indexTag] = Data[dJson][d]
                else:
                    res[indexTag] = None
        else:
            res[dJson] = Data[dJson]
    return res      #createDict #GOOD



if __name__=='__main__':
    main()
