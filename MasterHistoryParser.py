import keyboard
from pathlib import Path
import sys
import csv
import os
import json
     
############################################################################################################################
def typesFoundList(inputFile,fileNum):
    global keepAliveDataList
    global robotMalfunctions
    global cleaningEvents
    global RobotFailingMessages
    global MasterStateMessages

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
    if fileNum==None:
        FileName = '{0}_{1}.csv'.format(theDir[len(theDir)-1],EventFileType)
    else:
        FileName = '{0}_{1}__{2}.csv'.format(theDir[len(theDir)-1],EventFileType,fileNum)    
    print(FileName)
    File = open(os.path.join(outputDir,FileName),"w+")
    writerFile = csv.DictWriter(File, fieldnames=headers,lineterminator='\n')
    writerFile.writeheader()
    return [writerFile,File]    #createCSVfile   ##GOOD

def logTOcsvLines(lines):
    global keepAliveDataList
    global robotMalfunctions
    global cleaningEvents
    global RobotFailingMessages
    global MasterStateMessages

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


def getHeadersFromJson(FileTypeData):
    headers = list()
    for dType in FileTypeData:
        for k in dType:
            if k not in headers:
                headers.append(k)
    return headers

def directoryMainUnitedFiles(argv):
    global keepAliveDataList
    global robotMalfunctions
    global cleaningEvents
    global RobotFailingMessages
    global MasterStateMessages

    keepAliveDataList = list()
    robotMalfunctions = list()
    cleaningEvents = list()
    RobotFailingMessages = list()
    MasterStateMessages = list()

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
            headers = getHeadersFromJson(allData[EventFileType])
            
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
        EventFiles[f][1].close()       

    headers = getHeadersFromJson(keepAliveDataList) 
    [writerFile,File] =createCSVfile(outputDir,'AllKeepAliveData',headers,inputDir,None)
    writerFile.writerows(keepAliveDataList)
    headers = getHeadersFromJson(robotMalfunctions) 
    [writerFile,File] =createCSVfile(outputDir,'robotMalfunctions',headers,inputDir,None)
    writerFile.writerows(robotMalfunctions)
    headers = getHeadersFromJson(cleaningEvents) 
    [writerFile,File] =createCSVfile(outputDir,'cleaningEvents',headers,inputDir,None)
    writerFile.writerows(cleaningEvents)
    headers = getHeadersFromJson(RobotFailingMessages) 
    [writerFile,File] =createCSVfile(outputDir,'RobotFailingMessages',headers,inputDir,None)
    writerFile.writerows(RobotFailingMessages)
    headers = getHeadersFromJson(MasterStateMessages) 
    [writerFile,File] =createCSVfile(outputDir,'MasterStateMessages',headers,inputDir,None)
    writerFile.writerows(MasterStateMessages)
    #global MasterStateMessages

    print('NOT_DONE:\t')
    for t in RECORDS_NOT_TODO:
        print('\t'+t)
    return #directoryMainUnitedFiles   ##GOOD

def main():
    directoryMainUnitedFiles(sys.argv)  #GOOD

RECORDS_NOT_TODO = ['CommunicationCycle','StationMessage','DisablerState','GradualCleanState','DisablerEvent','SchedulerState','UserMessage','SMSEvent']
RECORDS_PARSED_SEPARATELY = ['RobotSuccessfulMessage','RobotMalfunction','CleaningEvent','RobotFailingMessage','MasterState']

def cutUnwantedChars(dataStr):
    res = ''
    wantedDataChars = ['0','1','2','3','4','5','6','7','8','9','-','.']
    for s in dataStr:
        if s in wantedDataChars:
            res = res+s
    return res

def getRobotLocation(CurrentLocationDescription):
    CurrentLocationDescriptionStr = CurrentLocationDescription.split(',')
    SurfaceType = CurrentLocationDescriptionStr[1].split(': ')[0]
    SurfaceNumber = int(CurrentLocationDescriptionStr[1].split(': ')[1])
    isOdd = bool(SurfaceNumber%2)
    if SurfaceType=='seg':
        if isOdd:SurfaceNumber = int((SurfaceNumber+1)/2)
        else: SurfaceNumber = int((SurfaceNumber+2)/2)
    elif SurfaceType=='dock':
        if isOdd:SurfaceNumber = int((SurfaceNumber+3)/2)
        else: SurfaceNumber = int((SurfaceNumber+2)/2)
    elif SurfaceType=='bri':
        if isOdd:SurfaceNumber = int((SurfaceNumber+1)/2)
        else: SurfaceNumber = int(SurfaceNumber/2)
    else:
        SurfaceNumber = None
        SurfaceType = 'INVALID'
    CurrentLocationDescription = {'LastCleanPercent':CurrentLocationDescriptionStr[0].strip(),'SurfaceType':SurfaceType,'SurfaceNumber_N_S':SurfaceNumber,'currentSegmentStatus':CurrentLocationDescriptionStr[2].strip(),'Direction':cutUnwantedChars(CurrentLocationDescriptionStr[3].strip()),'TT_NS':cutUnwantedChars(CurrentLocationDescriptionStr[4].split('(')[1].strip()),'TT_EW':cutUnwantedChars(CurrentLocationDescriptionStr[5].split(')')[0].strip())}
    return CurrentLocationDescription

def getkeepAliveData(Data,AssetId,date,time,UTC,isCleaningEvent):
    if not isCleaningEvent:
        CurrentLocationDescription = getRobotLocation(Data['Header']['CurrentLocationDescription'])
        res = {'AssetId':AssetId,'Date':date,'Time':time,'UTC':UTC,'PVersion':Data['PVersion'],'Station':Data['Station'],'UnitToStationRssi':Data['UnitToStationRssi'],'StationToUnitRssi':Data['Header']['StationToUnitRssi'],'Command':Data['Command']['Command'],'Header':Data['RobotMessage']['Header'],'CurrentLocationDescription':CurrentLocationDescription}
        res = createDict(res['Header'],res,'')
        res.pop('Header')
        res = createDict(res['CurrentLocationDescription'],res,'')
        res.pop('CurrentLocationDescription')
    else:
        res = {'AssetId':AssetId,'Date':date,'Time':time,'UTC':UTC,'PVersion':None,'Station':None,'UnitToStationRssi':None,'StationToUnitRssi':None,'Command':Data['Event'],'Header':Data['Details']['Header'],'CurrentLocationDescription':None}
        res = createDict(res['Header'],res,'')
        res.pop('Header')
        
    return res

def lineAnalizeHistory(lineJson,i):
    global keepAliveDataList
    global robotMalfunctions
    global cleaningEvents
    global RobotFailingMessages
    global MasterStateMessages
    #if i==174:
    #    print('t')

    RecordType = lineJson['RecordType']

    
    if RecordType in RECORDS_NOT_TODO:
        return None

    AssetId = lineJson['AssetId']
    Timestamp = lineJson['Timestamp'].split('T')
    date = Timestamp[0]
    timeUTC = Timestamp[1].split('+')
    time = timeUTC[0].split('.')[0]
    UTC = '+'+timeUTC[1]
    res = {'AssetId':AssetId,'Date':date,'Time':time,'UTC':UTC}

    Data = lineJson['Data']

    res = createDict(Data,res,'')

    if RecordType=='RobotSuccessfulMessage':
        keepAliveDataList.append(getkeepAliveData(Data,AssetId,date,time,UTC,False))

    elif RecordType=='CleaningEvent':
        event = Data['Event']
        if type(Data['Details'])==dict and not Data['Details'].get('Header')==None:
            for KA in keepAliveDataList:
                if KA['AssetId']==AssetId:
                    if date == KA['Date']:
                        if not time == KA['Time']:
                            keepAliveDataList.append(getkeepAliveData(Data,AssetId,date,time,UTC,True))

        if event=='cleaningComplete':
            res['Reason'] = Data['Details']['Reason']
            res['Details'] = Data['Details']['Details']
            res['CleanBehavior'] = None
            res['unitParams'] = None
            res['requestTime'] = None
            res['cleaningSession'] = None
        elif event=='startCommandRequest':
            res['Reason'] = None
            res['Details'] = None
            res['unitParams'] = Data['Details']['Command']['startClean']['unitPrameters']
            res['requestTime'] = Data['Details']['Trigger']['RequestTime']
            res['cleaningSession'] = Data['Details']['Trigger']['cleaningSession']
            res['CleanBehavior'] = Data['Details']['CleanBehaviour']
        
        else:
            res['Reason'] = None
            res['Details'] = None
            res['CleanBehavior'] = None
            res['unitParams'] = None
            res['requestTime'] = None
            res['cleaningSession'] = None
           
        toPop = list()
        for r in res:
            if not r.find('Details.')==-1:
                toPop.append(r)
        for p in toPop:
            res.pop(p)
        cleaningEvents.append(res)

    elif RecordType=='RobotMalfunction':
         CurrentLocationDescription = getRobotLocation(Data['Location'])
         res = createDict(CurrentLocationDescription,res,'')
         res.pop('Location')
         robotMalfunctions.append(res)

    elif RecordType=='RobotFailingMessage':
         res.pop('StationRfConfig')
         res['Command'] = Data['Command']['Command']
         toPop = list()
         for r in res:
             if not r.find('Command.')==-1:
                 toPop.append(r)
         for p in toPop:
             res.pop(p)
         RobotFailingMessages.append(res)
    
    elif RecordType=='MasterState':
         toPop = list()
         toPop.append('Time')
         if type(Data['Trigger'])==dict:
            if not Data['Trigger'].get('User')==None:
                res['User'] = Data['Trigger']['User']
                toPop.append('Trigger.User')
                res['Error'] = None
                res['Cleaning'] = None
                res['CurrentLocation'] = None
                res['CurrentLocationDescription'] = None
            elif not Data['Trigger'].get('Error')==None:
                error = Data['Trigger']['Error']
                res['User'] = None
                res['Error'] = error
                res['Cleaning'] = None
                res['CurrentLocation'] = None
                res['CurrentLocationDescription'] = None
                toPop.append('Trigger.Error')
            else:
                res['User'] = None
                if not Data['Trigger'].get('Cleaning')==None:
                    res['Cleaning'] = Data['Trigger']['Cleaning']
                    res['CurrentLocation'] = Data['Trigger']['CurrentLocation']
                    res['CurrentLocationDescription'] = getRobotLocation(Data['Trigger']['CurrentLocationDescription'])
                else:
                    res['State'] = Data['Trigger']['NewState']
                    res['Cleaning'] = None
                    res['CurrentLocation'] = None
                    res['CurrentLocationDescription'] = None
                res['Error'] = None
                for r in res:
                     if not r.find('Trigger.')==-1:
                         toPop.append(r)
         else:
             res['User'] = None
             res['Error'] = None
             res['Cleaning'] = None
             res['CurrentLocation'] =None
             res['CurrentLocationDescription'] = None
         
         
         for p in toPop:
             res.pop(p)
         MasterStateMessages.append(res)
    #MasterState
    
    if RecordType in RECORDS_PARSED_SEPARATELY:
        return None
    else:
        return [RecordType,res]     #lineAnalizeHistory     #GOOD

def createDict(Data,res,baseName):
    if not type(Data)==dict:
        res[baseName] = Data
        return res
    for dJson in Data:
        if type(Data[dJson])==dict:
            for d in Data[dJson]:
                if not baseName =='':
                    indexTag = '{0}.{1}.{2}'.format(baseName,dJson,d)
                else:
                    indexTag = '{0}.{1}'.format(dJson,d)
                if not Data[dJson][d]==None:
                    if type(Data[dJson][d])==dict:
                        res[indexTag] = Data[dJson][d]
                    else:
                        res[indexTag] = Data[dJson][d]
                else:
                    res[indexTag] = None
        else:
            if not baseName =='':
                indexTag = '{0}.{1}'.format(baseName,dJson)
            else:
                indexTag = '{0}'.format(dJson)
            res[indexTag] = Data[dJson]
    return res      #createDict #GOOD



if __name__=='__main__':
    main()
    print('\nDone. press key to exit')
    keyboard.read_key()
