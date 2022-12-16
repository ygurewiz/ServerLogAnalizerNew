import datetime
from pathlib import Path
import sys
import csv
import os
from binascii import hexlify
import json
     
def trascodeAsciiToHex(word):
    return '0x'+hexlify(word.encode()).decode()
    #return " ".join([hex(ord(x)) for x in word])

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
    return [allData,typesFound,notParsedRecordTypes]

def createCSVfile(outputDir,EventFileType,headers,inputDir,fileNum): #createCSVfile   ##GOOD
    theDir = str.split(inputDir,'\\')
    #create output files and open it
    FileName = '{0}_{1}__{2}.csv'.format(theDir[len(theDir)-1],EventFileType,fileNum)    
    print(FileName)
    File = open(os.path.join(outputDir,FileName),"w+")
    writerFile = csv.DictWriter(File, fieldnames=headers,lineterminator='\n')
    writerFile.writeheader()
    return [writerFile,File]

#return [writerSuccessfulMessageFile,writerFailingfulMessageFile,writerStationMessageFile,writerSMSEventFile,writerUserMessageFile,writerRobotMalfunctionFile,writerCleaningEventFile]
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
    return [allData,typesFound,notParsedRecordTypes]


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

    return [RecordType,res]

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
    return res

def getAssetID(line,i):
    index = str.find(line,"AssetId")
    AssetId = str.split(line[index+len("AssetId"):],":")[1]
    AssetId = str.split(str.split(str.split(AssetId,",")[0],'"')[1],'\n')[0].strip()
    return AssetId

def getStationToUnitResponse(line,i):
    index = str.find(line,"UnitId")
    UnitId = str.split(str.split(line[index+len("UnitId"):],":")[1],",")[0]
    index = str.find(line,"TelitResponse")
    TelitResponse = str.split(str.split(str.split(line[index+len("TelitResponse"):],":")[1],",")[0],'}')[0]
    return UnitId+ ' ' +TelitResponse

def getStationToUnitsResponses(line,i):######################################
    #ToUnit = str.split(str.split(str.split(str.split(line,"ToUnit\":")[1],'}],\"')[0],"FromUnit")[0],'},{')
    #FromUnit = str.split(str.split(str.split(str.split(line,"FromUnit\":")[1],'}],\"')[0],"Type")[0],'},{')
    toFrom = str.split(line,"FromUnit\":[{")
    ToUnit = str.split(str.split(toFrom[0],"ToUnit\":[{")[1],"UnitId\":")[1:]
    FromUnit = str.split(toFrom[1],"UnitId\":")[1:]
    parsed_line = ""
    j=0
    while(j < len(ToUnit) and j<len(FromUnit)):
        parsed_line += ToUnit[j]+'@'+FromUnit[j]+'$'
        j=j+1

    return parsed_line

def getStationFromUnitResponse(line,i):
    index = str.find(line,"UnitId")
    UnitId = str.split(str.split(line[index+len("UnitId"):],":")[1],",")[0]
    index = str.find(line,"EndReason")
    EndReason = str.split(str.split(line[index+len("EndReason"):],":")[1],",")[0]
    index = str.find(line,"Rssi")
    Rssi = str.split(str.split(str.split(line[index+len("Rssi"):],":")[1],",")[0],'}')[0]
    return UnitId+ ' ' +EndReason+ ' ' +Rssi


def getTimeStamp(line,i):
    ############
    #DATE + TIME
    index = str.find(line,"Timestamp")
    theLine = line[index+len("Timestamp : "):]
    timestamp = str.split(theLine,"T")
    if(len(timestamp)<2):
        return ""
    date = str.split(timestamp[0],"-")
    #TIME
    time = str.split(timestamp[1],":")
    seconds = str.split(time[2],".")
    microseconds = str.split(seconds[1],"+")[0][:6]
    theTime = datetime.datetime(int(date[0]),int(date[1]),int(date[2]),int(time[0]),int(time[1]),int(seconds[0]),int(microseconds))
    timeStamp = str(theTime.strftime("%b-%d-%Y %H:%M:%S")).strip()
    ##########
    return timeStamp

def getCommunication(line,i):
    #STATION
    index = str.find(line,"Station")
    station = str.split(str.split(line[index+len("Station : "):],",")[0],'"')[0]
    ##########
    #TELIT
    #'{"RecordType":"RobotSuccessfulMessage","AssetId":"row-5df2dba6-683e-4610-8b93-4cff8d2e73de","Timestamp":"2020-02-12T15:12:12.4831277+02:00","Data":{"PVersion":"T4_1_34","Station":"ES-0000-00-00-266","StationRfConfig":"","UnitToStationRssi":175,"Command":{"Command":"KEEP_ALIVE","Payload":null,"RawCommand":"hAEi"},"Header":{"Battery":132.0,"StationToUnitRssi":174,"Cleaning":{"SquareMeter":0.0,"Description":"0","Details":{"CurrentSegmentCleandSquareMeter":0.0,"CurrentSegmentCleandPercentage":0.0,"CleandSquareMeter":0.0,"CleandPercentage":0.0}},"CurrentLocationDescription":"0% ,dock: 1, 0%, 270.7°, (-2.32°,44.3°)","IsClockUpdated":true,"ErrorOutOfBase":false,"Response":"ok","MalfunctionsFlags":[],"CleaningLocationFlags":{"PowerUp":true,"StartClean":false,"GoHomeEntered":false,"AtBase":false}},"RobotMessage":{"Header":{"ResponseCode":"ok","CurrentSurfaceType":"surfacE_TYPE_PARKING","CurrentSurfaceTypeAppearnaceNum":1,"DesiredCleaningArea":0,"CleanSegmentsArea":0,"CleaningDistance":0,"RobotProcedure":"parkinG_STATE","RobotStep":"steP_PARKED","segmentsCleaned":0,"iterationInStep":0,"totalIterationsInStep":0,"RSSI":174,"Direction":27070,"Roll":-178,"Pitch":-4428,"Battey":132,"Events":["eventS_BIT_CHANGE_PARKING","eventS_BIT_POWER_UP"]},"Payload":null,"RawHeader":"AAEBAAAAAAAAAAAAAACuvmlO/7TuhAAEAQAA","RawPayload":""}}}\n'
    index = str.find(line,"TelitId")
    if(not index == -1):
        telit = str.split(str.split(line[index+len("TelitId = "):],",")[0],'"')[0]
    else:
        telit="?"
    #UnitToStationRssi
    index = str.find(line,"UnitToStationRssi")
    if(not index == -1):
        uRssi = str.split(line[index+len("UnitToStationRssi: "):],",")[0]
    else:
        uRssi= "?"
    return telit + ' ' +'? ' + station + ' ' +uRssi

def getMasterInfo(line,i):
    #command
    index = str.find(line,"Command")
    command = str.replace(str.split(str.split(line[index+len("Command"):],":")[2],',')[0],'"','')
    #battery
    index = str.find(line,"Battery")
    if(not index == -1):
        battery = str.split(str.split(line[index+len("Battery"):],":")[1],",")[0]
    else:
        battery =" "
    #StationToUnitRssi
    index = str.find(line,"StationToUnitRssi")
    if(not index == -1):
        sRssi = str.split(str.split(line[index+len("StationToUnitRssi"):],":")[1],",")[0]
    else:
        sRssi =" "
    return command + ' '+battery+ ' '+sRssi

def getCleaningInfo(line,i):
    #metersToClean
    index = str.find(line,"SquareMeter")
    SquareMeter = str.split(str.split(line[index+len("SquareMeter"):],":")[1],",")[0]
    #CurrentSegmentCleandSquareMeter
    index = str.find(line,"CurrentSegmentCleandSquareMeter")
    if(index==-1):
        CurrentSegmentCleandSquareMeter ='?'
    else:
        CurrentSegmentCleandSquareMeter = str.split(str.split(line[index+len("CurrentSegmentCleandSquareMeter"):],":")[1],",")[0]
    #CurrentSegmentCleandPercentage
    index = str.find(line,"CurrentSegmentCleandPercentage")
    if(index==-1):
        CurrentSegmentCleandSquareMeter ='?'
    else:
        CurrentSegmentCleandSquareMeter = str.split(str.split(line[index+len("CurrentSegmentCleandPercentage"):],":")[1],",")[0]
    #CleandSquareMeter
    index = str.find(line,"CleandSquareMeter")
    if(index==-1):
        CleandSquareMeter ='?'
    else:
        CleandSquareMeter = str.split(str.split(line[index+len("CleandSquareMeter"):],":")[1],",")[0]
    #CleandPercentage
    index = str.find(line,"CleandPercentage")
    if(index==-1):
        CleandPercentage ='?'
    else:
        CleandPercentage = str.split(str.split(line[index+len("CleandPercentage"):],":")[1],",")[0]
    return SquareMeter+' '+CurrentSegmentCleandSquareMeter+' '+CleandSquareMeter+' '+CleandPercentage

def getCurrentLocation(line,i):
    #serverData - CurrentLocationDescription + tableAngles
    index = str.find(line,"CurrentLocationDescription")
    serverData = line[index+len("CurrentLocationDescription"):]
    serverData = str.split(serverData,",")
    #"0% ,dock: 1, 0%, 270�, (1.87�,-1.29�)
    index = str.find(serverData[0],'%')
    if(index==-1):
        return "? ? ? ? ? ?"
    robotCleaningPercent = str.split(serverData[0],'"')[2].strip()
    robotLocationS = str.split(serverData[1],' ')
    robotLocation = robotLocationS[0]+str.split(robotLocationS[1],'"')[0].strip()
    percentSegmentCleaned = serverData[2].strip()
    yaw = str.split(serverData[3],'ï')[0].strip()
    tableAnglesTiltNorth = str.split(serverData[4],'(')[1]
    tableAnglesTiltNorth = str.split(tableAnglesTiltNorth,'ï')[0].strip()
    tableAnglesTiltEast = str.split(serverData[5],'(')[0]
    tableAnglesTiltEast = str.split(str.split(tableAnglesTiltEast,'ï')[0],')')[0].strip()
    parsed_line = robotCleaningPercent+' '+robotLocation+' '+percentSegmentCleaned+' '+yaw+' '+tableAnglesTiltNorth+' '+tableAnglesTiltEast
    return parsed_line

def getCleaningEvent(line,i):
    #index = str.find(line,"Trigger")
    #Trigger = line[index+len("Trigger"):]#'":{"Auto":"Master"}},"Message":""}}}\n'
    #Triggerf = str.split(Trigger,":")
    #Trigger = str.split(Triggerf[1],'{')[1]+':'+str.split(Triggerf[2],'}')[0]
    #index = str.find(line,"SubCategory")
    #SubCategory = line[index+len("SubCategory"):]
    #SubCategory = str.split(str.split(SubCategory,":")[1],',')[0]
    #index = str.find(line,"Category")
    #Category = line[index+len("Category"):]
    #Category = str.split(str.split(Category,":")[1],',')[0]
    index = str.find(line,"State")
    State = line[index+len("State"):]
    State = str.split(str.split(State,":")[1],',')[0]
    index = str.find(line,"Data")
    Time = line[index+len("Data")+11:]
    Time = str.split(Time,",")[0]
    indexT = str.find(line[index:],"Event")
    Event = line[index+indexT+len("Event"):]
    Event = str.split(str.split(Event,":")[1],',')[0]
    index = str.find(line,"NewState")
    NewState = line[index+len("NewState"):]
    NewState = str.split(str.split(NewState,":")[1],',')[0]
    return NewState+' '+Event +' '+Time +' '+State

def getrobotCleaningFlags(line,i):
    #IsClockUpdated
    index = str.find(line,"IsClockUpdated")
    IsClockUpdated = str.split(str.split(line[index+len("IsClockUpdated"):],":")[1],",")[0].strip()
    #ErrorOutOfBase
    index = str.find(line,"ErrorOutOfBase")
    ErrorOutOfBase = str.split(str.split(line[index+len("ErrorOutOfBase"):],":")[1],",")[0].strip()
    #Response
    index = str.find(line,"Response")
    Response = str.split(str.split(line[index+len("Response"):],":")[1],",")[0].strip()
    #MalfunctionsFlags
    index = str.find(line,"MalfunctionsFlags")
    MalfunctionsFlags = str.split(str.split(line[index+len("MalfunctionsFlags"):],":")[1],",")
    MalfunctionsFlagsList = MalfunctionsFlags[0]
    MalfunctionsFlags=" "
    for m in MalfunctionsFlagsList:
        MalfunctionsFlags=(MalfunctionsFlags+','+m).strip()
    #PowerUp
    index = str.find(line,"PowerUp")
    PowerUp = str.split(str.split(line[index+len("PowerUp"):],":")[1],",")[0].strip()
    #StartClean
    index = str.find(line,"StartClean")
    StartClean = str.split(str.split(line[index+len("StartClean"):],":")[1],",")[0].strip()
    #GoHomeEntered
    index = str.find(line,"GoHomeEntered")
    GoHomeEntered = str.split(str.split(line[index+len("GoHomeEntered"):],":")[1],",")[0].strip()
    #AtBase
    index = str.find(line,"AtBase")
    AtBase = str.split(str.split(str.split(line[index+len("AtBase"):],":")[1],",")[0],'}')[0].strip()
    #robotResponseCode
    index = str.find(line,"ResponseCode")
    if(index == -1):
        robotResponseCode ='?'
    else:
        robotResponseCode = str.split(str.split(line[index+len("ResponseCode"):],":")[1],",")[0].strip()
    return IsClockUpdated+' '+ErrorOutOfBase+' '+Response+' '+MalfunctionsFlags+' '+PowerUp+' '+StartClean+' '+GoHomeEntered+' '+AtBase+' '+robotResponseCode

def getRobotHeaderResponse(line,i):
    #robotCurrentSurfaceType
    line2 = str.split(line,"Header")[2]
    index = str.find(line2,"CurrentSurfaceType")
    if(index==-1):
        robotCurrentSurfaceType = '?'
    else:
        robotCurrentSurfaceType = str.split(str.split(line2[index+len("CurrentSurfaceType"):],":")[1],",")[0]
    #robotDesiredCleaningArea
    index = str.find(line,"DesiredCleaningArea")
    if(index==-1):
        robotDesiredCleaningArea = '?'
    else:
        robotDesiredCleaningArea = str.split(str.split(line[index+len("DesiredCleaningArea"):],":")[1],",")[0].split()[0]
    #robotCleanSegmentsArea
    index = str.find(line,"CleanSegmentsArea")
    if(index==-1):
        robotCleanSegmentsArea = '?'
    else:
        robotCleanSegmentsArea = str.split(str.split(line[index+len("CleanSegmentsArea"):],":")[1],",")[0].split()[0]
    #robotCleaningDistance
    index = str.find(line,"CleaningDistance")
    if(index==-1):
        robotCleaningDistance = '?'
    else:
        robotCleaningDistance = str.split(str.split(line[index+len("CleaningDistance"):],":")[1],",")[0].split()[0]
    #robotRobotProcedure
    index = str.find(line,"RobotProcedure")
    if(index==-1):
        robotRobotProcedure = '?'
    else:
        robotRobotProcedure = str.split(str.split(line[index+len("RobotProcedure"):],":")[1],",")[0].split()[0]
    #robotRobotStep
    index = str.find(line2,"RobotStep")
    if(index==-1):
        robotRobotStep = '?'
    else:
        robotRobotStep = str.split(str.split(line2[index+len("RobotStep"):],":")[1],",")[0].split()[0]
    #robotsegmentsCleaned
    index = str.find(line,"segmentsCleaned")
    if(index==-1):
        robotsegmentsCleaned = '?'
    else:
        robotsegmentsCleaned = str.split(str.split(line[index+len("segmentsCleaned"):],":")[1],",")[0].split()[0]
    #robotiterationInStep
    index = str.find(line,"iterationInStep")
    if(index==-1):
        robotiterationInStep = '?'
    else:
        robotiterationInStep = str.split(str.split(str.split(line[index+len("iterationInStep"):],":")[1],",")[0].split()[0],'}')[0]
    #robottotalIterationsInStep
    index = str.find(line,"totalIterationsInStep")
    if(index==-1):
        robottotalIterationsInStep = '?'
    else:
        sline = str.split(line[index+len("totalIterationsInStep"):],':')
        robottotalIterationsInStep = str.split(sline[1],',')[0].split()[0]
    return robotCurrentSurfaceType+' '+robotDesiredCleaningArea+' '+robotCleanSegmentsArea+' '+robotCleaningDistance+' '+robotRobotProcedure+' '+robotRobotStep+' '+robotsegmentsCleaned+' '+robotiterationInStep+' '+robottotalIterationsInStep

def getRobotHeaderResponseDetails(line,i):
    #RSSI
    index = str.find(line,"RSSI")
    if(index==-1):
        RSSI = '?'
    else:
        RSSI = str.split(str.split(line[index+len("RSSI"):],":")[1],",")[0].split()[0]
    #Direction
    index = str.find(line,"Direction")
    if(index==-1):
        Direction = '?'
    else:
        Direction = str.split(str.split(line[index+len("Direction"):],":")[1],",")[0].split()[0]
    #Roll
    index = str.find(line,"Roll")
    if(index==-1):
        Roll = '?'
    else:
        Roll = str.split(str.split(line[index+len("Roll"):],":")[1],",")[0].split()[0]
    #Pitch
    index = str.find(line,"Pitch")
    if(index==-1):
        Pitch = '?'
    else:
        Pitch = str.split(str.split(line[index+len("Pitch"):],":")[1],",")[0].split()[0]
    #Battery
    index = str.find(line,"Battery")
    if(index==-1):
        Battery = '?'
    else:
        Battery = str.split(str.split(line[index+len("Battery"):],":")[1],",")[0].split()[0]
    #Events
    index = str.find(line,"Events")
    if(index==-1):
        Events = '?'
    else:
        EventsList = str.split(str.split(str.split(str.split(line[index+len("Events"):],":")[1],"[")[1],'}')[0],']')[0]
        EventsList = str.split(EventsList,',')
        Events=" "
        for e in EventsList:
            Events=(Events+','+e).split()[0]
    return RSSI+' '+Direction+' '+Roll+' '+Pitch+' '+Battery+' '+Events


if __name__=='__main__':
    main()
