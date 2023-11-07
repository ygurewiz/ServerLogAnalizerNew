
from xmlrpc.client import MAXINT, MININT
import keyboard
from pathlib import Path
import sys
import csv
import os
import json
from operator import itemgetter
import datetime

     
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
    return [allData,typesFound,notParsedRecordTypes]    #typesFoundList     

def createCSVfile(outputDir,EventFileType,headers,inputDir,toAppendName): 
    theDir = str.split(inputDir,'\\')
    #create output files and open it
    if toAppendName==True:
        FileName = '{0}_{1}.csv'.format(theDir[len(theDir)-1],EventFileType)
    else:
        FileName = '{0}.csv'.format(EventFileType)    
    print(FileName)
    File = open(os.path.join(outputDir,FileName),"w+")
    writerFile = csv.DictWriter(File, fieldnames=headers,lineterminator='\n')
    writerFile.writeheader()
    return [writerFile,File]    #createCSVfile   

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
    return [allData,typesFound,notParsedRecordTypes]    #logTOcsvLines      


def getHeadersFromJson(FileTypeData):
    headers = list()
    for dType in FileTypeData:
        for k in dType:
            if k not in headers:
                headers.append(k)
    return headers      #getHeadersFromJson

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
    
    if(os.path.isdir(inputDir)):        
        outputDir = inputDir + '\\'+'Parsed'
        if not (os.path.isdir(inputDir + '\\'+'Parsed')):
           os.mkdir(outputDir)

    fileNum = 0
    entries = Path(inputDir)
    for entry in entries.iterdir():
        if(os.path.isdir(entry)):
            continue
        fileNum = fileNum+1
        theFile = open(entry,'r')
        lines = theFile.readlines()
        i=0
        for line in lines:
            i=i+1
            try:
                lineJson = json.loads(line)
            except:
                print('problem')
            res = lineAnalizeHistory(lineJson,i)
            if not res==None:
                print('jj')
        theFile.close()

    EventFilesData = {'AllKeepAliveData':sorted(keepAliveDataList, key=itemgetter('DateTime')),
                      'robotMalfunctions':sorted(robotMalfunctions, key=itemgetter('DateTime')),
                      'cleaningEvents':sorted(cleaningEvents, key=itemgetter('DateTime')),
                      'RobotFailingMessages':sorted(RobotFailingMessages, key=itemgetter('DateTime')),
                      'MasterStateMessages':sorted(MasterStateMessages, key=itemgetter('DateTime'))}
    for s in EventFilesData:
        theList = EventFilesData[s]
        headers = getHeadersFromJson(theList) 
        [writerFile,File] =createCSVfile(outputDir,s,headers,inputDir,True)
        writerFile.writerows(theList)
        File.close()

    robotsFilesData = getDataPerRobot(EventFilesData)
    for robotFData in robotsFilesData:
        FileName = outputDir+'\\'+robotFData
        #FileName = outputDir+'\\'+robotFData['AssetId']
        headers = getHeadersFromJson(EventFilesData['AllKeepAliveData']) 
        theList = list()
        for t in EventFilesData['AllKeepAliveData']:
            if t['AssetId']==robotFData:
                theList.append(t)
            
        [writerFile,File] =createCSVfile(outputDir,robotFData,headers,inputDir,False)
        writerFile.writerows(theList)
        File.close()
         #UNUSED 07/11/2023
        #data = robotFData['DATA']
        #errors = robotFData['FileNameData']
        #FileName = FileName+'_'+errors
        #FileName = FileName[:251]+'.txt'
        #theFile = open(FileName,'w')
        #theFile.writelines(data)
        #theFile.close()
        

    print('NOT_DONE:\t')
    for t in RECORDS_NOT_TODO:
        print('\t'+t)
    return #directoryMainUnitedFiles   
    
def main():
    directoryMainUnitedFiles(sys.argv)  

def getDataPerRobot(EventFilesData):
    robotsFilesData = list()
    robotsDataList = dict()
    for event in EventFilesData:
        data = EventFilesData[event]
        for d in data:
            AssetId = d['AssetId']
            if robotsDataList.get(AssetId)==None:
                robotsDataList[AssetId] = []
            robotsDataList[AssetId].append({'dataType':event,'Data':d})
    #UNUSED 07/11/2023
    #for robot in robotsDataList:
    #    [AssetId,UTC,PVersion,communicationFails,cleaningCommands,malfunctions,masterState,cleaningData,parkingData,errorOutOfBaseData] = analyseRobotData(robot,robotsDataList[robot])
    #    res = createFileDataPerRobot(AssetId,UTC,PVersion,communicationFails,cleaningCommands,malfunctions,masterState,cleaningData,parkingData,errorOutOfBaseData)
    #    robotsFilesData.append(res)
    #return robotsFilesData      #getDataPerRobot
    return robotsDataList      #getDataPerRobot

def getBits(d):     #UNUSED 07/11/2023
    bits = list()
    for c in d:
        if not c.find('eventS_')==-1:
            if not d[c]==0:
                bits.append(c)
    return bits     #getBits


def analyseRobotData(AssetId,Data):     #UNUSED 07/11/2023
    cleaningCommands = list()
    masterState = list()
    communicationFails = 0

    UTC = None
    PVersion = None
    
    malfunctions = list()

    cleaningData = {'Data':[],'communicationData':[]}
    parkingData = {'Data':[],'communicationData':[]}
    errorOutOfBaseData = {'Data':[],'communicationData':[]}
    
    
    isErroring = False
    isCleaning = False
    inParking = False

    for data in Data:
        res = dict()
        d = data['Data']
        if data['dataType']=='AllKeepAliveData':
            UTC = d['UTC']
            PVersion = d['PVersion']
            currentSurfaceNum = 'CurrentSurfaceNum'
            currentSequence = 'CurrentSequence'
            SequencesCleaned = 'SequencesCleaned'
            PanelNumber = 'PanelNumber'
            ExpectedSequences = 'ExpectedSequences'
            if d.get('CurrentSurfaceNum')==None:
                currentSurfaceNum = 'CurrentSurfaceTypeAppearnaceNum'
            if d.get('SequencesCleaned')==None:
                SequencesCleaned = 'segmentsCleaned'
            if d.get('CurrentSequence')==None:
                currentSequence = 'CleanSegmentsArea'
            if d.get('PanelNumber')==None:
                PanelNumber = 'iterationInStep'
            if d.get('ExpectedSequences')==None:
                ExpectedSequences = 'totalIterationsInStep'
            if d. get('totalIterationsInStep')==None:
                res = {'DateTime':d['DateTime'],'Command':d['Command'],'CurrentSurfaceType':d['CurrentSurfaceType'],'CurrentSurfaceNum':d[currentSurfaceNum],'DesiredCleaningArea':d['DesiredCleaningArea'],'CleanSegmentsArea':d['CleanSegmentsArea'],'CurrentSequence':d[currentSequence],'RobotProcedure':d['RobotProcedure'],'RobotStep':d['RobotStep'],'SequencesCleaned':d[SequencesCleaned],'PanelNumber':d[PanelNumber],'ExpectedSequences':None,'Direction':d['Direction'],'Pitch':d['Pitch'],'Roll':d['Roll'],'Battery':d['Battey'],'bits':getBits(d)}
            else:
                res = {'DateTime':d['DateTime'],'Command':d['Command'],'CurrentSurfaceType':d['CurrentSurfaceType'],'CurrentSurfaceNum':d[currentSurfaceNum],'DesiredCleaningArea':d['DesiredCleaningArea'],'CleanSegmentsArea':d['CleanSegmentsArea'],'CurrentSequence':d[currentSequence],'RobotProcedure':d['RobotProcedure'],'RobotStep':d['RobotStep'],'SequencesCleaned':d[SequencesCleaned],'PanelNumber':d[PanelNumber],'ExpectedSequences':d[ExpectedSequences],'Direction':d['Direction'],'Pitch':d['Pitch'],'Roll':d['Roll'],'Battery':d['Battey'],'bits':getBits(d)}
            if PVersion==None:            
                masterState.append(res)
                continue
            
            inParkingNow = (d['CurrentSurfaceType']=='surfacE_TYPE_PARKING') or (d['RobotProcedure']=='activE_COMMAND_STATE')
            isCleaningNow = (d['RobotProcedure']=='cleaninG_STATE') #and not (d['RobotStep']=='steP_PARKED')
            isReturningHomeNow = (d['RobotProcedure']=='returN_HOME_STATE')
            isPowerResetNow = (d['CurrentSurfaceType']=='surfacE_TYPE_INVALID') or (d['RobotProcedure']=='booT_UP_STATE')
            isErrorNow = (d['RobotProcedure']=='erroR_STATE') or (d['RobotProcedure']=='minimaL_OPERATING_VOLTAGE_STATE')
            
            CurrentSurfaceNum = 'CurrentSurfaceNum'
            if d.get('CurrentSurfaceNum')==None:
                CurrentSurfaceNum = 'CurrentSurfaceTypeAppearnaceNum'
            if d.get('TT_NS')==None:
                if d.get('Tilt_NS'):
                    d['TT_NS'] = d['Tilt_NS']
                else:
                    d['TT_NS'] = None
            if d.get('TT_EW')==None:
                if d.get('Tilt_EW'):
                    d['TT_EW'] = d['Tilt_EW']
                else:
                    d['TT_EW'] = None
            comRes = {'DateTime':d['DateTime'],'TT_NS':d['TT_NS'],'TT_EW':d['TT_EW'],'Station':d['Station'],'UnitToStationRssi':d['UnitToStationRssi'],'StationToUnitRssi':d['StationToUnitRssi'],'Battery':d['Battey'],'isCharging':d['eventS_BIT_SENSING_FENCE_CHARGING'],'isSensing':d['eventS_BIT_SENSING_FENCE_CONNECTED'],'SurfaceID':d[CurrentSurfaceNum]}

            if inParkingNow and not isCleaningNow:
                res['stateMachineLocation'] = 'inParkingNow'
                if isCleaning:
                    cleaningData['Data'].append(res)
                    cleaningData['communicationData'].append(comRes)
                    if not isCleaningNow:
                        isCleaning = False
                        inParking = True
                    
                elif isErroring:
                    isErroring = False
                    inParking = True
                    parkingData['Data'].append(res)
                    parkingData['communicationData'].append(comRes)
                elif inParking:
                    parkingData['Data'].append(res)
                    parkingData['communicationData'].append(comRes)
                else:
                    inParking = True
                    parkingData['Data'].append(res)
                    parkingData['communicationData'].append(comRes)
            elif isCleaningNow or isReturningHomeNow:
                if isCleaningNow:
                    res['stateMachineLocation'] = 'isCleaningNow'
                elif isReturningHomeNow:
                    res['stateMachineLocation'] = 'isReturningHomeNow'
                elif isErroring:
                    isErroring = False
                elif inParking:
                    inParking = False
                else:
                    print('t')
                isCleaning = True
                cleaningData['Data'].append(res)
                cleaningData['communicationData'].append(comRes)
            elif isPowerResetNow:
                res['stateMachineLocation'] = 'isPowerResetNow'
                if inParking:
                    parkingData['Data'].append(res)
                    parkingData['communicationData'].append(comRes)
                elif isErroring:
                    errorOutOfBaseData['Data'].append(res)
                    errorOutOfBaseData['communicationData'].append(comRes)
                elif isCleaning:
                    isCleaning = False
                    isErroring = True
                    cleaningData['Data'].append(res)
                    cleaningData['communicationData'].append(comRes)
                else:
                    inParking = True
                    parkingData['Data'].append(res)
                    parkingData['communicationData'].append(comRes)

            elif isErrorNow:
                res['stateMachineLocation'] = 'isErrorNow'
                if isCleaning:
                    isCleaning = False
                    isErroring = True
                    cleaningData['Data'].append(res)
                    cleaningData['communicationData'].append(comRes)
                elif isErroring:    
                    errorOutOfBaseData['Data'].append(res)
                    errorOutOfBaseData['communicationData'].append(comRes)
                elif inParking:
                    parkingData['Data'].append(res)
                    parkingData['communicationData'].append(comRes)
                    if isErroring:
                        isErroring = False
                else:
                    cleaningData['Data'].append(res)
                    cleaningData['communicationData'].append(comRes)
                    isErroring = True
            else:
                print('t')
            
        elif data['dataType']=='robotMalfunctions':
            malfunctions.append(d)
        elif data['dataType']=='cleaningEvents':
            if d['Event']=='startCommandRequest':
                if not d.get('cleaningSession')==None:
                    cleaningCommands.append({'DateTime':d['DateTime'],'unitParams':d['unitParams'],'cleaningSession':d['cleaningSession'],'CleanBehavior':d['CleanBehavior']})
                else:
                    cleaningCommands.append({'DateTime':d['DateTime'],'unitParams':d['unitParams'],'CleanBehavior':d['CleanBehavior']})
        elif data['dataType']=='RobotFailingMessages':
            communicationFails = communicationFails+1
        #elif data['dataType']=='MasterStateMessages':
        #    print('')                
        #else:
        #    print('')

    cleaningCommands = sorted(cleaningCommands, key=itemgetter('DateTime'))
    malfunctions = sorted(malfunctions, key=itemgetter('DateTime'))
    masterState = sorted(masterState, key=itemgetter('DateTime'))
  
    return [AssetId,UTC,PVersion,communicationFails,cleaningCommands,malfunctions,masterState,cleaningData,parkingData,errorOutOfBaseData]      

def createFileDataPerRobot(AssetId,UTC,PVersion,communicationFails,cleaningCommands,malfunctions,masterState,cleaningData,parkingData,errorOutOfBaseData):      #UNUSED 07/11/2023
    res = ''
    noCleaning  = False
    if cleaningData['Data']==[]:
        noCleaning = True
    RobotDetails = '*******\nROBOT: {0}, UTC: {1}, PVersion: {2}\n*******\n'.format(AssetId,UTC,PVersion)
    res = res + RobotDetails

    successFullComs = 0
    [communicationResParking,successFull] = analyseCommunicationData(parkingData['communicationData'],AssetId,'Parking')
    res = res + communicationResParking
    successFullComs = successFullComs+successFull
    [communicationResCleaning,successFull] = analyseCommunicationData(cleaningData['communicationData'],AssetId,'Cleaning')
    res = res + communicationResCleaning
    successFullComs = successFullComs+successFull
    [communicationResErroring,successFull] = analyseCommunicationData(errorOutOfBaseData['communicationData'],AssetId,'Erroring')
    res = res + communicationResErroring
    successFullComs = successFullComs+successFull
    [cleaningRes,errorsFound] = analyseCleaningData(AssetId,cleaningData['Data'])
    for l in cleaningRes:
        res = res+l

    resMalfunctions = analyseMalfunctions(malfunctions)
    res = res+ resMalfunctions+'\n\n'
    resCleaningCommands = analyseCommands(cleaningCommands)
    res = res+ resCleaningCommands+'\n\n'
    [resMasterState,e] = analyseMasterState(masterState)
    errors = e
    for E in errorsFound:
        if e.find(E)==-1:
            errors = errors+E
    res = res+ resMasterState+'\n\n'
    strCommunicationFails = 'COMMUNICATION_FAILS: {0}, SUCCESFULL: {1}\n'.format(communicationFails,successFullComs)
    res = res+ strCommunicationFails+'\n\n'
    #print(res)
    if noCleaning and errors=='':
        errors = 'NO_CLEAN'
    return {'AssetId':AssetId,'DATA':res,'FileNameData':errors}     #UNUSED 07/11/2023

def analyseMasterState(masterState):
    resMasterState = 'MASTER_STATE_DATA: \n'
    startTime = None
    errors = ''
    for ms in masterState:
        startTime = getTimeStamp(ms['DateTime'])
        Command = ms['Command']
        if Command=='errorOutOfBaseIdentified':
            errorBits = ''
            for b in ms['bits']:
                if b in EVENT_BITS_ERROR:
                    errorBits = errorBits+'_'+b
            errors = '{0}_{2}_CleanPercent_{1}_ErrorBits{3}'.format(ms['CurrentSurfaceType'],int(ms['CleanSegmentsArea']/max(1,ms['DesiredCleaningArea']*100)),ms['CurrentSurfaceNum'],errorBits)
        resMasterState = resMasterState+'{0}: {1}\n\t'.format(startTime,Command)   
        for i in ms:
            if i=='DateTime' or i=='Command':
                continue
            resMasterState = resMasterState+'{0}: {1},'.format(i,ms[i])
        resMasterState = resMasterState+'\n\n'
    return [resMasterState,errors]      #UNUSED 07/11/2023

def getTimeStamp(timeStamp):
    theStr = timeStamp.split(' ')
    dateList = theStr[0].split('-')
    timeList = theStr[1].split(':')
    res = datetime.datetime(int(dateList[0]),int(dateList[1]),int(dateList[2]),int(timeList[0]),int(timeList[1]),int(timeList[2]),int(timeList[3]))
    return res      #getTimeStamp

def getCleanCyclesData(cleaningData):
    cleanCycles = list()
    cleanData = list()
    cleanStartTimeStamps = list()

    cleanCycleStarted = False
    for cData in cleaningData:
        if cData['Command']=='startCommand':
            if cleanCycleStarted:
                cleanCycles.append(cleanData)
                cleanData = list()
            else:
                cleanCycleStarted = True
                cleanStartTimeStamps.append(getTimeStamp(cData['DateTime']))

        if cleanCycleStarted:
            cleanData.append(cData)
    cleanCycles.append(cleanData)
    return cleanCycles     #UNUSED 07/11/2023

def analyseCommunicationData(communicationData,AssetId,location):            #UNUSED 07/11/2023
    if communicationData==[]:
        return ['\n'+location+'_DATA: NO_DATA\n\n',0]
    successFull = 0
    stations = dict()
    surfaceIDs = list()
    cleanRes = 'COMMUNICATION_DATA:\n'
    i=0
    j=0
    maxBattery = MININT
    minBattery = MAXINT
    avgBattery = 0
    avgTT_EW = 0
    maxTT_EW = MININT
    minTT_EW = MAXINT
    avgTT_NS = 0
    maxTT_NS = MININT
    minTT_NS = MAXINT
    sensing = 0
    charging = 0
    currentSurfaceId = None

    for data in communicationData:        
        i=i+1
        j=j+1
        if currentSurfaceId==None:
            currentSurfaceId = data['SurfaceID']
        if currentSurfaceId not in surfaceIDs:
            surfaceIDs.append(currentSurfaceId)

        if data['isSensing']==1:
            sensing = sensing+1

        if data['isCharging']==1:
            charging = charging+1

        currentStation = data['Station']
        if not currentStation==None:
            successFull = successFull+1
            if stations.get(currentStation)==None:
                stations[currentStation] = {'avgUnitToStationRssi':0,
                                            'maxUnitToStationRssi':MININT,
                                            'minUnitToStationRssi':MAXINT,
                                            'avgStationToUnitRssi':0,
                                            'maxStationToUnitRssi':MININT,
                                            'minStationToUnitRssi':MAXINT,
                                            'minRssiDataU2S':None,
                                            'minRssiDataS2U':None,
                                            'numValues':0}

            stations[currentStation]['numValues'] = stations[currentStation]['numValues']+1

            UnitToStationRssi = data['UnitToStationRssi']
            if UnitToStationRssi<stations[currentStation]['minUnitToStationRssi']: 
                stations[currentStation]['minUnitToStationRssi'] = UnitToStationRssi
                stations[currentStation]['minRssiDataU2S'] = data
                 
                
            if UnitToStationRssi>stations[currentStation]['maxUnitToStationRssi']: 
                stations[currentStation]['maxUnitToStationRssi'] = UnitToStationRssi
                
            stations[currentStation]['avgUnitToStationRssi']= int((stations[currentStation]['avgUnitToStationRssi']*(stations[currentStation]['numValues']-1)+UnitToStationRssi)/stations[currentStation]['numValues'])
            
            StationToUnitRssi = data['StationToUnitRssi']
            if StationToUnitRssi<stations[currentStation]['minStationToUnitRssi']: 
                stations[currentStation]['minStationToUnitRssi'] = UnitToStationRssi
                stations[currentStation]['minRssiDataS2U'] = data
                
            if StationToUnitRssi>stations[currentStation]['maxStationToUnitRssi']: 
                stations[currentStation]['maxStationToUnitRssi'] = UnitToStationRssi
                
            stations[currentStation]['avgStationToUnitRssi']= int((stations[currentStation]['avgStationToUnitRssi']*(stations[currentStation]['numValues']-1)+StationToUnitRssi)/stations[currentStation]['numValues'])

        Battery = data['Battery']
        if Battery<minBattery: minBattery = Battery
        if Battery>maxBattery: maxBattery = Battery
        avgBattery = float((avgBattery*(i-1)+Battery)/i)

        if not data.get('TT_EW')==None:
            TT_EW = data['TT_EW']
            if TT_EW<minTT_EW: minTT_EW = TT_EW
            if TT_EW>maxTT_EW: maxTT_EW = TT_EW
            avgTT_EW = float((avgTT_EW*(j-1)+TT_EW)/j)

            TT_NS = data['TT_NS']
            if TT_NS<minTT_NS: minTT_NS = TT_NS
            if TT_NS>maxTT_NS: maxTT_NS = TT_NS
            avgTT_NS = float((avgTT_NS*(j-1)+TT_NS)/j)
        else:
            j = j-1
    cleanRes = location+'_DATA:\n'        
    cleanRes = cleanRes+'\tBATTERY_STATS: minBattery: {0}, maxBattery: {1}, avgBattery: {2}\n'.format(minBattery,maxBattery,avgBattery)
    cleanRes = cleanRes+'\tTABLE_ANGLES: \t\tmaxTT_NS: {0}, minTT_NS: {1}, avgTT_NS: {2},\n\t\t\t\tmaxTT_EW: {3}, minTT_EW: {4}, avgTT_EW: {5}\n'.format(maxTT_NS,minTT_NS,avgTT_NS,maxTT_EW,minTT_EW,avgTT_EW)
    cleanRes = cleanRes+'\tSTATIONS_DATA: \n'
    for s in stations:
        cleanRes = cleanRes+'\t\t{0}: avgUnitToStationRssi: {1}, maxUnitToStationRssi: {2}, minUnitToStationRssi: {3},\n\t\t\tavgStationToUnitRssi: {4}, maxStationToUnitRssi: {5}, minStationToUnitRssi: {6}\n'.format(s,stations[s]['avgUnitToStationRssi'],stations[s]['maxUnitToStationRssi'],stations[s]['minUnitToStationRssi'],stations[s]['avgStationToUnitRssi'],stations[s]['maxStationToUnitRssi'],stations[s]['minStationToUnitRssi'])
        cleanRes = cleanRes+'\t\t\t\tTableAnglesOnMinRssiU2S: NS:{0}, EW:{1} ,SurfaceId: {4}\n, \t\t\t\tTableAnglesOnMinRssiS2U: NS:{2}, EW:{3}, SurfaceId: {5}\n'.format(stations[s]['minRssiDataU2S']['TT_NS'],stations[s]['minRssiDataU2S']['TT_EW'],stations[s]['minRssiDataS2U']['TT_NS'],stations[s]['minRssiDataS2U']['TT_EW'],stations[s]['minRssiDataU2S']['SurfaceID'],stations[s]['minRssiDataS2U']['SurfaceID'])
    cleanRes = cleanRes+'\tSENSING_STATS: sensing: {0}, charging: {1}\n'.format(sensing,charging)
    cleanRes = cleanRes+'\tSURFACE_IDS: '
    for d in surfaceIDs:
        cleanRes = cleanRes+str(d)+'\t'
    cleanRes = cleanRes+'\n\n'
    return [cleanRes,successFull]     #UNUSED 07/11/2023


def analyseMalfunctions(malfunctions):
    res = 'MALFUNCTIONS FOUND: \n'
    if malfunctions==[]:
        res = res+'NO_ERRORS_IN_SERVER\n'
        return res
    for m in malfunctions:
        m.pop('Time')
        res = res+'\t'+json.dumps(m)+'\n'
    return res     #UNUSED 07/11/2023

def analyseCommands(cleaningCommands):
    res = 'CLEANING COMMANDS FOUND: \n'
    for c in cleaningCommands:
        unitParams = c['unitParams']
        if not unitParams.get('rowId')==None:
            rowId = unitParams['rowId']
            if rowId == 1:
                to = 'South'
            else:
                to = 'North'
            cleanCom = ''
            for m in unitParams['whereToStartAndWhatToDo']:
                cleanCom = '\t{{start: {0}, ToClean: {1}}}'.format(m['Start'],m['ToClean'])
            if not c.get('cleaningSession')==None:
                data = 'timeStamp: {0}, cleaningSession: {1}, CleanBehavior: {2}\n'.format(c['DateTime'],c['cleaningSession'],c['CleanBehavior'])
            else:
                data = 'timeStamp: {0}, CleanBehavior: {1}\n'.format(c['DateTime'],c['CleanBehavior'])
            data = data+'\tSTART_CLEAN: {0}, {1}'.format(to,cleanCom)
        else:
            if not c.get('cleaningSession')==None:
                data = 'timeStamp: {0}, cleaningSession: {1}, CleanBehavior: {2}\n'.format(c['DateTime'],c['cleaningSession'],c['CleanBehavior'])
            else:
                data = 'timeStamp: {0}, CleanBehavior: {1}\n'.format(c['DateTime'],c['CleanBehavior'])
            data = data+'\tSTART_CLEAN: {0}'.format(unitParams)
        res = res+data
    return res      #UNUSED 07/11/2023

def analyseCleaningData(AssetId,cleaningData):  #UNUSED 07/11/2023
    cycleRes = list()
    cleaningCycles = getCleaningCycles(AssetId,cleaningData)
    numCycles = 0
    ERRORS = list()
    isExpectedSequences = True
    for cycle in cleaningCycles:
        numCycles = numCycles+1
        cleanCycleTime = (cycle['endTime']-cycle['startTime']).total_seconds()
        currentRes = '\nCLEAN_CYCLE_#{0}\n startTime: {1}, endTime: {2}, cleanCycleTime: {3}\n'.format(numCycles,cycle['startTime'].strftime("%m-%d-%Y %H:%M:%S:%f"),cycle['endTime'].strftime("%m-%d-%Y %H:%M:%S:%f"),cleanCycleTime)
        DesiredCleaningArea = max(d['DesiredCleaningArea'] for d in cycle['cycleData'])
        CleanSegmentsArea = max(d['CleanSegmentsArea'] for d in cycle['cycleData'])
        SequencesCleaned = max(d['SequencesCleaned'] for d in cycle['cycleData'])
        for d in cycle['cycleData']:
            if d.get('ExpectedSequences')==None:
                isExpectedSequences = False
                break
        if(isExpectedSequences):
            ExpectedSequences = max(d['ExpectedSequences'] for d in cycle['cycleData'])
        
            if SequencesCleaned==0:
                timePerSequence = 0
            else:
                timePerSequence = cleanCycleTime/SequencesCleaned
            if CleanSegmentsArea==0:
                timePerSqMeters = 0
            else:
                timePerSqMeters = cleanCycleTime/CleanSegmentsArea
            if DesiredCleaningArea==0:
                cleanAreaPercent = 0
            else:
                cleanAreaPercent = int((CleanSegmentsArea/DesiredCleaningArea)*100)
            if ExpectedSequences==0:
                sequencesPercent = 0
            else:
                sequencesPercent = int((SequencesCleaned/ExpectedSequences)*100)
        else:
            ExpectedSequences = None
            timePerSequence = None
            timePerSqMeters = None
            cleanAreaPercent = None
            sequencesPercent = None
        lenCycle = len(cycle['cycleData'])
        currentRes = currentRes+ 'CLEAN DATA: numKA: {2}, timePerSequence: {0}s, timePerSqMeters: {1}s\n'.format(timePerSequence,timePerSqMeters,lenCycle)
        currentRes = currentRes +'DesiredCleaningArea: {0}, CleanSegmentsArea: {1}, SequencesCleaned: {2}, ExpectedSequences: {3}, cleanAreaPercent: {4}%, sequencesPercent: {5}%\n'.format(DesiredCleaningArea,CleanSegmentsArea,SequencesCleaned,ExpectedSequences,cleanAreaPercent,sequencesPercent)
        currentRes = currentRes +'Bits: {0}\n'.format(cycle['cycleData'][lenCycle-1]['bits'])
        currentStateMachineLocation = cycle['cycleData'][lenCycle-1]['stateMachineLocation']
        if (currentStateMachineLocation=='inParkingNow') or (currentStateMachineLocation=='isErrorNow') or (currentStateMachineLocation=='isPowerResetNow'):
            for e in cycle['cycleData'][lenCycle-1]['bits']:
                errors = ''
                if e in EVENT_BITS_ERROR:
                    errors = errors+'_'+e
                    ERRORS.append(e)
            if not ERRORS==[]:
                currentRes = currentRes+'CLEANING_ENDED_IN_PARKING_WITH_ERRORS: {0}\n'.format(errors)
                
            else:
                currentRes = currentRes+'CLEANING_ENDED_IN_PARKING_NO_ERRORS\n'
        elif cycle['cycleData'][lenCycle-1]['stateMachineLocation']=='isPowerResetNow':
            currentRes = currentRes+'CLEANING_ENDED_IN_POWER_RESET\n'
        elif cycle['cycleData'][lenCycle-1]['stateMachineLocation']=='isErrorNow':
            currentRes = currentRes+'CLEANING_ENDED_IN_ERROR_STATE\n'
        cycleRes.append(currentRes)
        #print(currentRes)
        
    return [cycleRes,ERRORS]        #UNUSED 07/11/2023

def getCleaningCycles(AssetId,cleaningData):        #UNUSED 07/11/2023
    currentStateMachineLocation = None
    prevStateMachineLocation = None
    cleanCycleStarted = False
    currentClean = 0
    cleaningCycles = list()
    singleCycle = {'cycleNumber':None,'cycleData':[],'startTime':None,'endTime':None}
    
    for data in cleaningData:
        currentStateMachineLocation = data['stateMachineLocation']
        if prevStateMachineLocation == None:
            if currentStateMachineLocation =='isCleaningNow' or currentStateMachineLocation =='isReturningHomeNow': 
                if not cleanCycleStarted:
                    cleanCycleStarted = True
                else:
                    print('g')
                currentClean = currentClean+1
                singleCycle['cycleNumber'] = currentClean
                singleCycle['startTime'] = getTimeStamp(data['DateTime'])
                singleCycle['cycleData'].append(data)
            elif currentStateMachineLocation == 'isErrorNow':
                currentClean = currentClean+1
                singleCycle['cycleNumber'] = currentClean
                singleCycle['startTime'] = getTimeStamp(data['DateTime'])
                singleCycle['cycleData'].append(data)
            else:
                print('WRONG CLEAN CYCLE MAPPING1')
        elif prevStateMachineLocation == 'isPowerResetNow' or prevStateMachineLocation=='isErrorNow' or prevStateMachineLocation=='inParkingNow':
            if currentStateMachineLocation =='isCleaningNow' or currentStateMachineLocation =='isReturningHomeNow':
                if not cleanCycleStarted:
                    cleanCycleStarted = True
                else:
                    print('g')
                singleCycle['cycleNumber'] = currentClean
                singleCycle['startTime'] = getTimeStamp(data['DateTime'])
                singleCycle['cycleData'].append(data)     
            else:
                print('WRONG CLEAN CYCLE MAPPING2')
        elif (prevStateMachineLocation=='isCleaningNow' and currentStateMachineLocation== 'isReturningHomeNow') or (prevStateMachineLocation=='isReturningHomeNow'and currentStateMachineLocation== 'isReturningHomeNow') or (prevStateMachineLocation=='isCleaningNow'and currentStateMachineLocation== 'isCleaningNow'):
            singleCycle['cycleData'].append(data)       
        elif (prevStateMachineLocation=='isCleaningNow' or prevStateMachineLocation=='isReturningHomeNow') and ((currentStateMachineLocation=='inParkingNow') or (currentStateMachineLocation=='isPowerResetNow') or (currentStateMachineLocation=='isErrorNow')):
             if cleanCycleStarted==True:
                 singleCycle['cycleData'].append(data)
                 singleCycle['endTime'] = getTimeStamp(data['DateTime'])
                 cleaningCycles.append(singleCycle)
                 currentClean = currentClean+1
                 singleCycle = {'cycleNumber':currentClean,'cycleData':[],'startTime':None,'endTime':None}
                 cleanCycleStarted = False
                 prevStateMachineLocation = currentStateMachineLocation
                 continue
             else:
                 print('WRONG CLEAN CYCLE MAPPING23')
        else:
            print('WRONG CLEAN CYCLE MAPPING4')
        if cleanCycleStarted==False and singleCycle['cycleNumber']==0:
            print('WRONG CLEAN CYCLE MAPPING5')
        prevStateMachineLocation = currentStateMachineLocation
    return cleaningCycles       #getCleaningCycles


RECORDS_NOT_TODO = ['CommunicationCycle','StationMessage','DisablerState','GradualCleanState','DisablerEvent','SchedulerState','UserMessage','SMSEvent','RobotInvalidProtocolMessage']
RECORDS_PARSED_SEPARATELY = ['RobotSuccessfulMessage','RobotMalfunction','CleaningEvent','RobotFailingMessage','MasterState']

def cutUnwantedChars(dataStr):
    res = ''
    wantedDataChars = ['0','1','2','3','4','5','6','7','8','9','-','.']
    for s in dataStr:
        if s in wantedDataChars:
            res = res+s
    return float(res)      #cutUnwantedChars

def getRobotLocation(CurrentLocationDescription):
    return {'CurrentLocationDescription':CurrentLocationDescription}

EVENT_BITS_STATUS = ['eventS_BIT_CHANGE_PARKING',
                     'eventS_BIT_AT_BASE',
                     'eventS_BIT_POWER_UP',
                     'eventS_BIT_BRUSH_DRY_ROTATION',
                     'eventS_BIT_MAGNET_DOWN']

EVENT_BITS_CLEANING = ['eventS_BIT_PARKING_ENTERED',
                       'eventS_BIT_START_CLEAN',
                       'eventS_BIT_GO_HOME_ENTERED',
                       'eventS_BIT_ABORT_GO_HOME',
                       'eventS_BIT_SEGMENT_CLEANING_ENDED']

EVENT_BITS_ERROR = ['eventS_BIT_LOW_BATTERY',
                    'eventS_BIT_PARKING_ERROR',
                    'eventS_BIT_MEASUREMENT_ERROR',
                    'eventS_BIT_EDGE_SENSOR_ERROR',
                    'eventS_BIT_TABLE_ANGLE_ERROR',
                    'eventS_BIT_TIME_OUT',
                    'eventS_BIT_DEFAULT_CONFIGURATION',
                    'eventS_BIT_SYSTEM_CALL_FAILURE',
                    'eventS_BIT_MINIMAL_OPERATING_VOLTAGE',
                    'eventS_BIT_INVALID_FSM_SCALE_FACTOR',
                    'eventS_BIT_NO_UPDATED_CLOCK',
                    'eventS_BIT_LOSS_OF_ORIENTATION',
                    'eventS_BIT_HIGH_ENCODER_DIFF',
                    'eventS_BIT_STEP_DETECTED',
                    'eventS_BIT_FAULTY_FSM',
                    'eventS_BIT_FAULTY_ENGINE',
                    'eventS_BIT_TILT_ABOVE_MAXIMUM_AVOID_CLEANING',
                    'eventS_BIT_MEASURED_LENGTH_INCOSISTENT_WITH_MAP_LENGTH',
                    'eventS_BIT_CALIBRATION_ERROR',
                    'eventS_BIT_BRIDGE_CROSS_FAILURE']

EVENT_BITS_SENSING = ['eventS_BIT_SENSING_FENCE_CONNECTED',
                      'eventS_BIT_SENSING_FENCE_CHARGING']

def getEventsList(res,eventsList):
#    for e in EVENT_BITS_STATUS:
#        if e in eventsList: 
#            res[e] = 1 
#        else:
#            res[e] = 0
#    for e in EVENT_BITS_CLEANING:
#        if e in eventsList: 
#            res[e] = 1 
#        else:
#            res[e] = 0
#    for e in EVENT_BITS_ERROR:
#        if e in eventsList: 
#            res[e] = 1 
#        else:
#            res[e] = 0
#    for e in EVENT_BITS_SENSING:
#        if e in eventsList: 
#            res[e] = 1 
#        else:
#            res[e] = 0
    for e in eventsList:
        res[e] = 1
    return res          #getEventsList

def getkeepAliveData(Data,AssetId,DateTime,UTC,isCleaningEvent):
    if not isCleaningEvent:
        CurrentLocationDescription = getRobotLocation(Data['Header']['CurrentLocationDescription'])
        res = {'DateTime':DateTime,'AssetId':AssetId,'UTC':UTC,'PVersion':Data['PVersion'],'Station':Data['Station'],'UnitToStationRssi':Data['UnitToStationRssi'],'StationToUnitRssi':Data['Header']['StationToUnitRssi'],'Command':Data['Command']['Command'],'Header':Data['RobotMessage']['Header'],'CurrentLocationDescription':CurrentLocationDescription}
        res = createDict(res['Header'],res,'')
        res.pop('Header')
        res = getEventsList(res,Data['RobotMessage']['Header']['Events'])
        res.pop('Events')
        res = createDict(res['CurrentLocationDescription'],res,'')
        res.pop('CurrentLocationDescription')
    else:
        res = {'DateTime':DateTime,'AssetId':AssetId,'UTC':UTC,'PVersion':None,'Station':None,'UnitToStationRssi':None,'StationToUnitRssi':None,'Command':Data['Event'],'Header':Data['Details']['Header']}
        res = createDict(res['Header'],res,'')
        res.pop('Header')
        res = getEventsList(res,Data['Details']['Header']['Events'])
        res.pop('Events')
        res['TT_NS'] = None
        res['TT_EW'] = None
        res['Direction'] = res['Direction']/100
    return res      #getkeepAliveData

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
    timeS = timeUTC[0].split('.')
    UTC = '+'+timeUTC[1]
    DateTime = '{0} {1}:{2}'.format(date,timeS[0],timeS[1][:3])
    res = {'DateTime':DateTime,'AssetId':AssetId,'UTC':UTC}

    Data = lineJson['Data']

    res = createDict(Data,res,'')

    if RecordType=='RobotSuccessfulMessage':
        keepAliveDataList.append(getkeepAliveData(Data,AssetId,DateTime,UTC,False))

    elif RecordType=='CleaningEvent':
        event = Data['Event']
        if type(Data['Details'])==dict and not Data['Details'].get('Header')==None:
            for KA in keepAliveDataList:
                if KA['AssetId']==AssetId:
                    if DateTime == KA['DateTime']:
                        currentKA = getkeepAliveData(Data,AssetId,DateTime,UTC,True)
                        if currentKA not in keepAliveDataList:
                            keepAliveDataList.append(currentKA)

        if event=='cleaningComplete':
            res['Reason'] = Data['Details']['Reason']
            if not Data['Details'].get('Details')==None:
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
            if not res.get('cleaningSession')==None:
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
        res.pop('Time')
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
        return [RecordType,res]     #lineAnalizeHistory

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
    return res      #createDict



if __name__=='__main__':
    main()
    print('\nDone. press key to exit')
    keyboard.read_key()
