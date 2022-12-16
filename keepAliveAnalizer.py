import sys
import string
import pathlib
from pathlib import Path
import datetime
import re

def main():
    print("Input full path directory of logs folder:")
    inputFolder = input()
    print("Input type of robot: T or E")
    rType = input()
    print("Input site Id in master 00##, (can parse sites: 0036,0037,0038,0039,0040,0042,0046,0047,0048,0049,0050)")
    siteID = input()
    logDirectoryParser([inputFolder,rType,siteID])

#parses all *.log files in given director to *.txt files, and then concatinates all text files to one text file
#the name of the file will be the name of the given directory + time of parse +.txt
def logDirectoryParser(argv):
    #function takes 3 arguments: folder, type of robot, siteID
    if(len(argv)<3):
        print("not enough arguments")
        return
    ###################################
    #go over log files in directory input (argv[0]) and parse them all to txt file, if argv[1] is 'E'is E4, else T4
    entries = Path(argv[0])
    for entry in entries.iterdir():
        if(".log" in entry.name):
            if(argv[1]=='E'):
                if(argv[2] in ['0046','0036','0037','0038','0039','0040','0042','0047','0048','0050','0049']):
                    keepAliveAnalizerE4(argv[0]+"\\"+entry.name,argv[2])
                    print("parsed: "+ entry.name)
                else:
                    print("no parse for site" + argv[2])
                    return
            elif(argv[1]=='T'):
                keepAliveAnalizerT4(argv[0]+"\\"+entry.name)
            else:
                print("Start Parameters Error")
    ###################################
    #concatinate all parsed text files to one file
    filesList = list()
    for entry in entries.iterdir():
        if(".txt" in entry.name):
            if(not "PARSED" in entry.name):
                filesList.append(argv[0]+"\\"+entry.name)
    ####################################
    #create merged file
    cFile = str.split(argv[0],"\\")
    conacatFileName = argv[0]+"\\"+ cFile[len(cFile)-1]+"--"+str.replace(str(datetime.datetime.now()),":","_")+"PARSED.txt"
    #########################
    #open merge file
    print(conacatFileName)
    try:
        fp = open(conacatFileName, "w+")
    except IOError: 
           print ("Error: File does not appear to exist.")
           return
    #########################
    #merge files into one:
    if(argv[1]=='E'):
        fp.writelines("line: date: time: robot:   currentLocation: cleaningDistance Rssi: battery:\n")
    else:
        headerRow = "line date time robotName station robotTelit robotID robotResponse percentAllCleaned robotLocation percentCleanedCurrentSegment locationYaw tableTiltEast tableTiltNorth unknown powerUpBit startCleanBit goHomeEnteredBit atBaseBit\n"
        fp.writelines(headerRow)
    for file in filesList:
        with open(file) as infile:
            #fp.write(infile.read()+'\n')
            fp.writelines(infile.read())
            infile.close()
    #########################
    #close files
    fp.close()


def keepAliveAnalizerE4(inputFile,siteID):    
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
    #create output file and open it
    analizedFileName = inputFile
    analizedFileName = str.replace(analizedFileName,".log",".txt")
    analizedFileName = str.replace(analizedFileName," ","_")
    analizedFile = open(analizedFileName,"w+")
    ###########################
    i=0
    for line in lines:
        i=i+1
        parsed_line = logLineAnalizerE4(line,i,siteID)
        if(parsed_line != ""):
            analizedFile.writelines(analizedFileName+"_"+ parsed_line)
    #########################
    #closing all files
    analizedFile.close()
    fp.close()


def logLineAnalizerE4(line,i,siteID):
    parsed_line=""
    if(not re.search("UnitLog-",line)):
        return parsed_line
    ###########################################
    #split line according to where keepalive-info is
    if(siteID=='0048'):
        line1 = str.split(line,"RECIVED=")
    else:
        line1 = str.split(line,"KEEP_ALIVE,")
    if(len(line1)<2):
        return parsed_line
    ###########################################       
    sline = str.split(line1[0]," ")
    eline = str.split(line1[1]," ")
    ###########################################
    if(not re.search("OK",eline[1])):
        return parsed_line
    ###########################################
    numline = str(i)
    sdate = str.split(sline[0],"-")
    date = datetime.date(int(sdate[0]),int(sdate[1]),int(sdate[2]))
    time = str.split(sline[1],",")[0]
    #robotInfo:
    if(siteID=='0047' or siteID=='0039' or siteID=='0048'):
        robotInfo = str.split(sline[len(sline)-3],"(")[0]
    elif(siteID=='0040' or siteID=='0042'):
        if(date>datetime.date(2020,1,1)):
            robotInfo = str.split(sline[len(sline)-4]+"_"+ sline[len(sline)-3],"(")[0]
        else:
            robotInfo = sline[len(sline)-3]+"_"+ sline[len(sline)-2]
    else:
        robotInfo = sline[len(sline)-2]
    #location:
    if(siteID=='0048'):
        currentLocation = str(int(str.split(str.split(str.split(eline[3],"(")[1],"0x")[1],",")[0],16))
        cleaningDistance = str(int(str.split(str.split(eline[4],")")[0],"0x")[1],16))
    elif(siteID=='0040' or siteID=='0042'):
        location = str.split(str.split(eline[2],"(")[1],",")
        currentLocation = location[0]
        cleaningDistance = str.split(location[1],")")[0]
    else:
        location = str.split(eline[2],",")
        currentLocation = str.split(location[0],"(")[1]
        cleaningDistance = str.split(location[1],")")[0]
    #rssi:
    if(siteID=='0048'):
        rssi = int(str.split(str.split(line,"RSSI: ")[1],",")[0],16)
    else:
        if(len(eline)<7):
            return parsed_line
        rssi = int(str.split(eline[len(eline)-7],",")[0],16)
    #battery
    if(siteID=='0048'):
        battery = int(str.split(str.split(line,"Battey: ")[1]," ")[0],16)
    else:
        battery = int(str.split(eline[len(eline)-5],",")[0] + str.split(eline[len(eline)-6],",")[0],16)
    #all info from line:
    parsed_line = numline + " "+str(date) + " " +time+" "+ robotInfo + " " +currentLocation+" "+cleaningDistance+" "+str(rssi)+" "+str(battery)+"\n"
    return parsed_line

    

def keepAliveAnalizerT4(inputFile):    
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
    #create output file and open it
    analizedFileName = str.replace(inputFile,".log",".txt")
    print(analizedFileName)
    analizedFile = open(analizedFileName,"w+")
    ###########################
    i=0
    for line in lines:
        i=i+1
        parsed_line = newlogLineAnalizerT4(line,i,"Revivim")
        if(parsed_line != ""):
            analizedFile.writelines(parsed_line)
    #########################
    #closing all files
    analizedFile.close()
    fp.close()

#date time robotName station robotTelit robotID robotResponse percentAllCleaned robotLocation percentCleanedCurrentSegment locationYaw tableTiltEast tableTiltNorth unknown powerUpBit startCleanBit goHomeEnteredBit atBaseBit
def newlogLineAnalizerT4(line,i,siteName):
    parsed_line = ""
    index = str.find(line,"DEBUG")
    if(index==-1):
        return parsed_line
    
    #IF NOT UNITLOG - SKIP IT
    index = str.find(line,"UnitLog-")
    if(index==-1):
        return parsed_line

    #TIMESTAMP:
    dateStr = str.split(line,'[')[0]
    timeStamp = str.split(dateStr,' ')
    dateStr = str.split(timeStamp[0],'-')
    ###DATE:
    date = datetime.date(int(dateStr[0]),int(dateStr[1]),int(dateStr[2]))
    parsed_line += str(i)+' ' + str(date)

    dateStr = str.split(timeStamp[1],':')
    seconds = str.split(dateStr[2],',')
    ###TIME:
    time = datetime.time(int(dateStr[0]),int(dateStr[1]),int(seconds[0]),int(seconds[1]))
    parsed_line += ' ' + time.strftime("%H:%M:%S")

    #CHECK SITENAME IS CORRECT:
    index = str.find(line,siteName +':')
    if(index==-1):
        return parsed_line + '\n'

    line = str.split(line[index + len(siteName+": "):],'(')

    ###ROBOT-NAME
    robotName = line[0]
    parsed_line += ' ' + robotName

    #ROBOT-DETAILS + station
    robotDetails = str.split(line[1],')')
    ###station
    station = str.split(robotDetails[1],' ')[1]
    robotDetails = str.split(robotDetails[0],',')
    
    ###ROBOT-TELIT
    robotTelit = robotDetails[0]
    ###ROBOT-ID
    robotID = robotDetails[1]
    parsed_line += ' ' + station + ' ' +robotTelit + ' ' +robotID

    ###robot response - only KEEPALIVE
    if(str.find(line[1],"KEEP_ALIVE,")==-1):
        return ""
    robotResponse= str.split(line[1],"KEEP_ALIVE,")[1]

    #robotLocationDetails
    if(len(line)<3):
        robotResponse= str.split(robotResponse,'\n')[0]
        return parsed_line + ' ' +robotResponse + '\n'
    parsed_line += ' ' +robotResponse
    locationDetails = str.split(line[2],',')
    ###percentAllCleaned
    percentAllCleaned = locationDetails[0]
    ###robotLocation
    robotLocation = str.split(locationDetails[1],':')[0]
    ###percentCleanedCurrentSegment
    percentCleanedCurrentSegment = locationDetails[2]
    ###locationYaw
    locationYaw = locationDetails[3]
    parsed_line += ' ' + percentAllCleaned +' ' +robotLocation+' ' +percentCleanedCurrentSegment +' ' +locationYaw

    #locationDetails = str.split(line[3],'{')
    locationDetails = str.split(line[2],'{')
    bits = str.split(locationDetails[1],',')
    locationDetails = str.split(locationDetails[0],',')
    ###tableTilt
    tableTiltEast = locationDetails[0]
    tableTiltNorth= str.split(locationDetails[1],')')[0]
    unknown = str.split(locationDetails[2],')')[0]
    parsed_line += ' ' + tableTiltEast + ' ' + tableTiltNorth + ' ' + unknown
    ###bits
    powerUpBit = str.split(bits[0],':')[1]
    startCleanBit = str.split(bits[1],':')[1]
    goHomeEnteredBit = str.split(bits[2],':')[1]
    atBaseBit = str.split(str.split(bits[3],':')[1],'}')[0]
    parsed_line += ' ' + powerUpBit  + ' ' + startCleanBit  + ' ' +  goHomeEnteredBit +' ' +  atBaseBit+'\n'
    return parsed_line

        
def logLineAnalizerT4(line,i,siteName):
    parsed_line=""
    spLine = str.split(line,"UnitLog-")
    if(len(spLine)<2 or not re.findall(siteName,line) or not re.findall("DEBUG",line)):
        return parsed_line
    sline = str.split(spLine[0]," ")
    eline = str.split(spLine[1]," ")
    #DATE:
    sDate = str.split(sline[0],"-")
    date = datetime.date(int(sDate[0]),int(sDate[1]),int(sDate[2]))
    #TIME:
    sTime = str.split(sline[1],":")
    seconds = str.split(sTime[2],",")
    time = datetime.time(int(sTime[0]),int(sTime[1]),int(seconds[0]),int(str.split(seconds[1],"[")[0]))
    #RobotDetails
    RobotDetails = str.split(eline[2],"(")
    robotName = RobotDetails[0]
    robotTelit = str.split(RobotDetails[1],",")[0]
    robotId = str.split(eline[3],")")[0]
    #Location
    currentSurface = str.split(eline[5],"CurrentSurfaceType")
    #returns:
    parsed_line = date + " " + time + " " + robotName + " "+ robotTelit + " " + robotId + " "
    return parsed_line


if __name__=='__main__':
    main()
    #main(sys.argv)
    #main(["C:\\EcoppiaBarLevTools\\LogAnalizer\\keepAlive-UnitLevel\\revivim-0021",'T'])
