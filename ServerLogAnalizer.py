import keepAliveAnalizer

def main():
    #print("Input full path directory of logs folder:")
    #inputFolder = input()
    #print("Input type of robot: T or E")
    #rType = input()
    #print("Input site Id in master 00##, (can parse sites: 0036,0037,0038,0039,0040,0042,0046,0047,0048,0049,0050)")
    #siteID = input()
    #print("Input start date for parsing: dd-mm-yy")
    #sDate = input()
    #print("Input end date for parsing: dd-mm-yy")
    #eDate = input()
    
    #keepAliveAnalizer.logDirectoryParserToTextFiles([inputFolder,rType,siteID,sDate,eDate])
    
    keepAliveAnalizer.logDirectoryParser(["C:\\Users\\user\\Downloads\\parse\\unitLog\\parseFile","T","0021"])
  

    

if __name__=='__main__':
    main()
