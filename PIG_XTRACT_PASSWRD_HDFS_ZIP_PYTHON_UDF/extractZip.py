import subprocess
import zipfile
import os


ERROR_POSITION = 58-1 #(-1) List indexed from 0
HeaderProgNo = 32-1 #(-1) List indexed from 0

def getPercent(data, ecu, pnumber, trip_log_file):
    try:
        #print 'Outside data[HeaderProgNo]={0}'.format(data[HeaderProgNo])
        #pnumber = data[HeaderProgNo]

        if ecu == 'xxx':
            if (pnumber is not None and pnumber == '51') and len(data) >= 416 and data[408] is not None and data[408] == '61' and data[409] is not None and data[409] == '61':
                if data[415] is not None and data[416] is not None:
                    if len(data[415]) < 2:
                        data[415]='0'+data[415]
                    if len(data[416]) < 2:
                        data[416]='0'+data[416]
                    return round((int(data[415] + data[416], 16) * 0.0001), 4)*100

            if len(data) >= 190 and data[182] is not None and data[182] == '61' and data[183] is not None and data[183] == '61':
                if data[189] is not None and data[190] is not None:
		    if len(data[189]) < 2:
		        data[189]='0'+data[189]
                    if len(data[190]) < 2:
                        data[190]='0'+data[190] 
                    return round((int(data[189] + data[190], 16) * 0.0001), 4)*100
	    
        if ecu == 'xx':
            if len(data) >= 473 and data[465] is not None and data[465] == '61' and data[466] is not None and data[466] == '61':
                if data[472] is not None and data[473] is not None:
		    if len(data[472]) < 2:
		        data[472]='0'+data[472]
		    if len(data[473]) < 2:
		        data[473]='0'+data[473]
                    return round((int(data[472] + data[473], 16) * 0.0001), 4)*100
    except:
        return 'ERROR: Exception while computing Percent Value...'
    return ''

def getBAR(data, ecu, pnumber, trip_log_file):
    try:
        if ecu == 'xx':
            if len(data) >= 87 and data[1] is not None and data[1] == '61' and data[2] is not None and data[2] == '1' and data[87] is not None:
	        bar = "{0:b}".format(int(data[87], 16))[:-3]
                if bar is None or bar =='':
                    return '0'
	        bar_base_two = int(bar, 2)
	        return bar_base_two
    except:
        return 'ERROR: Exception while computing BAR Value...'
    return ''

def executeHdfs(args):
    try:
        proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        output, error = proc.communicate()
        returnCode =  proc.returncode
        return returnCode, output, error
    except:
        return 111, 'ERROR:Exception in executeHdfs()', 'ERROR:Exception in executeHdfs()'    

@outputSchema("t:tuple(percent:chararray,bar:chararray)")
def getValues(ecu_id, trip_log_file, password, p_number):
    try:
	if ecu_id == 'xx':
            password = 'xx'
        if ecu_id == 'xx':
            password = 'xx'
        if ecu_id == 'xx':
            password = 'xx'
        #Verifying the file presence
        #print '\n raw_file_name_trip_log_file: ',trip_log_file
        checkFile = executeHdfs(['hdfs', 'dfs', '-test', '-e', trip_log_file])
        if checkFile[0] != 0:
            #print '\nERROR: RAW file does not exist..'
            return ('ERROR: RAW file does not exist..', 'ERROR: RAW file does not exist..')

        #Coping file locally for unzipping
        copyFile = executeHdfs(['hdfs', 'dfs', '-get', trip_log_file])
        if copyFile[0] != 0:
            #print '\nERROR: Unable to access RAW file..'
            return ('ERROR: Unable to access RAW file..', 'ERROR: Unable to access RAW file..')
        #print 'getting zf'
        zf = zipfile.ZipFile(os.path.basename(trip_log_file))
	#print 'geting hst_file'
        hst_file = zf.infolist()[0]
	#print 'getting data'
        data = zf.read(hst_file, password)
        #print 'type of data = {0}'.format(type(data))
	#print 'binary data[HeaderProgNo]={0}'.format(data[HeaderProgNo])
	#print 'getting ord'
        data = ["{0:x}".format(ord(c)) for c in data]
        #print 'data={0}'.format(data)
        #print 'ORD data[HeaderProgNo]={0}'.format(data[HeaderProgNo])
        #data = None
        #print 'Outside data[HeaderProgNo]={0}'.format(data[HeaderProgNo])
        pnumber = data[HeaderProgNo]
        if data[ERROR_POSITION] is not None and data[ERROR_POSITION] == '0':
            #print 'Calling percent() , bar()'
            bar = getBAR(data[ERROR_POSITION:], ecu_id, p_number, trip_log_file)
            if bar is None:
	        bar = '' 
            percent = getPercent(data[ERROR_POSITION:], ecu_id, p_number, trip_log_file)
	    if percent is None:
	        percent = ''
            return (str(percent), str(bar))
        else:
            return ('', '')
    except:
        return ('ERROR:Exception in getValues()', 'ERROR:Exception in getValues()')

#def main():
    #print("in main function.....!")
    """
    #TEST : ECU xx Percent based on new Program Logic..
    ecu_id='xx'
    trip_log_file='xx'
    password='xx'
    #password='xx'
    p_number='xx'
    """

    """
    #TEST : ECU xx Percent based on new Program Logic.., known passed Examle, UT.
    ecu_id='xx'
    trip_log_file='xxx'
    password='xx'
    #password='xx'
    p_number='xx'
    """

    """
    #TEST : ECU xx Percent based on new Program Logic.., known passed Examle, UT.
    ecu_id='xx'
    trip_log_file='xx'
    password='xx'
    #password='xx'
    p_number='xx'
    """

    """
    #TEST : ECU xx bar based on new Program Logic..(UT Example) --> PASSED
    ecu_id='xx'
    trip_log_file='xx'
    #password='xx'
    password='xx'
    p_number='xx'
    """
 

    #percent, bar = getValues(ecu_id, trip_log_file, password, p_number)
    #print 'bar={0} & Percent={1}'.format(bar, percent)
  
#if __name__== "__main__":
    #main()


