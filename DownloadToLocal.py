from influxdb import InfluxDBClient
import numpy as np
import itertools 
import math
import datetime
import subprocess
import sys
import operator
from datetime import datetime 
from datetime import timezone
#Remote data base information from sili

IP = '3.136.84.223'
PORT = 8086
USER = 'sili'
PASSWORD = 'sensorweb'
DBNAME = 'floorseis'


#Local database information
Local_Database_name = 'floorseis'

#Time range
starttime = datetime(2020, 6, 4,15,40,0)
endtime = datetime(2020, 6, 4,15,41,0)




#starttime = "2020-06-04T15:40:00.000000000Z"
#endtime = "2020-06-04T15:42:00.000000000Z"


#utc_time = starttime.replace(tzinfo = timezone.utc)
timestamp = starttime.timestamp()*1000
start_str = str(int((timestamp)*1000000))

#utc_time = endtime.replace(tzinfo = timezone.utc)
timestamp = endtime.timestamp()*1000
end_str=str(int((timestamp)*1000000))

print(start_str,end_str)


client = InfluxDBClient(IP, PORT, USER, PASSWORD, DBNAME)
#plt.ion()
#myobj = plt.imshow(np.zeros((Nz,Nx)))

#query = 'SELECT "*" FROM Z WHERE "location" = \'unit1\' and time > '+start_str+' and time < '+end_str
#query = 'SELECT "value" FROM Z WHERE "location" = \'unit1\' and time > '+start_str+' and time < '+end_str

#get series

query = 'show series'
result = client.query(query)

points = list(result.get_points())
#value =  np.append(value,np.array(list(map(operator.itemgetter('value'), points))))
print(points[0]['key'])
print(len(points))

#get and upload values

for i in range(len(points)):
    read = points[i]['key']
    sname = read.split(',')[0]
    tagname = read.split(',')[1].split('=')[0]
    tagvalue = read.split(',')[1].split('=')[1]
    print(sname,tagname,tagvalue) 
    query = 'SELECT "value" FROM '+ sname +' WHERE "'+ tagname +'" = \''+ tagvalue +'\' and time > '+start_str+' and time < '+end_str
    result = client.query(query)
    values = list(result.get_points())


    #upload loop

    count = 0
    http_post  = "curl -i -XPOST \'http://"+"localhost"+":8086/write?db="+Local_Database_name+"\' --data-binary \' "
    for row in values:
        point_time = datetime.strptime(str(row['time']), '%Y-%m-%dT%H:%M:%S.%fZ')
        #utc_time = starttime.replace(tzinfo = timezone.utc)
        timestamp = point_time.timestamp()*1000
        #print(point_time,timestamp)
        http_post  += "\n"+ sname +"," + tagname + "=" +tagvalue+ " value=" +str(row['value']) + " " + str(int((timestamp)*1000000))
        count+=1
        if count == 2000:
            http_post += "\'  &"
            subprocess.call(http_post, shell=True)
            print(http_post)
            count = 0
            http_post  = "curl -i -XPOST \'http://"+"localhost"+":8086/write?db="+Local_Database_name+"\' --data-binary \' "
    http_post += "\'  &"
    print(http_post)
    subprocess.call(http_post, shell=True)
    #print(values)

'''
while aa:
    currentDT = datetime.datetime.now()
    DT=currentDT.strftime("%Y-%m-%dT%H:%M:%S.000000000Z")
    print(DT)
    value=[]
    for b in range(pml,pml+Nx,1):

        points = list(result.get_points())
        value =  np.append(value,np.array(list(map(operator.itemgetter('value'), points))))
    length = len(points)
    value=np.reshape(value, (300,-1))



'''


