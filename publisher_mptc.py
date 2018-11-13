"""------------------------------------------------------------------------

                        WHAT THIS SCRIPT DOES
From each .TAB file in a given directory , this script extracts 3 kinds of
information, 1) filename, 2) table headers, 3) content. The content
converts to a json-encoded string before it is sent to a mqtt-broker,
together with the filename and table header.


                            IMPORTS
re -> .search() method finds index of particular word in string
json -> .dumps() method converts dictionary to json-encoded string
os -> .listdir() returns list of entry names in the directory given by path
paho.mqtt.publish -> .single() method sends enclosed message to mqtt-broker


                            FUNCTIONS
dict(zip()) converts two lists into dictionary

------------------------------------------------------------------------"""
import paho.mqtt.publish as publish
import re, json, os

#directory = 'C:/Users/watts/Desktop/data'				# change 1
directory = '/home/pi/ShareFile'

for filename in os.listdir(directory):
    if filename.endswith(".TAB"):
        #with open(directory+'/'+filename, "r") as f:			# change 3
        with open(directory+'/'+filename, encoding="ISO-8859-1") as f:
            while True:
                line = f.readline()
                if line.startswith('#STRUCTEND'):
                    break

            ref = f.readline()
            headers = ref.split()
            headers.remove('AXIS')

            indexes = []
            for i in range(len(headers)):
                string = headers[i]
                index = re.search(r'\b'+string, ref)
                indexes.append(index.start())

            data = []
            while True:
                line = f.readline()
                if line.startswith('[END]'):
                    break
                elif line.strip():
                    vals = []
                    for e in indexes:
                        if line[e] == ' ':
                            vals.append(None)
                        else:
                            end = line.index(' ', e)
                            val = line[e:end]
                            vals.append(val)
                else:
                    continue

                dic = dict(zip(headers, vals))
                data.append(dic)

        jsonstring = {}
        jsonstring["MachineMeasurementData"] = data
        string1 = filename.split('.')[0]
        string2 = headers
        string3 = json.dumps(jsonstring)

        localhost = '172.20.115.20'
        #localhost = 'iot.eclipse.org'					# change 2

        topic = 'Heidenhain/Pi'
        payload = str([string1,string2,string3])
        publish.single(topic, payload, qos=2, retain=False, hostname=localhost,
                   port=1883, client_id="", keepalive=60, will=None, auth=None,
                   tls=None)

        print("Message Published Successfully...")

        continue
    else:
        continue
