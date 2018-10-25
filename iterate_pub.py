''''
---------------------------------------------------
            This script does the following:
            1. reads .TAB files,
            2. formats content in JSON,
            3. publishes out in MQTT.
---------------------------------------------------
'''

import re, json, os
import paho.mqtt.publish as publish

directory = 'C:/Users/watts/Desktop/data'

for filename in os.listdir(directory):
    if filename.endswith(".TAB"):
        with open(directory+'/'+filename, 'r') as f:
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

        msg = {}
        msg["MachineMeasurementData"] = data
        msg = json.dumps(msg)

        topic = 'Heidenhain/Pi'
        localhost = '172.20.115.20'
        localhost = 'iot.eclipse.org'
        publish.single(topic, payload=msg, qos=2, retain=False, hostname=localhost,
                   port=1883, client_id="", keepalive=60, will=None, auth=None,
                   tls=None)

        print("Pubs...")

        continue
    else:
        continue


