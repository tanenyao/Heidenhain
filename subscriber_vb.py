"""------------------------------------------------------------------------

                        WHAT THIS SCRIPT DOES
For each byte-message this script receives, it converts the byte to string
to list. From the list, 3 kinds of information is extracted, 1) filename,
2) headers, 3) json-encoded string. A connection to a database is made,
after which the filename and headers are used to create a table. The
json-encoded string is decoded to a list of dictionaries. Values from
dictionary are extracted and then inserted as values into the table created
earlier.


                            IMPORTS
ast -> .literal_eval() method converts string-quoted list to list
json -> .loads() method converts json-encoded string to disctionary
pyodbc -> .connect() connects to specified database
paho.mqtt.client -> .Client() method sets up both the connection to
                    mqtt-broker, and the listener

------------------------------------------------------------------------"""
import paho.mqtt.client as mqtt
import ast, json, pyodbc

cnxn = pyodbc.connect('Driver={SQL Server};'
                        'Server=ST-TANEY-NB\WATTSSQL;'
                        'Database=Heidenhain;'
                        'Trusted_Connection=yes;')

cursor = cnxn.cursor()

def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))
    client.subscribe('Heidenhain/Pi', qos=2)

def on_message(client, userdata, msg):
    msg = ast.literal_eval(msg.payload.decode("UTF-8"))

    tablename = msg[0]

    columnstring1 = '('
    columnstring2 = '('

    for i in msg[1]:
        if i == 'KEY':
            i += '_'
        if i == 'RADIUS':
            i = 'AXIS_' + i
        columnstring1 += i + ' float(5), '
        columnstring2 += i + ', '

    columnstring1 = columnstring1[:-2] + ')'
    columnstring2 = columnstring2[:-2] + ')'

    cursor.execute('CREATE TABLE %s %s' % (tablename, columnstring1))


    dic = json.loads(msg[2])

    for j in dic["MachineMeasurementData"]:
        valuelist = list(j.values())
        valuestring = '('
        for k in valuelist:
            if k == None:
                valuestring += 'NULL, '
            else:
                valuestring += k + ', '

        valuestring = valuestring[:-2] + ')'

        cursor.execute('INSERT INTO %s %s VALUES %s' % (tablename, columnstring2, valuestring))

    cnxn.commit()



client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

#client.connect("iot.eclipse.org", 1883, 60)                #change 1
#client.connect("172.20.115.20", 1883, 60)
client.connect("192.168.101.2", 1883, 60)

client.loop_forever()



