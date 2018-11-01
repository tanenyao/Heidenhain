import paho.mqtt.client as mqtt
import ast
import json
import pyodbc 

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
    
    table = msg[0]

    init_column = '('
    column = '('
    for i in msg[1]:
        if i == 'KEY':
            i += '_'
        if i == 'RADIUS':
            i = 'AXIS_' + i
        init_column += i + ' float(5), '
        column += i + ', '
    init_column = init_column[:-2] + ')'
    column = column[:-2] + ')'

    cursor.execute('CREATE TABLE %s %s' % (table, init_column))
    print('CREATE TABLE %s %s' % (table, init_column))

    d = json.loads(msg[2])
    for j in d["MachineMeasurementData"]:
        vals = list(j.values())
        val = '('
        for e in vals:
            if e == None:
                val += "NULL, "
            else:
                val += e + ', '
        val = val[:-2] + ')'

        cursor.execute('INSERT INTO %s %s VALUES %s' % (table, column, val))

    cnxn.commit()
        
            

client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

client.connect("iot.eclipse.org", 1883, 60)

client.loop_forever()



