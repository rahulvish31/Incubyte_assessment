import pandas as pd
import psycopg2

conn = psycopg2.connect(database='Incubyte',user='postgres',password='',host='127.0.0.1')


'''Reading the data from the file and converting it into the Intermediate table as defined in PDF'''
df = pd.read_csv('hospital_data.csv',sep = '|')
header_list = ['H','Customer_Name','Customer_Id','Open_Date','Last_Consulted_Date','Vaccination_Id','Dr_Name','State','Country','DOB','Is_Active']
df = df[header_list]

rename_column = {'Customer_Name':'Name','Customer_Id':'Cust_I','Open_Date':'Open_Dt','Last_Consulted_Date':'Consul_Dt',
                 'Vaccination_Id':'VAC_ID','Dr_Name':'DR_Name','Country':'County','Is_Active':'Flag'}
df = df.rename(columns=rename_column)

table_names = df['County'].unique()    #getting unique country 
curr = conn.cursor()


#The below method is used to write back all the tables with respect to each country along with data from the intermediate table
for i in range(0,len(table_names)):
    con_df = df[df['County'] == table_names[i]]
    table_names[i] = 'Table_' + table_names[i]
    curr.execute('''
        Create Table {} 
        (H TEXT NOT NULL,
            Name TEXT NOT NULL,
            Cust_I INT PRIMARY KEY NOT NULL,
            Open_Dt Date NOT NULL,
            Consul_Dt Date NOT NULL,
            Vac_Id TEXT NOT NULL,
            DR_Name TEXT NOT NULL,
            State TEXT NOT NULL,
            County TEXT NOT NULL,
            DOB Date NOT NULL,
            FLAG TEXT NOT NULL);'''.format(table_names[i]))
    conn.commit()
    if len(con_df) > 0:
        columns = '(' + ','.join(list(con_df)) + ')'
    values = ""
    for index,row in con_df.iterrows():
        values = values+"("
        for col in list(con_df):
            if isinstance(row[col],int):
                if col in ['Open_Dt','Consul_Dt','DOB']:  #converting date integer to date format yyyy-mm-dd
                    val = str(row[col])
                    if col == 'DOB':
                        val = val[len(val)-4:len(val)] + '-' + val[len(val)-6:len(val)-4] + '-' + val[:len(val)-6]
                    else:
                        val = val[0:4] + '-' + val[4:6] + '-' + val[6:8]
                    values = values + "'" + val + "'"
                else:
                    values = values + str(row[col])
            else:
                values = values + "'" +row[col] + "'"
            values = values + ","
        values = values[0:len(values)-1]
        values = values + "),"
    values = values[0:len(values)-1]
    print('INSERT INTO {} {} VALUES {}'.format(table_names[i],columns,values))
    curr.execute('INSERT INTO {} {} VALUES {}'.format(table_names[i],columns,values))
    conn.commit() 

conn.close()    





