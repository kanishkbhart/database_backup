import mysql.connector
import os, time, datetime

def query():
    for lane in range(1,17):
        if not os.path.exists(f"/home/datr/Database Backup"):
            os.makedirs(f"/home/datr/Database Backup")
        if not os.path.exists(f"/home/datr/Database Backup/Lane{lane}"):
            os.makedirs(f"/home/datr/Database Backup/Lane{lane}")
        today = datetime.date.today()
        yesterday = today - datetime.timedelta(days = 1)
        print(today,yesterday)

        dataBase = mysql.connector.connect(
        host ="localhost",
        user ="vrt",
        passwd ="vrt@12345",
        database="vrt"
        )

        y1=f'tran_data_{yesterday}'
        y2=f'merged_data_{yesterday}'
        y3=f'avc_data_{yesterday}'
        print(y1)
        file1 = open(f'/home/datr/Database Backup/Lane{lane}/{y1}', 'a')
        file2 = open(f'/home/datr/Database Backup/Lane{lane}/{y2}', 'a')
        file3 = open(f'/home/datr/Database Backup/Lane{lane}/{y3}', 'a')
        
        mycursor = dataBase.cursor()

        q1=f"SELECT *  FROM `EmployeeApp_tran1` WHERE  TransactionTIme LIKE '{yesterday}%' AND lane='{lane}';"

        q2=f"SELECT * FROM `map1` WHERE Date='{yesterday}' AND LaneId='Lane{lane}';"
        print(q2)

        q3=f"SELECT * FROM `EmployeeApp_avcc_db` WHERE date<='{yesterday}' AND lane='Lane{lane}';"

        sql1=f"DELETE FROM `EmployeeApp_tran1` WHERE TransactionTIme <='{yesterday}' AND lane={lane}"

       
        sql3=f"DELETE FROM `EmployeeApp_avcc_db` WHERE date<='{yesterday}' AND lane='Lane{lane}';"
        mycursor.execute(sql1)
        print("deleted sucess", sql1)
        mycursor.execute(sql3)
        print("deleted sucess",sql3)
        print("yesterday",)
        mycursor.execute(q1)
        r1 = mycursor.fetchall()
        if len(r1)>0:
            for row in r1:
                for i in row:
                    file1.write(f"{i},")
                file1.write('\n')
            file1.close()
            print('written to ', y1)
            

        
      

        mycursor.execute(q3)
        r3 = mycursor.fetchall()
        if len(r3)>0:
            for row in r3:
                for i in row:
                    file3.write(f"{i},")
                file3.write('\n')
            file3.close()
            print('written to ', y3)
            
        
        dataBase.commit()
flag = True
while True:
    now = datetime.datetime.now()
    if str(now.time()) < '00:00:05' and flag:
        flag = False
        query()
        print(now.time(), 'backup saved.')
    elif  str(now.time()) > '00:00:10':
        flag = True

