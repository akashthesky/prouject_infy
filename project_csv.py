
from datetime import datetime

'''choice=2
if choice==2:
    from config2 import *
else:
    from config import *'''
    

import time
import MySQLdb
import csv
import re

start = time.time()

class Start_Connection:

    def startconnection(self):
        self.db1 = MySQLdb.connect("localhost", "root", "#infy123")
        self.cursor = self.db1.cursor()
        return 1
        
   


class Create_Database():

    def __init__(self, dbname, Start_Connection_Object):
        self.dbname = dbname
        self.Start_Connection_Object = Start_Connection_Object
        
    
    def create_db(self):
        dropdb = "drop database if exists " + self.dbname + ";"
        createdb = "create database " + self.dbname + ";"
        usedb = "use " + self.dbname + ";"
        self.Start_Connection_Object.cursor.execute(dropdb)
        self.Start_Connection_Object.cursor.execute(createdb)
        self.Start_Connection_Object.cursor.execute(usedb)
        return 1
        

        
class Table_Create():
    
    def __init__(self, tablename, fname, Start_Connection_Object):
        self.tablename = tablename
        self.fname = fname
        self.rows = []
        self.res = []
        self.n = []
        self.Start_Connection_Object = Start_Connection_Object

    def tbcreate(self):
        str1 = "drop table if exists " + self.tablename + ";"
        self.Start_Connection_Object.cursor.execute(str1)
        str1 = "create table " + self.tablename + "("
        strr = ""
        for x in range(0, len(self.rows)):
            if x == (primarykey_col - 1):
                strr = strr + self.rows[x] + " " + self.res[x] + " PRIMARY KEY,"
            else:
                strr = strr + self.rows[x] + " " + self.res[x] + ","
        str1 = str1 + strr[0:len(strr) - 1] + ",Created_By varchar(20) default '"+author+"',Created_time datetime default now(),Updated_By varchar(20) default null,Updated_time datetime default null);"
        self.Start_Connection_Object.cursor.execute(str1)
        return 1
    
    def extractcol(self):
        if(choice==2):
            self.rows=col_name
        if(choice==1):
            with open(self.fname, "r") as file:
                read = csv.reader(file, delimiter=deli)
                for row in read:
                    for i in row:
                        self.rows.append(i)
                    break
        print(self.rows)
        return 1
    
    def datatype(self):
        for k in datatype_dict.keys():
            
            self.res.append(datatype_dict[k])
        
        SetNullValue_object = SetNullValue(self.res)
        SetNullValue_object.generatedict()
        return self.res
        
        # print(self.res)
    def get_index_column(self, index):
        return self.res[index]
        

class Insert_Data():

    def __init__(self, Start_Connection_Object,Table_Create_Object):
        self.Start_Connection_Object = Start_Connection_Object
        self.Table_Create_Object = Table_Create_Object

    def insertfromcsv(self):
        usedb = "use " + dbname + ";"
        
        self.Start_Connection_Object.cursor.execute(usedb)
        count = 1
        with open(filename) as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=deli)
            
            final_qry = ""
            qry = ""
            if choice==2:
                count=2
            
            insert_qry = "insert into " + tname + " values"
            for i in csv_reader:
                index = 0
                # print(i)
                # insert_qry+="("
                qry = ""
                cc=0
                res=[]
                
                if count > 1:
                    for j in i:
                        
                        if count == 1:
                            break
            
                        else:
                            flag = 0
                            cc+=1
                            mat = re.match('(\d{1,2})[/.-](\d{1,2})[/.-](\d{4})$', j)
                            bat = re.match('(\d{1,2})[/.-](\d{1,2})[/.-](\d{4}) (\d{1,2}):*', j)
                            
                            if mat is not None:
                                j = datetime.strptime(j, "%d/%m/%Y").strftime('%Y-%m-%d')
                            if bat is not None:
                                datetime_generate_obj=datetime_generate()
                                a=datetime_generate_obj.get_datetime_string(j)
                                j=str(a)
                                
                            if (j == '' or j == ' ' or j == '?'):
                                key = self.Table_Create_Object.get_index_column(index)
                                #print(key)
                                j = thisdict[key]
                                if j == 'null':
                                    flag = 1
                            
                            if flag == 1:
                                qry += "" + j + ","
                            else:
                                qry += "\'" + j + "\',"
                                        
                            index += 1
                            final_qry = "(" + qry[0:len(qry) - 1] + "\"),"
                            final_qry = final_qry[0:len(final_qry) - 3] + ",'"+author+"',now(),null,null)"
                            #print(cc)
                            ValidateError_Object=ValidateError(j,cc)
                            resultt=ValidateError_Object.valid()
                            #print(resultt)
                            res.append(resultt)
                    
                    if False in res:
                        if count==2:
                            insert_qry+="("
                            
                        else:
                            #print(final_qry,count)
                            final_qry=''
                        f = open("errorlog.log", "a")
                        stmt="Row number :" + str(count-2)+"Is not inserted \n"
                        f.write(stmt)   
                            
                        
                    else:
                        insert_qry += final_qry + ","

                if(count % batch_size == 0 and count > 0):
                    insert_qry = insert_qry[0:len(insert_qry) - 2] + ")"
                    #print(insert_qry)
                    self.Start_Connection_Object.cursor.execute(insert_qry)
                    self.Start_Connection_Object.db1.commit()
                    
                    insert_qry = "insert into " + tname + " values"
                    qry = ""
                    print("Inserting In Batches " + str(count // batch_size))
                count += 1
        insert_qry = insert_qry[0:len(insert_qry) - 2] + ")"

        self.Start_Connection_Object.cursor.execute(insert_qry)
        self.Start_Connection_Object.db1.commit()
        print("Insert Finished")
        
        self.Start_Connection_Object.cursor.close()
        #print(insert_qry)
        
        return 1

class datetime_generate:
    def get_datetime_string(self,str):
        input_datetime=str
        date_input=datetime.strptime(input_datetime[0:10], "%d.%m.%Y").strftime('%Y-%m-%d')
        date_input=date_input+"T"+input_datetime[11:19]+'.'+input_datetime[20:]
        regxf = "%Y-%m-%dT%H:%M:%S.%f"
        out = datetime.strptime(date_input, regxf)
        return out


class SetNullValue:

    def __init__(self, res):
        self.result = res
    
    def generatedict(self):
        for i in self.result:
            default_value = self.getnullvalue(i)
            thisdict[i] = default_value
        return 1

    def getnullvalue(self, datatype):
        i = datatype
        if (i[0:7] == 'varchar'):
            return 'None'
        elif (i == 'int'):
            return '0'
        elif (i[0:8] == 'datetime'):
            return 'null'
        elif (i == 'double'):
            return '0'
        elif (i[0:7] == 'DECIMAL'):
            return '0.0'
        elif (i == 'date'):
            return 'null'
        elif (i == 'time'):
            return 'null'
        else:
            return 'None'


class ValidateError:
    def __init__(self,value,index):
        self.value=value
        self.index=index
    def valid(self):
        datatype=datatype_dict[self.index]
        #print(self.value)
        if(datatype[0:8]=='datetime'):
            yy=re.match('(\d{4})[/.-](\d{2})[/.-](\d{2}) (\d{1,2})[/.-:](\d{1,2})[/.:-](\d{2}).(\d+):*',self.value)
            
            if yy is not None:
                return True
            elif self.value=='null':
                return True
            else:
                return False
        elif(datatype=='double' or datatype[0:7]=='DECIMAL' or datatype=='float'):
            yy=re.match('\d+.\d+',self.value)
            if yy is not None:
                return True
            elif self.value=='0':
                return True
            else:
                return False
        elif(datatype[-3:]=='int'):
            yy=re.match('\d+',self.value)
            if yy is not None:
                return True
            elif self.value=='0':
                return True
            else:
                return False
        elif(datatype=='date'):
            yy=re.match('(\d{4})[/.-](\d{1,2})[/.-](\d{2})$',self.value)
            if yy is not None:
                return True
            elif self.value=='null':
                return True
            else:
                return False
        elif(datatype=='time'):
            yy=re.match('(\d{2})[/.-:](\d{1,2})[/.-:](\d{2})$',self.value)
            if yy is not None:
                return True
            elif self.value=='null':
                return True
            else:
                return False
          
        elif(datatype[0:7]=='varchar' or self.value=='null'):
            return True    
                


    
print("***PROJECT MENU***\n")
print("1: CSV With Coulmn Name\n")
print("2: CSV With-Out Coulmn Name\n")
choice=int(input("Enter The Choice \n"))
if choice==1:
    from config import *
elif choice==2:
    from config2 import * 
else:
    print("Wrong Choice\n")
    exit()

f = open("errorlog.log", "w")

try:
    Start_Connection_Object = Start_Connection()  # connection started
    Start_Connection_Object.startconnection()

        
        
    Create_Database_Object = Create_Database(dbname, Start_Connection_Object)  # Connection Object is passed
    Create_Database_Object.create_db()# Database Created
        
    Table_Create_Object = Table_Create(tname, filename, Start_Connection_Object)
    Table_Create_Object.extractcol()  # Coumn Name Extracted
    Table_Create_Object.datatype()  # Data Types Predicted
    Table_Create_Object.tbcreate()  # Table Is Created Finally!! :)
        
        
    
    Insert_Data_Object = Insert_Data(Start_Connection_Object,Table_Create_Object)
    Insert_Data_Object.insertfromcsv()
except:
    f = open("errorlog.log", "a")
    

end = time.time()
print("Total time taken is : ",end - start," Seconds")
    
