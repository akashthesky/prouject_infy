import unittest 
import MySQLdb
import datetime 
from project_csv import*
from config2 import *
import re
 

class test(unittest.TestCase):
     
    def inc(self,x):
        return x 
     
    def test_answer1(self):
        t=Start_Connection()
        tab=t.startconnection()
        self.assertEqual(self.inc(tab),1)
        return t
        
    def test_answer2(self):
        t=Start_Connection()
        t.startconnection()
        tab=Create_Database(dbname,t)
        t1=tab.create_db()
        self.assertEqual(self.inc(t1),1)
        return tab
        
        
    def test_answer3(self):
        tt=self.test_answer1()
        tab=Create_Database(dbname,tt)
        tab.create_db()
        tc=Table_Create(tname, filename,tt)
        tc1=tc.extractcol()
        tc.datatype()
        tc.tbcreate()
        tc.get_index_column(1)
        self.assertEqual(self.inc(tc1),1)
        return tc
    
    def test_answer4(self):
        tt=self.test_answer1()
        tab=Create_Database(dbname,tt)
        tab.create_db()
        tc=Table_Create(tname, filename,tt)
        tc2=tc.datatype()
        self.assertNotEqual(self.inc(tc2),None)
        return tc2
    
        
    def test_answer5(self):
        tt=self.test_answer3()
        tc3=tt.tbcreate()
        self.assertEqual(self.inc(tc3),1)
        
    def test_answer6(self):
        tt=self.test_answer3()
        tc4=tt.get_index_column(1)
        self.assertEqual(self.inc(tc4),datatype_dict[2])
        
    def test_answer7(self):
        t=self.test_answer1()
        tab=self.test_answer2()
        tc=self.test_answer3()
        id0=Insert_Data(t,tc)
        id1=id0.insertfromcsv()
        tc3=self.test_answer8()
        self.assertEqual(self.inc(id1),1)
        
    def test_answer8(self):
        t=self.test_answer1()
        tab=self.test_answer2()
        tc=self.test_answer3()
        id0=Insert_Data(t,tc)
        id1=id0.insertfromcsv()
        tc1=ValidateError("A01",1)
        ve=tc1.valid()
        self.assertEqual(self.inc(ve),True)
        return tc1
        
    def test_answer9(self):
        td=self.test_answer4()
        sn=SetNullValue(td)
        sn1=sn.generatedict()
        sn2=sn.getnullvalue('int')
        self.assertEqual(self.inc(sn1),1)
        
    def test_answer10(self):
        td=self.test_answer4()
        sn=SetNullValue(td)
        sn1=sn.generatedict()
        sn2=sn.getnullvalue('int')
        self.assertEqual(self.inc(sn2),'0')
        
    def test_answer11(self):
        gd=datetime_generate()
        gd1=gd.get_datetime_string('27.05.2009 14:03:25:210')
        self.assertNotEqual(self.inc(gd1),None)
        
        
    
    
    
    
    
    
        

     
    
