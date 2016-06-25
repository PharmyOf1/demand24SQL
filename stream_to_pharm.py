#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=#
# Title:      Stream Items & Fcst | MDLZ Backend -> Dashboard Server #
# Author:     Phil Harm                                              #
#             phillip.harm@mdlz.com                                  #
#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=#

import cx_Oracle
import csv
from engine_info import o_kinect, pd_kinect
import os
from sqlalchemy import create_engine, Table, MetaData, schema
from sqlalchemy.sql import table, column, select, update, insert

get_item_info = "SELECT * FROM IMPORT_ITEM ORDER BY KGF_STD_ITEM_CDE"

# get_item_info = """SELECT
# RANK() OVER (ORDER BY ITEM) as RN,
# ITEM,
# DESCR,
# BUSN_SEGM_DESC,
# RESP_CTGY_DESC,
# NFP_LD_GRP_ITM,
# NPP_LD_GRP,
# KLVL_CD,
# KLVL_DES
# FROM EXPORT_ITEM_OPTIANT"""

get_fcst = """SELECT
ROW_NUMBER() OVER(ORDER BY SKU) as RN,
SKU,
CALMONTH,
CALYEAR,
CASES,
LBS
FROM (
SELECT
    IMPORT_FCST.DMDUNIT as SKU,
    EXTRACT(month FROM IMPORT_FCST.STARTDATE) as CalMonth,
    EXTRACT(year FROM IMPORT_FCST.STARTDATE) as CalYear,
    SUM(IMPORT_FCST.QTY) AS CASES,
    SUM(IMPORT_FCST.QTY*IMPORT_ITEM.NET_WT_CSE_QTY) as LBS
   
FROM
    IMPORT_FCST
   
INNER JOIN
    IMPORT_ITEM
ON
    IMPORT_ITEM.KGF_STD_ITEM_CDE=IMPORT_FCST.DMDUNIT

GROUP BY
    IMPORT_FCST.DMDUNIT,
    EXTRACT(month FROM IMPORT_FCST.STARTDATE),
    EXTRACT(Year FROM IMPORT_FCST.STARTDATE),
    EXTRACT(Year FROM IMPORT_FCST.STARTDATE))
   
ORDER BY
RN,
CALYEAR,
CALMONTH"""

class connect_APS(object):
	def __init__(self,connect_info):
		self.connect_info = connect_info
		self.connection = cx_Oracle.connect(self.connect_info)
		self.cursor = self.connection.cursor()
		print ('***APS Connection Established***')

	def exec_query(self,query):
		self.query = query
		self.cursor.execute(query)
		return self.cursor.fetchall()

	def close(self):
		self.cursor.close()
		self.connection.close()
		print ('***APS Connection Terminated***')

class connect_PD(object):
	def __init__(self,login_info):
		self.login_info = login_info
		self.engine = create_engine(login_info)	
		self.connection = self.engine.connect()
		self.meta = MetaData()
		self.meta.reflect(bind=self.engine)
		self.sku_info = schema.Table('skulist_sku_info', self.meta, autoload=True)
		self.sku_fcst = schema.Table('skulist_sku_fcst', self.meta, autoload=True)

		print ('***PD Connection Established***')

	def item_check(self):
		items = self.sku_info.select(self.sku_info.c.KGF_STD_ITEM_CDE)
		return [x[0] for x in self.connection.execute(items)]
	
	def auto_upload(self, table_data):
		self.table_data = table_data

		if len(table_data[0])>7:
			self.item_check = self.item_check()
			for row in table_data:
				if row[0] not in self.item_check:
					self.connection.execute(self.sku_info.insert(values=row))
					print ("{} | {} uploaded").format(row[0],row[1])

		else:
			print ('hi')
			self.connection.execute(self.sku_fcst.delete())
			print ('hi2')
			for row in table_data:
				self.connection.execute(self.sku_fcst.insert(values=row))
				print ("{} | {} | {} | {} | {} uploaded").format(row[0],row[1],row[2],row[3],row[4])

	def close(self): 
		self.connection.close()
		print ('***PD Connection Terminated***')

class log(object):
	def __init__(self):
		self.list_of_items = []
		pass

if __name__=='__main__':
	#Connect VPN
	#os.system("/opt/cisco/anyconnect/bin/vpn connect connect-americas.mdlz.com")
	#os.system("Echo ''")
	#time.sleep(15)
	Log = log()	
	APS = connect_APS(o_kinect)
	PD = connect_PD(pd_kinect)
	

	#--------------------------------------------
	APS_Items = APS.exec_query(get_item_info)
	PD.auto_upload(APS_Items)
	
	APS_Fcst = APS.exec_query(get_fcst)
	PD.auto_upload(APS_Fcst)

	#---------------------------------------------

	APS.close()
	PD.close()

	#os.system("/opt/cisco/anyconnect/bin/vpn disconnect")

	####Need to add
	#Auto login to VPN

#To Do
#Add all the columns back to the django model for uploading

