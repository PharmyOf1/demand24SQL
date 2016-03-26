#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
# Title:      Quickly Pull Demand- MDLZ   | APS Backend
# Author:     Phil Harm
#             phillip.harm@mdlz.com  
#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

import cx_Oracle
import csv
from engine_info import o_kinect
import pandas as pd

class connect_db(object):
	def __init__(self,connect_info):
		self.connect_info = connect_info
		self.connection = cx_Oracle.connect(self.connect_info)
		self.cursor = self.connection.cursor()

	def exec_query(self,query):
		self.query = query
		self.cursor.execute(query)
		return self.cursor.fetchall()

	def close(self):
		self.cursor.close()
		self.connection.close()

	def create_df(self,query_list):
		frame_list = []
		for num,q in enumerate(query_list):
			h = pd.DataFrame(self.exec_query(q),columns=["SKU",'Month ' + str(num+1)])
			frame_list.append(h)


			current = frame_list[0]
			for frame in (frame_list[1:]):
				current = current.merge(frame,on='SKU',how='outer')

		cols = list(current)
		cols.insert(0, cols.pop(cols.index('SKU')))
		current = current.ix[:, cols]

		return current


class get_SQL(object):
	def __init__(self,network):
		self.network = network

	def get_dates(self):
		date_ranges = [
					'01/01/2016', #Jan
					'02/01/2016', #Feb
					'03/01/2016', #Mar
					'04/01/2016', #Apr
					'05/01/2016', #May
					'06/01/2016', #Jun
					'07/01/2016', #Jul
					'08/01/2016', #Aug
					'09/01/2016', #Sep
					'10/01/2016', #Oct
					'11/01/2016', #Nov
					'12/01/2016', #Dec
					'01/01/2017', #YE
					]
		return list(date for date in date_ranges)


	def statements(self):
		queries = []
		dates = self.get_dates()
		
		for x in range(12):
			a=dates[x]
			b=dates[x+1]
#=-=-=-=-=-=-=-=-RAW SQL TO PULL SKU AND BY MONTH-=-=-=-=-=-=-=-=
			SQL=("""SELECT EXPORT_FCST_OPTIANT.DMDUNIT,
				SUM(EXPORT_FCST_OPTIANT.TOTALQTY*IMPORT_ITEM.NET_WT_CSE_QTY) 
				FROM EXPORT_FCST_OPTIANT
				INNER JOIN IMPORT_ITEM
				ON IMPORT_ITEM.KGF_STD_ITEM_CDE=EXPORT_FCST_OPTIANT.DMDUNIT
				WHERE LOWER(IMPORT_ITEM.SHORT_DESCRIPTION) 
        			LIKE '%%{}%%'
				AND EXPORT_FCST_OPTIANT.STARTDATE 
    				between TO_DATE('{}','MM/DD/YYYY')
    				and     TO_DATE('{}','MM/DD/YYYY')
				GROUP BY EXPORT_FCST_OPTIANT.DMDUNIT
				ORDER BY EXPORT_FCST_OPTIANT.DMDUNIT""").format(self.network,a,b)
#-=-=-==-=-=-=-END RAW SQL PULL BY MONTH=-=-=-=-=-=-=-=-=-=-=-=
			queries.append(SQL)
		
		return queries


if __name__=='__main__':
	db = connect_db(o_kinect)
	oreo = get_SQL('oreo')
	oreo_queries = oreo.statements()
	
	oreo_dfs = db.create_df(oreo_queries)

	print (oreo_dfs.head())

	
	



	db.close()



