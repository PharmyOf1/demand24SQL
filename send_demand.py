#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
# Title:      Quickly Pull Demand- MDLZ   | APS Backend
# Author:     Phil Harm
#             phillip.harm@mdlz.com  
#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

import cx_Oracle
import csv
from engine_info import o_kinect, email_login
import pandas as pd
import smtplib
from email.MIMEMultipart import MIMEMultipart
from email.MIMEText import MIMEText
from email.mime.application import MIMEApplication


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


			merged_df = frame_list[0]
			for frame in (frame_list[1:]):
				merged_df = merged_df.merge(frame,on='SKU',how='outer')

		cols = list(merged_df)
		cols.insert(0, cols.pop(cols.index('SKU')))
		merged_df = merged_df.ix[:, cols]


		return merged_df


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
					'01/01/2017', #Jan
					'02/01/2017', #Feb
					'03/01/2017', #Mar
					'04/01/2017', #Apr
					'05/01/2017', #May
					'06/01/2017', #Jun
					'07/01/2017', #Jul
					'08/01/2017', #Aug
					'09/01/2017', #Sep
					'10/01/2017', #Oct
					'11/01/2017', #Nov
					'12/01/2017', #Dec
					'01/01/2018', #YE
					]
		return list(date for date in date_ranges)


	def statements(self):
		queries = []
		dates = self.get_dates()
		
		for x in range(len(dates)-1):
			a=dates[x]
			b=dates[x+1]

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

			queries.append(SQL)
		
		return queries

class network_info(object):
	def __init__(self,frame,name='Bisc'):
		self.frame = frame
		self.name = name

		try:
			self.frame.to_csv(self.name+'.csv')
		except:
			open(self.name+'.csv','a').close()
			self.frame.to_csv(self.name+'.csv')

	def twitter(self):
		pass

	def log(self):
		print ('{} email sent.').format(self.name)

	def email(self):
		gmailUser = email_login['user']
		gmailPassword = email_login['pw']
		recipient = email_login['receiver']
		msg = MIMEMultipart()
		msg['From'] = gmailUser
		msg['To'] = recipient
		msg['Subject'] = ("{}: Monthly Demand").format(self.name)
		part = MIMEText('Please see attached file.')
		msg.attach(part)
		part = MIMEApplication(open(self.name+'.csv',"rb").read())
		part.add_header('Content-Disposition', 'attachment', filename=self.name+'.csv')
		msg.attach(part)
		mailServer = smtplib.SMTP('smtp.gmail.com', 587)
		mailServer.ehlo()
		mailServer.starttls()
		mailServer.ehlo()
		mailServer.login(gmailUser, gmailPassword)
		mailServer.sendmail(gmailUser, recipient, msg.as_string())
		mailServer.close()
		self.log()


if __name__=='__main__':
	db = connect_db(o_kinect)
	ntw = '_'
	network_list = ['oreo','chips', 'belvita', 'ritz','triscuit']

	for ntw in network_list:
		BISC_NTW = network_info(db.create_df(get_SQL(ntw.lower()).statements()),ntw.upper())
		BISC_NTW.email()

	db.close()



