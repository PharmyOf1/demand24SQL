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

class alert_all(object):
	def __init__(self):
		pass

	def csv_create(self,data_frame):
		self.data_frame = data_frame
		self.data_frame.to_csv('temp.csv')
		return []

	def email(self,data_frame,name_of_ntw):
		self.data_frame = data_frame
		self.name_of_ntw = name_of_ntw


    	from email.MIMEMultipart import MIMEMultipart
    	from email.MIMEText import MIMEText
    	from email.mime.application import MIMEApplication

    	gmailUser = email_login['user']
    	gmailPassword = email_login['pw']
    	recipient = email_login['receiver']

    	msg = MIMEMultipart()
    	msg['From'] = gmailUser
    	msg['To'] = recipient
    	msg['Subject'] = ("{}: Network Monthly Demand").format('hello')
    	part = MIMEText('Please see attached file.')
    	msg.attach(part)
    	part = MIMEApplication(open('/home/pharmyof1/Desktop/Python Scripts/APSPulls/temp.csv',"rb").read())
    	part.add_header('Content-Disposition', 'attachment', filename='network.csv')
    	msg.attach(part)


    	mailServer = smtplib.SMTP('smtp.gmail.com', 587)
    	mailServer.ehlo()
    	mailServer.starttls()
    	mailServer.ehlo()
    	mailServer.login(gmailUser, gmailPassword)
    	mailServer.sendmail(gmailUser, recipient, msg.as_string())
    	mailServer.close()
		
    	print ('Email sent succesful.')

	def twitter(self,data_frame):
		pass

	def log(self,data_frame):
		pass

	def perform_all(self,data_frame):
		self.data_frame = data_frame
		self.csv_create(self.data_frame)
		self.email(self.data_frame)
		self.twitter(self.data_frame)
		self.log(self.data_frame)
		pass


if __name__=='__main__':
	db = connect_db(o_kinect)
	send_out = alert_all()
	
	oreo = db.create_df(get_SQL('oreo').statements())
	#belvita = db.create_df(get_SQL('belvita').statements())
	#chips = db.create_df(get_SQL('chips ahoy').statements())
	#ritz = db.create_df(get_SQL('ritz').statements())
	#triscuit = db.create_df(get_SQL('triscuit').statements())
	
	#send_out.csv_create(oreo)
	#send_out.email(oreo,'Oreo')


	db.close()



