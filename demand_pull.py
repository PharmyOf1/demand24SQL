#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
# Title:      Quickly Pull Demand- MDLZ   | APS Backend
# Author:     Phil Harm
#             phillip.harm@mdlz.com  
#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

import cx_Oracle
import csv
from engine_info import o_kinect
import pandas as pd


#ASSUME OREO
#network = "OREO"

date_ranges = {
					'JAN': '01/01/2016',
					'FEB': '02/01/2016',
					'MAR': '03/01/2016',
					'APR': '04/01/2016',
					'MAY': '05/01/2016',
					'JUN': '06/01/2016',
					'JUL': '07/01/2016',
					'AUG': '08/01/2016',
					'SEP': '09/01/2016',
					'OCT': '10/01/2016',
					'NOV': '11/01/2016',
					'DEC': '12/01/2016',
					'YE' : '01/01/2017',
					}

#=-=-=-=-=-=-=-=-RAW SQL TO PULL SKU AND BY MONTH-=-=-=-=-=-=-=-=
SQL=("""SELECT EXPORT_FCST_OPTIANT.DMDUNIT,
SUM(EXPORT_FCST_OPTIANT.TOTALQTY*IMPORT_ITEM.NET_WT_CSE_QTY) 
FROM EXPORT_FCST_OPTIANT
INNER JOIN IMPORT_ITEM
ON IMPORT_ITEM.KGF_STD_ITEM_CDE=EXPORT_FCST_OPTIANT.DMDUNIT
WHERE LOWER(IMPORT_ITEM.SHORT_DESCRIPTION) 
        LIKE '%%oreo%%'
AND EXPORT_FCST_OPTIANT.STARTDATE 
    between TO_DATE('{}','MM/DD/YYYY')
    and     TO_DATE('{}','MM/DD/YYYY')
GROUP BY EXPORT_FCST_OPTIANT.DMDUNIT
ORDER BY EXPORT_FCST_OPTIANT.DMDUNIT""").format(date_ranges["MAR"],date_ranges["APR"])
#-=-=-==-=-=-=-END RAW SQL PULL BY MONTH=-=-=-=-=-=-=-=-=-=-=-=


#Connect to Database
connection = cx_Oracle.connect(o_kinect)

#Perform Query To get SKUs
cursor = connection.cursor()
cursor.execute(SQL)
x = cursor.fetchall()

#Create Pandas   
df = pd.DataFrame(x)
df.columns = ['SKU','April']

print (df.head())

cursor.close()
connection.close()

