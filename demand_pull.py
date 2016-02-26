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


#GET SKUS FIRST
#=-=-=-=-=-=-=-=-RAW SQL TO PULL SKU AND BY MONTH-=-=-=-=-=-=-=-=
SQL=("""SELECT EXPORT_FCST_OPTIANT.DMDUNIT  
FROM EXPORT_FCST_OPTIANT
INNER JOIN IMPORT_ITEM
ON IMPORT_ITEM.KGF_STD_ITEM_CDE=EXPORT_FCST_OPTIANT.DMDUNIT
WHERE LOWER(IMPORT_ITEM.SHORT_DESCRIPTION) LIKE '%%oreo%%'
GROUP BY EXPORT_FCST_OPTIANT.DMDUNIT
ORDER BY EXPORT_FCST_OPTIANT.DMDUNIT""")
#-=-=-==-=-=-=-END RAW SQL PULL BY MONTH=-=-=-=-=-=-=-=-=-=-=-=


#=-=-=-=-=-=-=-=-RAW SQL TO PULL SKU AND BY MONTH-=-=-=-=-=-=-=-=
SQL_Demand=("""SELECT 
SUM(EXPORT_FCST_OPTIANT.TOTALQTY*IMPORT_ITEM.NET_WT_CSE_QTY) 
FROM EXPORT_FCST_OPTIANT
INNER JOIN IMPORT_ITEM
ON IMPORT_ITEM.KGF_STD_ITEM_CDE=EXPORT_FCST_OPTIANT.DMDUNIT
WHERE LOWER(IMPORT_ITEM.SHORT_DESCRIPTION) 
        LIKE '%%oreo%%'
AND EXPORT_FCST_OPTIANT.STARTDATE 
    between TO_DATE('02/29/2016','MM/DD/YYYY')
    and     TO_DATE('03/28/2016','MM/DD/YYYY')
GROUP BY EXPORT_FCST_OPTIANT.DMDUNIT
ORDER BY EXPORT_FCST_OPTIANT.DMDUNIT""")
#-=-=-==-=-=-=-END RAW SQL PULL BY MONTH=-=-=-=-=-=-=-=-=-=-=-=


#Connect to Database
connection = cx_Oracle.connect(o_kinect)

#Perform Query To get SKUs
cursor = connection.cursor()
cursor.execute(SQL)

#Create Pandas   
df = pd.DataFrame(cursor.fetchall(),header='SKUs')
df.columns = ['SKU']

print (df.head())

#with open('oreo_demand.csv','wb') as dmd:
#    cursor.execute(SQL)
#    for row in cursor.fetchall():
#        #dmd.write({"%s\n"} % str(row))
#        x = (",".join(str(x) for x in row))
#        dmd.write(x + '\n')

cursor.close()
connection.close()

