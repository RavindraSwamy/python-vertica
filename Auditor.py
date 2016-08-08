###############################################################################
#!/usr/bin/python
#Developed By: Akash Gupta (akash.gupta@hp.com)
#Description : QueryAuditor class keep audit of query executing in DB, it write
#			 : information into DB table which can be configured in GlobalVar
#Version History:
#Name			Date		Changes
#-----			------		----------------------------------------------------
#Akash Gupta	2015-06-11	Initial Creation
###############################################################################

import pyodbc
import GlobalVar
import os 

from datetime import datetime

import __main__

class QueryAuditor:
	'This Auditor Class'
	
	__cnxn=None
	__cursor=None
	__audit_query=None
	
	job_name=None
	job_start_ts=None
	job_id=None
	job_login_nm=None
	job_db_user=None

	def __init__(self,job_start_ts,db_user=None):
		
		if ( GlobalVar.AUDITOR_DB_DRIVER is None or GlobalVar.AUDITOR_DB_HOST is None or GlobalVar.AUDITOR_DB_NAME is None or GlobalVar.AUDITOR_DB_PORT is None or GlobalVar.AUDITOR_DB_USER is None or GlobalVar.AUDITOR_DB_PASSWORD is None ):
			print "Failed to initialize Vertica object due to missing values of Auditor Global Variable, Please set all required Global Variables under GlobalVar package."
			exit(1)
			
		conn_str="DRIVER="+GlobalVar.AUDITOR_DB_DRIVER+";SERVER="+GlobalVar.AUDITOR_DB_HOST+";DATABASE="+GlobalVar.AUDITOR_DB_NAME+";PORT="+GlobalVar.AUDITOR_DB_PORT+";UID="+GlobalVar.AUDITOR_DB_USER+";PWD="+GlobalVar.AUDITOR_DB_PASSWORD

		try:
			self.__cnxn=pyodbc.connect(conn_str,autocommit=True)
			self.__cursor=self.__cnxn.cursor()
			
			self.job_name=__main__.__file__
			self.job_start_ts=job_start_ts
			self.job_id=os.getpid()
			self.job_login_nm=os.getlogin()
			self.job_db_user=db_user
			
			self.__audit_query= "INSERT INTO "+GlobalVar.AUDITOR_SCHEMA+"."+GlobalVar.AUDTIOR_TABLE+" VALUES('"+self.job_name+"','"+str(self.job_start_ts)+"',"+str(self.job_id)+",'"+self.job_login_nm+"','"+ self.job_db_user+"', " + "?,?,?,?,?,?)"
		except pyodbc.Error as err:
			print "Could not connect to Auditor Database due to below error:-"
			print err[1]
			exit(1)

	def audit(self,query,query_start_ts,query_end_ts,query_status_code,rows_affected,error_msg):

		try:
			self.__cursor.execute(self.__audit_query,query,query_start_ts,query_end_ts,query_status_code,rows_affected,error_msg)
		except pyodbc.Error as err:
			print "Failed Auditing,Found Error in Query:\n"+self.__audit_query+" Error:\n" +err[1]
			exit(1)
	
	def __del__(self):
			
		try:
			self.__cursor.close()
			self.__cnxn.close()
		except Exception:
			pass
		except AttributeError:
			pass
		except SystemExit:
			pass

			