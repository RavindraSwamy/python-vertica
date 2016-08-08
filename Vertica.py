###############################################################################
#!/usr/bin/python
#Developed By: Akash Gupta (akash.gupta@hp.com)
#Description : Vertica class is a wrapper class built on top of pyodbc.
#			 : Objective is to easy the connectivity to Vertica through pyodbc
#
#Version History:
#Name			Date		Changes
#-----			------		---------------------------------------------------
#Akash Gupta	2015-06-11	Initial Creation
###############################################################################

import pyodbc
from datetime import datetime
import QueryAuditor
import GlobalVar

class Vertica(object):
	'This Vertica Class'
	
	__cnxn=None
	__cursor=None
	__auditor=None 
	
	__query=None
	__query_status_code="Success"
	__query_error_msg=None
	__query_start_ts=None
	__query_affected_rows=-1
	__query_end_ts=None
	__query_failed=False

	def __init__(self,commit_at_last=True,ignore_error=False,auditing=True,autocommit=False,verbose=False):
		
		if ( GlobalVar.DB_DRIVER is None or GlobalVar.DB_HOST is None or GlobalVar.DB_NAME is None or GlobalVar.DB_PORT is None or GlobalVar.DB_USER is None or GlobalVar.DB_PASSWORD is None ):
			print("Failed to initialize Vertica object due to missing values of Global Variable, Please set all required Global Variables under GlobalVar package.")
			exit(1)
			
		conn_str="DRIVER="+GlobalVar.DB_DRIVER+";SERVER="+GlobalVar.DB_HOST+";DATABASE="+GlobalVar.DB_NAME+";PORT="+GlobalVar.DB_PORT+";UID="+GlobalVar.DB_USER+";PWD="+GlobalVar.DB_PASSWORD
		
		self.__commit_at_last=commit_at_last
		self.__ignore_error=ignore_error
		self.__auditing=auditing
		self.__verbose=verbose

		try:
			if (autocommit):
				self.__cnxn=pyodbc.connect(conn_str,autocommit=autocommit)
				self.print_msg("Connected to Vertica, Autocommit On")
			else:
				self.__cnxn=pyodbc.connect(conn_str)
				self.print_msg("Connected to Vertica, Autocommit OFF")
			
			self.__cursor=self.__cnxn.cursor()

			self.__user_name=GlobalVar.DB_USER
			
			if ( self.__auditing ):
				self.__auditor=QueryAuditor.QueryAuditor(datetime.now(),self.__user_name)
				self.print_msg("Query Auditor ON")
			else:
				self.print_msg("Query Auditing OFF")
		except pyodbc.Error as err:
			print "Could not connect to Database due to below error:-"
			print err[1]
			exit(1)
				
	@property
	def query(self):
		return self.__query
	
	@property
	def query_status_code(self):
		return self.__query_status_code
	
	@property
	def query_error_msg(self):
		return self.__query_error_msg
	
	@property
	def query_start_ts(self):
		return self.__query_start_ts
	
	@property
	def query_end_ts(self):
		return self.__query_end_ts
	
	@property
	def query_affected_rows(self):
		return self.__query_affected_rows
	
	@property
	def query_failed(self):
		return self.__query_failed
		
	def audit_query(self,query,start_ts,end_ts,status,affected_rows,error):

		if (self.__auditing):
			self.__auditor.audit(query,start_ts,end_ts,status,affected_rows,error)
	
	def execute(self,query,ignore_error=None):

		self.__query=query
		self.__query_status_code="Success"
		self.__query_error_msg=None
		self.__query_failed=False
		self.__query_affected_rows=-1
		
		if ( ignore_error is None ):
			ignore_error= self.__ignore_error
			
		self.__query_start_ts=datetime.now()

		try:
			self.print_msg("\nExecuting Below Query:\n" + self.__query)
			self.__cursor.execute(self.__query)
		except pyodbc.Warning as warn:
			self.__query_status_code="Warning"
			self.__query_error_msg=warn[1]
			self.__query_failed=True
		except pyodbc.DataError as dataerr:
			self.__query_status_code="DataError"
			self.__query_error_msg=dataerr[1]
			self.__query_failed=True
		except pyodbc.DatabaseError as dberr:
			self.__query_status_code="DatabaseError"
			self.__query_error_msg=dberr[1]
			self.__query_failed=True
		except pyodbc.InterfaceError as intererr:
			self.__query_status_code="InterfaceError"
			self.__query_error_msg=intererr[1]
			self.__query_failed=True
		except pyodbc.OperationalError as operr:
			self.__query_status_code="OperationalError"
			self.__query_error_msg=operr[1]
			self.__query_failed=True
		except pyodbc.IntegrityError as integerr:
			self.__query_status_code="IntegrityError"
			self.__query_error_msg=integerr[1]
			self.__query_failed=True
		except pyodbc.InternalError as internerr:
			self.__query_status_code="InternalError"
			self.__query_error_msg=internerr[1]
			self.__query_failed=True
		except pyodbc.ProgrammingError as progerr:
			self.__query_status_code="ProgrammingError"
			self.__query_error_msg=progerr[1]
			self.__query_failed=True
		except pyodbc.NotSupportedError as notsuperr:
			self.__query_status_code="NotSupportedError"
			self.__query_error_msg=notsuperr[1]
			self.__query_failed=True
		except pyodbc.Error as err:
			self.__query_status_code="Error"
			self.__query_error_msg=err[1]
			self.__query_failed=True
		finally:
			self.__query_end_ts=datetime.now()
			self.__query_affected_rows=self.__cursor.rowcount
			self.audit_query(self.__query,self.__query_start_ts,self.__query_end_ts,self.__query_status_code,self.__query_affected_rows,self.__query_error_msg)
			
			if (  ( not ignore_error) and self.__query_failed ):
				print ("\nFailed: Query execution , exiting script, below is description:")
				print "Start Ts: " + str(self.__query_start_ts) +" End Ts: " + str(self.__query_end_ts) + " Execution Time: " +str(self.__query_end_ts-self.__query_start_ts) + "\nAffected Rows: " + str(self.__query_affected_rows)
				print "Error Code: " + self.__query_status_code + "\nError Message:" + self.__query_error_msg
				exit(1)
			
			self.print_msg("Start Ts: " + str(self.__query_start_ts) +" End Ts: " + str(self.__query_end_ts) + " Execution Time: " +str(self.__query_end_ts-self.__query_start_ts) + "\nAffected Rows: " + str(self.__query_affected_rows))
	
	def commit(self):

		self.__query="Commit"
		self.__query_status_code="Success"
		self.__query_error_msg=None
		self.__query_start_ts=datetime.now()
		self.__query_failed=False
		self.__query_affected_rows=None
		
		try:
			self.__cnxn.commit()
			self.print_msg("COMMIT")
		except pyodbc.Error as err:
			self.__query_status_code="CommitError"
			self.__query_error_msg=err[1]
			self.__query_failed=True
		finally:
			self.__query_end_ts=datetime.now()
			self.audit_query(self.__query,self.__query_start_ts,self.__query_end_ts,self.__query_status_code,self.__query_affected_rows,self.__query_error_msg)
			if (  ( not self.__ignore_error) and self.__query_failed ):
				print "Commit Failed, exiting script, below is Error:" + self.__query_status_code + "\nError:" + self.__query_error_msg
				exit(1)
	
	
	def print_msg(self,msg):
		if( self.__verbose ):
			print msg
	
	
	def rollback(self):
		self.__query="Rollback"
		self.__query_status_code="Success"
		self.__query_error_msg=None
		self.__query_start_ts=datetime.now()
		self.__query_failed=False
		self.__query_affected_rows=None
		
		try:
			self.__cnxn.rollback()
			self.print_msg("ROLLBACK")
		except pyodbc.Error as err:
			self.__query_status_code="RollbackError"
			self.__query_error_msg=err[1]
			self.__query_failed=True
		finally:
			self.__query_end_ts=datetime.now()
			self.audit_query(self.__query,self.__query_start_ts,self.__query_end_ts,self.__query_status_code,self.__query_affected_rows,self.__query_error_msg)
			if (  ( not self.__ignore_error) and self.__query_failed ):
				print "Rollback Failed, exiting script, below is Error:" + self.__query_status_code + "\n" + self.__query_error_msg
				exit(1)

	
	def query_desc(self):
		desc="Query:\n" + self.__query + "\n"
		desc += "Start Ts: " + str(self.__query_start_ts) +" End Ts: " + str(self.__query_end_ts) + " Execution Time: " +str(self.__query_end_ts-self.__query_start_ts) + "\nAffected Rows: " + str(self.__query_affected_rows)

		if ( self.__query_failed ):
			desc+="\nError: " + self.__query_status_code + "\n" + self.__query_error_msg
		
		return desc
	
	
	def fetch(self,limit=None):
		
		try:
			if ( limit is not None and limit > 1 ):
				result_set=self.__cursor.fetchall()
				return result_set[0:limit]
			elif ( limit == 1 or limit ==0 ):
				return self.__cursor.fetchone()
			elif ( limit < 0 ):
				result_set=self.__cursor.fetchall()
				return result_set[limit:]
			else:
				return self.__cursor.fetchall()
		except pyodbc.Error as err:
			return None
	
	def print_result_set(self,result_set=None,delimiter=',',file=None):
		
		out=""
		if (result_set is not None):
			for row in result_set:
				out += ( delimiter.join([str(col) for col in row]) + "\n" )
		else:
			out="No row found in result set"
		
		if ( file is not None):
			fobj=open(file,'w+')
			fobj.write(out)
		else:
			print out

	def __del__(self):
			
		try:
			if ( self.__commit_at_last ):
				self.__verbose=False
				self.commit()
		
			self.__cursor.close()
			self.__cnxn.close()
		except Exception:
			pass
		except AttributeError:
			pass
		except SystemExit:
			pass

			