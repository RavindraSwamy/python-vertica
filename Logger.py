#!/usr/bin/python

from datetime import datetime
import os
import sys
import __main__

class Logger():

	def __init__(self,dir=None,stdout=True,file=None):
		
		if ( file is None ):
			run_ts=datetime.now().strftime("%Y%m%d%H%M%S")
			self.log_file_name= __main__.__file__ +"_"+str(run_ts)+"_"+ str(os.getpid())
		else:
			self.log_file_name=file
			
		if ( dir is not None ):
			if (os.path.exists(dir)):
				self.log_dir=dir
			else:
				print  "Directory: " + dir + " either does not exists or you don't have permission" 
		else:
			self.log_dir="."
		
		
		try:
			log_file_path=self.log_dir + "/" + self.log_file_name
			self.log_file= open(log_file_path,'w')
		except IOError as err:
			print err
		
		if ( stdout ):
			sys.stdout=self.log_file
			
	
	def log(self,msg):
		
		try:
			self.log_file.write(msg)
		except IOError as err:
			print err
	
	def __del__(self):
		
		try:
			self.log_file.close()
		except Exception:
			pass
		except AttributeError:
			pass
		except SystemExit:
			pass