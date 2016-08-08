#! /usr/bin/python

import Vertica 

'''
#Basic : Connection and executing query

DB0=Vertica.Vertica()
DB0.execute("select user_name from users")

users=DB0.fetch()
for user in users:
	print user 


# commit_at_last 
# Default value is True, and purpose is to commit all transaction at the time of closing Vertica connection 
# If set false , DB object will not commit when script will exit

'''

'''
DB=Vertica.Vertica()

query="Insert into emp1 values('Akash')"
DB.execute(query)
print "This transaction has not been commited"

'''


#ignore_error
#Default value is False, Purpose to ignore any database related error but not db connection related
#if ignore error is set true, DB object will ignore sql error then move to next statement in script

'''
DB1=Vertica.Vertica()

#error in sql 
query="select * from xyz"
DB1.execute(query)

print "Python: I can not show my face to world, my past statements are error prone :("
'''

'''

DB2=Vertica.Vertica(ignore_error=True)
query="select * from xyz"
DB2.execute(query)

print "Python: I can show my face to world even my past statements are error prone :D"

'''

#autocommit
#Default False
#If set True , Vertica object will commit after each query execution 

DB3=Vertica.Vertica(autocommit=True,commit_at_last=False)

query="Insert into emp1 values('Akash')"
DB3.execute(query)

query="Insert into emp1 values('Pranjal')"
DB3.execute(query)

