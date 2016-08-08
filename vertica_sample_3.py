#! /usr/bin/python

import Vertica 

DB=Vertica.Vertica()

DB.execute("select user_id,user_name from users order by user_name")

res=DB.fetch()

print "Users:\n"

DB.print_result_set(res,delimiter='|',file='/home/akashg/users_list.csv')
