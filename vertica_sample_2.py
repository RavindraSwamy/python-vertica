#! /usr/bin/python

import Vertica 

#Basic : Connection and executing query

DB=Vertica.Vertica(verbose=True)

query=""" INSERT INTO emp1
		  VALUES ('Akash','Developer',0)
      """

DB.execute(query)

DB.commit()

query=""" INSERT INTO emp1
		  VALUES ('Ayush','Developer',0)
      """

DB.execute(query)

DB.rollback()


