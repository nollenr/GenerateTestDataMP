# GenerateTestDataMP


First, create a new "mpload_<tablename>" class, by copying an existing one.

cockroach_manager.py should not need to change.  this is used to connect to the database.

Change load_crdb.py to load and call the new "mpload_<tablename>" class.


