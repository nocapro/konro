####

konro error codes. code and description list should be in separated ts file


####

add konro createdAt and updatedAt and deletedAt feature for all strategy and all file format.
if konro deletedAt used, should auto soft delete include cascading relation... oh also you can add relational cascading feature
____

update readme

####

beside single file, multi file, I also want feature per record where dir name is table name, and the files is single record row. the file naming configurable can be auto incremental integer or uuid, the extension also configurable to yaml or json. the availability configurable in-memory or on-demand


----

just add new feature a new file storage strategy called 'per-record' where each record is a separate file. This is available in both 'in-memory' and 'on-demand' modes and support JSON or YAML formats.

please create the test files and cases to verify that implemented feature in integration test

----

update readme

####

add cases regarding on-demand multi of csv and  xlsx to test/integration/Adapters/OnDemand.test.ts

####

beside yaml and json, it also do with csv and xlsx. they should only support while on-demand multi. 

####

update readme for on demand feature, csv xlsx feature

####

Must provide ID for CSV/XLSX as lastId is not persisted


####

add integration test files and cases for on-demand feature


####

data aggregation

####

primary key can also use uuid