CREATE DATABASE MyDB;
GO


USE MyDB
GO
EXEC sys.sp_cdc_enable_db
GO

USE MyDB
GO
CREATE SCHEMA inventory;
GO
CREATE TABLE inventory.customers(
   id INTEGER IDENTITY(1001,1) NOT NULL PRIMARY KEY,
   first_name VARCHAR(255) NOT NULL,
   last_name VARCHAR(255) NOT NULL,
   email VARCHAR(255) NOT NULL UNIQUE
);
GO

USE MyDB
INSERT INTO inventory.customers  (first_name, last_name, email) VALUES ('Sally', 'Thomas', 'sally.thomas@acme.com');
INSERT INTO inventory.customers  (first_name, last_name, email) VALUES ('George', 'Bailey', 'gbailey@foobar.com');
INSERT INTO inventory.customers  (first_name, last_name, email) VALUES ('Edward', 'Walker', 'ed@walker.com');
INSERT INTO inventory.customers  (first_name, last_name, email) VALUES ('Anne', 'Kretchmar', 'annek@noanswer.org');
GO


