rem CS 541 SQL Project 12
rem your_first_name your_last_name
rem your_purdue_email_address

@db.sql

set serveroutput on size 30000;

--PART2--
------
--1)--
------
CREATE OR REPLACE PROCEDURE sp_univ_students
AS
BEGIN
   DBMS_OUTPUT.PUT_LINE(' Standing ' || ' NumOfStudents ' || ' AvgGPA ' || 'AvgCredit ' || ' AvgCreditHours ');
   DBMS_OUTPUT.PUT_LINE('-------------------------------------------------------------------');
   
END;
/
BEGIN
   sp_univ_students;
END;
/

--@p2_ravi18.sql
@droptables.sql



