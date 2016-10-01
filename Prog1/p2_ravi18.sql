rem CS 541 SQL Project 12
rem Prashant Ravi
rem ravi18@purdue.edu

@db.sql

set serveroutput on size 30000;
SET SERVEROUTPUT ON FORMAT TRUNCATED;
--PART2--
------
--1)--
------
CREATE OR REPLACE PROCEDURE sp_univ_students
AS
	v_standing students.standing%TYPE;
    v_num_students INTEGER;
	v_gpa students.gpa%TYPE;
    v_credit_hours courses.credits%TYPE;
	CURSOR c1 IS
    Select Count(*), COALESCE(AVG(gpaval), 0), COALESCE(AVG(counts), 0) from (Select S.snum, S.standing, S.gpa as gpaval, COALESCE(SUM(C.credits), 0) as counts from students S left join enrolled E on S.snum = E.snum left join courses C on C.cnum = E.onum group by S.snum, S.sname, S.standing, S.gpa having S.standing = 'FR');
	CURSOR c2 IS
    Select Count(*), COALESCE(AVG(gpaval), 0), COALESCE(AVG(counts), 0) from (Select S.snum, S.standing, S.gpa as gpaval, COALESCE(SUM(C.credits), 0) as counts from students S left join enrolled E on S.snum = E.snum left join courses C on C.cnum = E.onum group by S.snum, S.sname, S.standing, S.gpa having S.standing = 'SO');
	CURSOR c3 IS
    Select Count(*), COALESCE(AVG(gpaval), 0), COALESCE(AVG(counts), 0) from (Select S.snum, S.standing, S.gpa as gpaval, COALESCE(SUM(C.credits), 0) as counts from students S left join enrolled E on S.snum = E.snum left join courses C on C.cnum = E.onum group by S.snum, S.sname, S.standing, S.gpa having S.standing = 'JR');
	CURSOR c4 IS
    Select Count(*), COALESCE(AVG(gpaval), 0), COALESCE(AVG(counts), 0) from (Select S.snum, S.standing, S.gpa as gpaval, COALESCE(SUM(C.credits), 0) as counts from students S left join enrolled E on S.snum = E.snum left join courses C on C.cnum = E.onum group by S.snum, S.sname, S.standing, S.gpa having S.standing = 'SR');
	CURSOR c5 IS
    Select Count(*), COALESCE(AVG(gpaval), 0), COALESCE(AVG(counts), 0) from (Select S.snum, S.standing, S.gpa as gpaval, COALESCE(SUM(C.credits), 0) as counts from students S left join enrolled E on S.snum = E.snum left join courses C on C.cnum = E.onum group by S.snum, S.sname, S.standing, S.gpa having S.standing = 'GR');
BEGIN
	
	
	DBMS_OUTPUT.PUT_LINE(' Standing ' || ' NumOfStudents ' || ' AvgGPA ' || ' AvgCreditHours ');
	DBMS_OUTPUT.PUT_LINE('-------------------------------------------------------------------');
	open c1;
    fetch c1 into v_num_students, v_gpa, v_credit_hours;
	dbms_output.put_line(' FR             ' || rpad(v_num_students, 12)||rpad(v_gpa, 12)|| rpad(v_credit_hours, 12));
	close c1;
	
	open c2;
    fetch c2 into v_num_students, v_gpa, v_credit_hours;
	dbms_output.put_line(' SO             ' || rpad(v_num_students, 12)||rpad(v_gpa, 12)|| rpad(v_credit_hours, 12));
	close c2;
	
	open c3;
    fetch c3 into v_num_students, v_gpa, v_credit_hours;
	dbms_output.put_line(' JR             ' || rpad(v_num_students, 12)||rpad(v_gpa, 12)|| rpad(v_credit_hours, 12));
	close c3;
	
	open c4;
    fetch c4 into v_num_students, v_gpa, v_credit_hours;
	dbms_output.put_line(' SR             ' || rpad(v_num_students, 12)||rpad(v_gpa, 12)|| rpad(v_credit_hours, 12));
	close c4;
	
	open c5;
    fetch c5 into v_num_students, v_gpa, v_credit_hours;
	dbms_output.put_line(' GR             ' || rpad(v_num_students, 12)||rpad(v_gpa, 12)|| rpad(v_credit_hours, 12));
	close c5;

END;
/
BEGIN
   sp_univ_students;
END;
/
-----
--2)--
------







------
--3)--
------

CREATE OR REPLACE PROCEDURE sp_avail_course
AS
CURSOR c1(my_standing VARCHAR2, my_snum INTEGER) IS
	Select O1.* from 
	  (select O3.* from offerings O3 inner join courses C3 on C3.cnum = O3.cnum
	   where ((C3.course_level = 'UG') or (C3.course_level  = 'GR' and my_standing = 'GR')) and C3.cnum not in (Select C4.cnum from Courses C4 inner join Offerings O4 on C4.cnum = O4.cnum inner join Enrolled E4 on E4.onum = O4.onum where E4.snum = my_snum)
	   and O3.onum not in (Select ocpnum from (Select O6.onum as ocpnum, O6.max_occupancy from offerings O6 inner join enrolled E6 on O6.onum = E6.onum group by O6.onum, O6.max_occupancy having count(E6.snum) = O6.max_occupancy) beta)) O1
	  , 
	  (select O5.* from Offerings O5 inner join Enrolled E5 on O5.onum = E5.onum 
	   and E5.snum = my_snum ) O2				 
	WHERE (O2.starttime >= O1.endtime OR O2.endtime <= O1.starttime) OR (O2.day != O1.day);
BEGIN
  FOR itemA IN (
    SELECT *
    FROM students
  ) 
	LOOP
		DBMS_OUTPUT.PUT_LINE
			('Student ID: ' || itemA.snum);
		DBMS_OUTPUT.PUT_LINE
			('Student Name: ' || itemA.sname);
		DBMS_OUTPUT.PUT_LINE
			('Standing: ' || itemA.standing);
		FOR offer IN c1(itemA.standing, itemA.snum)
		LOOP
			-- process data record
			DBMS_OUTPUT.PUT_LINE('Offer_num' || offer.onum);
		END LOOP;
	END LOOP;
END;
/
BEGIN
   sp_avail_course;
END;
/


--@p2_ravi18.sql
@droptables.sql