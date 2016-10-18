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
	dbms_output.put_line(rpad(' FR', 13) || rpad(v_num_students, 12)||rpad(v_gpa, 12)|| rpad(v_credit_hours, 12));
	close c1;
	
	open c2;
    fetch c2 into v_num_students, v_gpa, v_credit_hours;
	dbms_output.put_line(rpad(' SO', 13) || rpad(v_num_students, 12)||rpad(v_gpa, 12)|| rpad(v_credit_hours, 12));
	close c2;
	
	open c3;
    fetch c3 into v_num_students, v_gpa, v_credit_hours;
	dbms_output.put_line(rpad(' JR', 13)|| rpad(v_num_students, 12)||rpad(v_gpa, 12)|| rpad(v_credit_hours, 12));
	close c3;
	
	open c4;
    fetch c4 into v_num_students, v_gpa, v_credit_hours;
	dbms_output.put_line(rpad(' SR', 13) || rpad(v_num_students, 12)||rpad(v_gpa, 12)|| rpad(v_credit_hours, 12));
	close c4;
	
	open c5;
    fetch c5 into v_num_students, v_gpa, v_credit_hours;
	dbms_output.put_line(rpad(' GR', 13) || rpad(v_num_students, 12)||rpad(v_gpa, 12)|| rpad(v_credit_hours, 12));
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
CREATE OR REPLACE PROCEDURE sp_course_registry
AS
    v_onum offerings.onum%TYPE;
	v_cname courses.cname%TYPE;
	v_day offerings.day%TYPE;
	v_snum enrolled.snum%TYPE;
	v_sname students.sname%TYPE;
	v_endtime offerings.endtime%TYPE;
	v_fid faculty.fid%TYPE;
	v_room offerings.room%TYPE;
	v_standing students.standing%TYPE;
	v_fname faculty.fname%TYPE;
    v_starttime offerings.starttime%TYPE;
	CURSOR c1 IS
    select fid, fname from faculty order by fid;
    CURSOR c2 IS
	select O.onum, C.cname, O.day, O.starttime, O.endtime, O.room from offerings O, courses C where O.fid = v_fid and O.cnum = C.cnum order by C.cname, O.onum;
	CURSOR c3 IS
	select E.snum, S.sname, S.standing from enrolled E, students S where E.onum = v_onum and E.snum = S.snum order by E.snum;
	
	BEGIN
	open c1;
	LOOP
    fetch c1 into v_fid, v_fname;
	EXIT WHEN c1%NOTFOUND;
	dbms_output.put_line('Faculty Member: '||v_fname);
	    open c2;
		LOOP
		fetch c2 into v_onum,v_cname,v_day,v_starttime,v_endtime,v_room;
		EXIT WHEN c2%NOTFOUND;
		DBMS_OUTPUT.PUT_LINE('OfferingNumber ' || ' CourseName ' || ' Day ' || ' Time ' || ' Room ');
	    DBMS_OUTPUT.PUT_LINE('-------------------------------------------------------------------');
		DBMS_OUTPUT.PUT_LINE( rpad(v_onum,5) || rpad(v_cname,5) || rpad(v_day, 5) || (v_starttime||'-'||v_endtime)|| lpad(v_room, 5));
		DBMS_OUTPUT.PUT_LINE('           ' || ' StudentID ' || ' StudentName ' || ' Standing');
	    DBMS_OUTPUT.PUT_LINE('            '||'----------------------------------------------------');
		    open c3;
			LOOP
			fetch c3 into v_snum,v_sname,v_standing;
			EXIT WHEN c3%NOTFOUND;
			DBMS_OUTPUT.PUT_LINE(lpad(v_snum,14) || lpad(v_sname, 7) ||lpad(v_standing, 7));
			end loop;
			close c3;
		end loop;
		if c2%ROWCOUNT=0 then
		 DBMS_OUTPUT.PUT_LINE('OfferingNumber ' || ' CourseName ' || ' Day ' || ' Time ' || ' Room ');
	     DBMS_OUTPUT.PUT_LINE('-------------------------------------------------------------------');   
		end if; 
		close c2;
		
END LOOP;
CLOSE c1;
END;
/
BEGIN
   sp_course_registry;
END;
/
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
	  left join 
	  (select O5.* from Offerings O5 inner join Enrolled E5 on O5.onum = E5.onum 
	   and E5.snum = my_snum ) O2				 
	ON ((O2.starttime >= O1.endtime OR O2.endtime <= O1.starttime) OR (O2.day != O1.day)) ORDER by O1.cnum, O1.onum;
	v_cnum INTEGER := -100;
	v_cnum_new INTEGER;
	v_cname courses.cname%TYPE;
CURSOR c2(my_cnum INTEGER) IS
	Select C.cname from Courses C where C.cnum = my_cnum;
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
			v_cnum_new := offer.cnum;
			OPEN c2(offer.cnum);
			FETCH c2 INTO v_cname;
			if c2%notfound then
				v_cname := 'UNDEFINED';
			end if;
			CLOSE c2;
			IF(v_cnum_new != v_cnum) THEN
				DBMS_OUTPUT.PUT_LINE(lpad('Course: ',8) || '      ' || v_cname);
				DBMS_OUTPUT.PUT_LINE(lpad('OfferingID ', 12) || ' CourseName ' || '     Day ' || '     Time ' || '            Room ');
				DBMS_OUTPUT.PUT_LINE('------------------------------------------------------------------------------------------------');
				v_cnum := v_cnum_new;
			END IF;
			DBMS_OUTPUT.PUT_LINE( lpad(offer.onum,6)|| '      ' || rpad(v_cname, 18) || rpad(offer.day, 5) || (offer.starttime||'-'||offer.endtime)|| '   ' || lpad(offer.room, 9));
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