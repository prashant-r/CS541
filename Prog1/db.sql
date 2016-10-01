--Sets the format of the DATE datatype for the current session in sqlplus to only display time in hours/minutes, with a 24-hour format
alter session set NLS_DATE_FORMAT='HH24:MI';
--Creates tables
create table students(
	snum int CHECK (snum > 0),
	sname varchar2(30) NOT NULL,
	major varchar2(25) NOT NULL,
	standing varchar2(2) NOT NULL,
	age int NOT NULL,
	gpa float NOT NULL,
	primary key(snum)
	);
create table faculty(
	fid int CHECK (fid > 0),
	fname varchar2(30) NOT NULL,
	deptid int NOT NULL,
	primary key(fid)
	);
create table courses(
	cnum int CHECK (cnum > 0),
	cname varchar2(40) NOT NULL,
	course_level varchar2(2) NOT NULL,
	credits float NOT NULL,
	primary key(cnum)
	);
create table offerings(
	onum int CHECK (onum > 0),
	cnum int NOT NULL,
	day varchar2(3) NOT NULL,
	starttime DATE NOT NULL,
	endtime DATE NOT NULL,
	room varchar2(10) NOT NULL,
	max_occupancy int NOT NULL,
	fid int NOT NULL,
	foreign key(cnum) references courses,
	foreign key(fid) references faculty,
	primary key(onum)
	);
create table enrolled(
	snum int,
	onum int,
	primary key(snum,onum),
	foreign key(snum) references students,
	foreign key(onum) references offerings(onum)
	);
	
-- following lines are a sample dataset.
-- you will need to populate more ones to see/test each query.
-- this is not the dataset that will be used for grading.

--student
INSERT INTO students VALUES(1,'Maria White','English','FR',18, 3.5);
INSERT INTO students VALUES(2,'Charles Harris','Architecture','SO',19, 4.0);
INSERT INTO students VALUES(3,'Susan Martin','Law','JR',20, 2.7);
INSERT INTO students VALUES(4,'Joseph Thompson','Computer Science','SR',22, 4.0);
--INSERT INTO students VALUES(5,'Christopher Garcia','Computer Science','GR',25, 3.5);
--faculty
INSERT INTO faculty VALUES(1,'Ivana Teach',1);
INSERT INTO faculty VALUES(2,'James Smith',1);
INSERT INTO faculty VALUES(3,'Mary Johnson',2);
--courses
INSERT INTO courses VALUES(1, 'Data Structures', 'UG', 3.0);
INSERT INTO courses VALUES(2, 'Database Systems', 'GR', 3.0);
INSERT INTO courses VALUES(3, 'Algorithms','UG', 3.0);
--offerings
INSERT INTO offerings VALUES(1, 1, 'MWF', '10:00', '10:50','R128', 5, 1);
INSERT INTO offerings VALUES(2, 1, 'TR', '13:00', '14:15', '1320 DCL', 5, 2);
INSERT INTO offerings VALUES(3, 3, 'TR', '1:00', '2:00', 'SIEL' , 2, 1);
INSERT INTO offerings VALUES(4, 3, 'TR', '10:00', '10:10', 'GL', 2, 1);
--enrolled
INSERT INTO enrolled VALUES(4,1);
INSERT INTO enrolled VALUES(3,3);
