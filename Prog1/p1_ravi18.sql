rem CS541 SQL Project 11
rem Prashant Ravi
rem ravi18@purdue.edu
@db.sql
-------
--Part 1-
----
--1)-
----
Select S.sname, S.gpa from Students S inner join Enrolled E on S.snum = E.snum GROUP BY S.snum,S.sname,S.gpa having count(*) = 1;


----
--2)-
----
SELECT C.cname from Courses C inner join Offerings O on C.cnum = O.cnum WHERE EXISTS ( SELECT NULL FROM Enrolled E inner join Students S  on S.snum = E.snum WHERE E.onum = O.onum group by E.onum having count(distinct S.major) = 1);

----
--3)-
----
SELECT C.cname, COUNT(*) FROM Courses C inner join Offerings O ON C.cnum = O.cnum group by C.cnum,C.cname;

----
--4)-
----
SELECT C.cname from  Courses C inner join Offerings O on C.cnum = O.cnum where O.onum in (SELECT onum from Enrolled group by onum having count(*) =(SELECT MAX(COUNT(*)) as totalCount from enrolled group by onum)) group by C.cnum, C.cname;

----
--5)-
----
Select C.cname from courses C inner join offerings O on C.cnum = O.cnum inner join Enrolled E on E.onum = O.onum inner join Students S on S.snum = E.snum where S.standing = 'GR' and C.course_level = 'UG' group by C.cname having count(*) = (Select MAX(count(*)) as filteredCount from courses C inner join offerings O on C.cnum = O.cnum inner join Enrolled E on E.onum = O.onum inner join Students S on S.snum = E.snum where S.standing = 'GR' and C.course_level = 'UG' group by C.cnum);
---
--6)-
----
Select S.sname, Count(E.onum), COALESCE(SUM(C.credits), 0) from Students S left join Enrolled E on S.snum = E.snum left join Courses C on C.cnum = E.onum group by S.snum, S.sname;

---
--7)-
----
with 
enrolled_alias AS
(Select F.fname as fn, F.fid as fi ,COUNT(E.snum) as counts from faculty F left join offerings O on O.fid = F.fid left join Enrolled E on E.onum = O.onum group by F.fid, F.fname)
SELECT fn, AVG(counts) from enrolled_alias group by fi, fn, counts;
---
--8)-
----

with
filtration_alias AS
(Select F.fname as fn, F.fid as fi, O.max_occupancy as occ from faculty F inner join offerings O on O.fid = F.fid inner join Courses C on C.cnum = O.cnum inner join enrolled E on O.onum = E.onum group by E.onum, F.fname, F.fid, O.max_occupancy having count(E.snum) = O.max_occupancy)
Select fn from filtration_alias group by fi, fn;

@droptables.sql
--@p1_ravi18.sql