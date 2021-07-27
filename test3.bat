rem Project Part 3 test batch file
rem
rem cleanup
rem
del dbfile.bin
del db.lo*
del *.log
del *.tab
del *.obj
del *.pdb
del *.ilk
del first
del second
del third
rem
rem 01. Setup and inserts
rem
db "create table tab1(name char(16), quizzes int, midterm int, final int)"
db "create table tab2(college char(20), zipcode char(5), rank int)"
db "insert into tab1 values('Siu', 11, 80, 560)"
db "insert into tab1 values('Frank', 22, 100, 700)"
db "insert into tab1 values('Jordon', 33, 75, 525)"
db "insert into tab2 values('UCLA', '11111', 3)"
db "insert into tab2 values('SJSU', '22222', 10)"
db "insert into tab2 values('Stanford', '33333', 2)"
db "select * from tab1"
db "select * from tab2"
rem
rem Check transaction log
rem
type db.log
rem
rem 02. Take backup, check image size and db.log
rem
db "backup to first"
rem
rem **Size** first=584; dbfile.bin=336; tab1=120 (3 rows); tab2=120 (3 rows)
dir first dbfile.bin *.tab
type db.log
rem
rem 03. Do more I/U/D
rem
db "insert into tab1 values('Jeff', 44, 60, 515)"
db "insert into tab2 values('UC Berkley', '44444', 1)"
echo Wait a few seconds
pause
db "insert into tab1 values('Ying', 55, 85, 625)"
db "insert into tab2 values('USC', '55555', 4)"
db "delete from tab2 where college = 'UCLA'"
echo Wait a few seconds
pause
db "delete from tab2 where college = 'SJSU'"
db "update tab1 set final = 999 where name = 'Siu'"
db "select * from tab1"
db "select * from tab2"
type db.log
rem
rem 04. Take 2nd backup, check image size and db.log
rem
db "backup to second"
rem
rem **Size** first=648; dbfile.bin=336; tab1=184 (5 rows); tab2=120 (3 rows)
dir second dbfile.bin *.tab
type db.log
rem
rem 05. drop tab2, restore from second, check tab2 and RF_START in log
rem
db "drop table tab2"
dir tab2.tab
db "restore from second"
dir tab2.tab
rem THIS SHOULD FAIL because it should be RF Pending
db "create table tab3(c1 int)"
type db.log
rem
rem 06. Do rollforward, tab2 should be dropped again and RF_START is removed
rem
db "rollforward"
dir tab2.tab
type db.log
rem
rem 07. Do restore from second without RF, check db.log1 before prune,
rem     check tab2 contents
rem
db "create table tab3(c1 int)"
db "insert into tab3 values(911)"
type db.log
db "restore from second without rf"
dir db.log1
rem tab 3 should be gone from PRUNING without rf
type db.log
db "select * from tab2"
rem
rem 08. restore from fisrt, check tab1 & tab2 contents
rem
db "restore from first"
db "select * from tab1"
db "select * from tab2"
type db.log
rem
rem 09. Do rollforward to timestamp  -  Manual step from a different window
rem In this case the timestamp is between the deletion of the 1st the 2nd row from tab2 e.g. db "rollforward to 20030531123030"
pause
rem
rem 10. Copy the db.log to db.log3, copy db.log1 to db.log, restore from second,
rem     rollforward, verify tab2 is dropped again.
rem
copy db.log db.log3
copy db.log1 db.log
db "restore from second"
db "rollforward"
dir tab2.tab
rem
rem 11 - 13. Errors - dupicate backup image name, bad image name, bad timestamp, wrong state
rem
db "backup to third"
db "backup to third"
db "restore from nothing"
db "insert into tab1 values('new', 55, 85, 625)"
db "rollforward"
db "restore from third"
db "rollforward to 20030531123030"
db "rollforward"
db "select * from tab1"
rem
rem End of test1.bat
rem

