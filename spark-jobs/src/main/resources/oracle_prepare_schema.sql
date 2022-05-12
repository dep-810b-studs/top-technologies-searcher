CREATE TABLESPACE TS_SO DATAFILE 'D:\app\Eugene\oradata\ORCL\SO01.dbf' SIZE 31G,
           'D:\app\Eugene\oradata\ORCL\SO02.dbf' SIZE 31G,
           'D:\app\Eugene\oradata\ORCL\SO03.dbf' SIZE 31G,
           'D:\app\Eugene\oradata\ORCL\SO04.dbf' SIZE 31G,
           'D:\app\Eugene\oradata\ORCL\SO05.dbf' SIZE 31G;

ALTER TABLESPACE USERS ADD DATAFILE 'D:/app/Eugene/oradata/ORCL/USERS02.dbf' SIZE 31G AUTOEXTEND ON NEXT 100M MAXSIZE UNLIMITED;
ALTER TABLESPACE USERS ADD DATAFILE 'D:\app\Eugene\oradata\ORCL\USERS03.dbf' SIZE 31G AUTOEXTEND ON NEXT 100M MAXSIZE UNLIMITED;
ALTER TABLESPACE USERS ADD DATAFILE 'D:\app\Eugene\oradata\ORCL\USERS04.dbf' SIZE 31G AUTOEXTEND ON NEXT 100M MAXSIZE UNLIMITED;
ALTER TABLESPACE USERS ADD DATAFILE 'D:\app\Eugene\oradata\ORCL\USERS05.dbf' SIZE 31G AUTOEXTEND ON NEXT 100M MAXSIZE UNLIMITED;

create user so identified by so default tablespace ts_so;
grant connect, resource to so;
GRANT UNLIMITED TABLESPACE TO so;

create user so_ru identified by so_ru default tablespace users;
grant connect, resource to so_ru;
GRANT UNLIMITED TABLESPACE TO so_ru;

select df.tablespace_name "Tablespace",
totalusedspace "Used MB",
(df.totalspace - tu.totalusedspace) "Free MB",
df.totalspace "Total MB",
round(100 * ( (df.totalspace - tu.totalusedspace)/ df.totalspace))
"Pct. Free"
from
(select tablespace_name,
round(sum(bytes) / 1048576) TotalSpace
from dba_data_files
group by tablespace_name) df,
(select round(sum(bytes)/(1024*1024)) totalusedspace, tablespace_name
from dba_segments
group by tablespace_name) tu
where df.tablespace_name = tu.tablespace_name ;

select * from v$tablespace

select segment_name, sum(bytes/1024/1024) MB
 from dba_segments
 where segment_type='TABLE' and tablespace_name = 'USERS'
 group by segment_name
 order by MB desc;

select * from dba_segments where segment_name = 'POSTS';

begin
  DBMS_STATS.GATHER_TABLE_STATS ('SO', 'POSTS');
end;

expdp system/manager@orcl dumpfile=SO_RU.dump schemas=SO_RU compression=all version=11.2.0