#simple project for backup files from server

depencious:
s3cmd need install on server
for backup posgresql need user`s grants with no password local connections
```
> useradd backuper
psql> create user backuper;
psql> alter user backuper with superuser;
> echo 'local   all             backuper                                peer' > /path/to/pg_hba.conf
```