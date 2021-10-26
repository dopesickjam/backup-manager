from fabric import Connection
from yaml.loader import SafeLoader
import yaml, argparse, logging, sys, datetime
logging.basicConfig(stream=sys.stdout, level=logging.INFO)

parser = argparse.ArgumentParser(description='Process backup manage')
parser.add_argument('--config', dest='backup_config', help='path to backup yaml config', nargs=1, metavar=("FILE"))
args = parser.parse_args()

def Connect(server, port, user):
    logging.info(f'Connect to {server}')
    c = Connection(host=server, user=user, port=port)
    return c

def folderBackup(c, folder, server):
    backup_dir = f'/tmp/{server}'
    c.sudo(f'mkdir -p {backup_dir}')
    c.sudo(f'cp -r --parents {folder} {backup_dir}')

def foldertoS3(c, folder, server, s3_bucket, s3_path, s3cfg, backup_type, date):
    logging.info(f'Sync {folder} to S3 type is {backup_type}')
    c.sudo(f's3cmd -c {s3cfg} sync /tmp/{server}{folder} s3://{s3_bucket}/{s3_path}/{backup_type}/{date}{folder}')

def rotateBackup(s3cfg, c, rotate_path, rotate_type, name, backup_type):
    s3_folder_list = c.sudo(f's3cmd -c {s3cfg} ls {rotate_path}/{rotate_type}/').stdout

    date_list = []
    for s3_folder in s3_folder_list.strip().split():
        if s3_folder != 'DIR':
            folder_date = s3_folder.split('/')[-2]
            date_list.append(datetime.datetime.strptime(folder_date, '%d_%m_%Y'))

    while True:
        if len(date_list) > retain_daily:
            oldest_folder = date_list[0]

            for old in date_list:
                if old < oldest_folder:
                    oldest_folder = old

            date_list.remove(oldest_folder)

            if backup_type == 'files':
                c.sudo(f's3cmd -c {s3cfg} del --recursive {rotate_path}/{rotate_type}/{oldest_folder.strftime("%d_%m_%Y")}{name}')
            elif backup_type == 'mysql' or backup_type == 'postgres':
                c.sudo(f's3cmd -c {s3cfg} del --recursive {rotate_path}/{rotate_type}/{oldest_folder.strftime("%d_%m_%Y")}/{name}.sql.gz')
        else:
            logging.info(f'All oldest backups was deleted')
            break

def listDB(c, ignore_db, extrafile_path, backup_type):
    logging.info(f'Get list db')

    if backup_type == 'mysql':
        db_list = c.sudo(f'mysql --defaults-extra-file={extrafile_path} -e "show databases;" | grep -v Database').stdout
    elif backup_type == 'postgres':
        db_list = c.run(f'psql -d template1 -c "select datname from pg_database;" | grep -Ev "datname|row"| sed "1d"| cut -f2 -d" "').stdout

    db_list = db_list.strip().split('\n')

    for db in ignore_db:
        db_list.remove(db)

    return db_list

def backupDB(c, server, db, user, backup_type):
    logging.info(f'Backup: {db}')
    backup_dir = f'/tmp/{server}'
    c.sudo(f'mkdir -p {backup_dir}')
    c.sudo(f'chown {user}.{user} {backup_dir}')

    if backup_type == 'mysql':
        c.sudo(f'mysqldump --defaults-extra-file={extrafile_path} --single-transaction {db} | gzip > {backup_dir}/{db}.sql.gz')
    elif backup_type == 'postgres':
        c.run(f'pg_dump {db} -F c -b -v > {backup_dir}/{db}.sql')
        c.run(f'gzip -f {backup_dir}/{db}.sql')

def dbtoS3(c, server, db, s3cfg, s3_bucket, s3_path, day_number, backup_type):
    logging.info(f'Put {db}.sql.gz to s3')
    c.sudo(f's3cmd -c {s3cfg} sync /tmp/{server}/{db}.sql.gz s3://{s3_bucket}/{s3_path}/{backup_type}/{day_number}/{db}.sql.gz')

if args.backup_config:
    logging.info(f'Render config from {args.backup_config[0]}')

    with open(args.backup_config[0]) as f:
        data = yaml.load(f, Loader=SafeLoader)

        server = data['server']
        port = data['port']
        user = data['user']

        for backup_type in data['backup']:
            s3_bucket = backup_type['s3_bucket']
            s3_path = backup_type['s3_path']
            retain_daily = backup_type['retain_daily']
            retain_weekly = backup_type['retain_weekly']
            retain_monthly = backup_type['retain_monthly']

            day_number = datetime.datetime.today().strftime("%d_%m_%Y")
            s3cfg = f'/home/{user}/.s3cfg'

            if backup_type['type'] == 'files':
                logging.info(f'{server}: Starting process for backup files')
                c = Connect(server, port, user)
                dirs = backup_type['dirs']

                for folder in dirs:
                    logging.info(f'{server}: Backup folder {folder}')
                    c.put('.env', s3cfg)

                    folderBackup(c, folder, server)
                    backup_type = 'daily'
                    foldertoS3(c, folder, server, s3_bucket, s3_path, s3cfg, backup_type, day_number)

                    if retain_weekly != 0:
                        if datetime.datetime.today().weekday() == 5:
                            backup_type = 'weekly'
                            foldertoS3(c, folder, server, s3_bucket, s3_path, s3cfg, backup_type, day_number)

                    if retain_monthly != 0:
                        if int(datetime.datetime.today().strftime("%d")) == 1:
                            backup_type = 'monthly'
                            foldertoS3(c, folder, server, s3_bucket, s3_path, s3cfg, backup_type, day_number)

                    c.sudo(f'rm -rf {s3cfg}')
                    c.sudo(f'rm -rf /tmp/{server}')

                logging.info(f'{server}: Backup files was done')

                logging.info(f'{server}: Starting process for rotation files')
                for folder in dirs:
                    logging.info(f'{server}: Rotation folder {folder}')
                    c.put('.env', s3cfg)

                    rotate_path = f's3://{s3_bucket}/{s3_path}'
                    rotate_type = 'daily'
                    rotateBackup(s3cfg, c, rotate_path, rotate_type, folder, 'files')

                    if retain_weekly != 0:
                        rotate_type = 'weekly'
                        rotateBackup(s3cfg, c, rotate_path, rotate_type, folder, 'files')

                    if retain_monthly != 0:
                        rotate_type = 'monthly'
                        rotateBackup(s3cfg, c, rotate_path, rotate_type, folder, 'files')
                    c.sudo(f'rm -rf {s3cfg}')

                c.close()
                logging.info(f'{server}: Rotation files was done')

            elif backup_type['type'] == 'mysql':
                logging.info(f'{server}: Starting process for backup mysql')
                c = Connect(server, port, user)

                extrafile_path = backup_type['extrafile_path']
                ignore_db = backup_type['ignore_db']

                list_db = listDB(c, ignore_db, extrafile_path, backup_type['type'])

                for db in list_db:
                    c.put('.env', s3cfg)

                    backupDB(c, server, db, user, 'mysql')

                    backup_type = 'daily'
                    dbtoS3(c, server, db, s3cfg, s3_bucket, s3_path, day_number, backup_type)

                    if retain_weekly != 0:
                        if datetime.datetime.today().weekday() == 5:
                            backup_type = 'weekly'
                            dbtoS3(c, server, db, s3cfg, s3_bucket, s3_path, day_number, backup_type)

                    if retain_monthly != 0:
                        if int(datetime.datetime.today().strftime("%d")) == 1:
                            backup_type = 'monthly'
                            dbtoS3(c, server, db, s3cfg, s3_bucket, s3_path, day_number, backup_type)

                    c.sudo(f'rm -rf {s3cfg}')
                    c.sudo(f'rm -rf /tmp/{server}')

                logging.info(f'{server}: Backup dbs was done')

                logging.info(f'{server}: Starting process for rotation dbs')
                for db in list_db:
                    c.put('.env', s3cfg)

                    rotate_path = f's3://{s3_bucket}/{s3_path}'
                    rotate_type = 'daily'
                    rotateBackup(s3cfg, c, rotate_path, rotate_type, db, 'mysql')

                    if retain_weekly != 0:
                        rotate_type = 'weekly'
                        rotateBackup(s3cfg, c, rotate_path, rotate_type, db, 'mysql')

                    if retain_monthly != 0:
                        rotate_type = 'monthly'
                        rotateBackup(s3cfg, c, rotate_path, rotate_type, db, 'mysql')

                    c.sudo(f'rm -rf {s3cfg}')
                c.close()
                logging.info(f'{server}: Rotation dbs was done')

            elif backup_type['type'] == 'postgres':
                logging.info(f'{server}: Starting process for backup postgres')
                c = Connect(server, port, user)

                ignore_db = backup_type['ignore_db']

                list_db = listDB(c, ignore_db, None, backup_type['type'])

                for db in list_db:
                    c.put('.env', s3cfg)

                    backupDB(c, server, db, user, backup_type['type'])

                    backup_type = 'daily'
                    dbtoS3(c, server, db, s3cfg, s3_bucket, s3_path, day_number, backup_type)

                    if retain_weekly != 0:
                        if datetime.datetime.today().weekday() == 5:
                            backup_type = 'weekly'
                            dbtoS3(c, server, db, s3cfg, s3_bucket, s3_path, day_number, backup_type)

                    if retain_monthly != 0:
                        if int(datetime.datetime.today().strftime("%d")) == 1:
                            backup_type = 'monthly'
                            dbtoS3(c, server, db, s3cfg, s3_bucket, s3_path, day_number, backup_type)

                    c.sudo(f'rm -rf {s3cfg}')
                    c.sudo(f'rm -rf /tmp/{server}')

                logging.info(f'{server}: Backup dbs was done')

                logging.info(f'{server}: Starting process for rotation dbs')
                for db in list_db:
                    c.put('.env', s3cfg)

                    rotate_path = f's3://{s3_bucket}/{s3_path}'
                    rotate_type = 'daily'
                    rotateBackup(s3cfg, c, rotate_path, rotate_type, db, 'postgres')

                    if retain_weekly != 0:
                        rotate_type = 'weekly'
                        rotateBackup(s3cfg, c, rotate_path, rotate_type, db, 'postgres')

                    if retain_monthly != 0:
                        rotate_type = 'monthly'
                        rotateBackup(s3cfg, c, rotate_path, rotate_type, db, 'postgres')

                    c.sudo(f'rm -rf {s3cfg}')
                c.close()
                logging.info(f'{server}: Rotation dbs was done')