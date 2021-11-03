from fabric import Connection
from yaml.loader import SafeLoader
import yaml, argparse, logging, sys, datetime, requests, os, subprocess
logging.basicConfig(stream=sys.stdout, level=logging.INFO)

parser = argparse.ArgumentParser(description='Process backup manage')
parser.add_argument('--config', dest='backup_config', help='path to backup yaml config', nargs=1, metavar=("FILE"))
args = parser.parse_args()

def Connect(server, port, user):
    logging.info(f'Connect to {server}')
    c = Connection(host=server, user=user, port=port)
    return c

def folderBackup(c, folder, server):
    logging.info(f'Copy {folder} to tmp dir')
    backup_dir = f'/tmp/{server}'
    c.sudo(f'mkdir -p {backup_dir}')
    c.sudo(f'cp -r --parents {folder} {backup_dir}')

def foldertoS3(c, folder, server, s3_bucket, s3_path, s3cfg, s3_quiet_sync, use_tmp):
    logging.info(f'Sync {folder} to S3')
    if s3_quiet_sync and use_tmp:
        c.sudo(f's3cmd -c {s3cfg} --quiet sync --skip-existing /tmp/{server}{folder} s3://{s3_bucket}/{s3_path}/.sync{folder}')
    elif not s3_quiet_sync and use_tmp:
        c.sudo(f's3cmd -c {s3cfg} sync --skip-existing /tmp/{server}{folder} s3://{s3_bucket}/{s3_path}/.sync{folder}')
    elif s3_quiet_sync and not use_tmp:
        c.sudo(f's3cmd -c {s3cfg} --quiet sync --skip-existing {folder} s3://{s3_bucket}/{s3_path}/.sync{folder}')
    elif not s3_quiet_sync and not use_tmp:
        c.sudo(f's3cmd -c {s3cfg} sync --skip-existing {folder} s3://{s3_bucket}/{s3_path}/.sync{folder}')

def syncS3(c, folder, server, s3_bucket, s3_path, s3cfg, s3_quiet_sync, backup_type, day_number):
    logging.info(f'Sync folder {folder} with backup type: {backup_type}')
    if s3_quiet_sync:
        c.sudo(f's3cmd -c {s3cfg} --quiet cp --skip-existing --recursive s3://{s3_bucket}/{s3_path}/.sync{folder} s3://{s3_bucket}/{s3_path}/{backup_type}/{day_number}{folder}')
    else:
        c.sudo(f's3cmd -c {s3cfg} cp --skip-existing --recursive s3://{s3_bucket}/{s3_path}/.sync{folder} s3://{s3_bucket}/{s3_path}/{backup_type}/{day_number}{folder}')

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

def dbtoS3(c, server, db, s3cfg, s3_bucket, s3_path, day_number, backup_type, s3_quiet_sync):
    logging.info(f'Put {db}.sql.gz to s3')
    if s3_quiet_sync:
        c.sudo(f's3cmd -c {s3cfg} --quiet sync /tmp/{server}/{db}.sql.gz s3://{s3_bucket}/{s3_path}/{backup_type}/{day_number}/{db}.sql.gz')
    else:
        c.sudo(f's3cmd -c {s3cfg} sync /tmp/{server}/{db}.sql.gz s3://{s3_bucket}/{s3_path}/{backup_type}/{day_number}/{db}.sql.gz')

def backupDomains(cloudflare_token):
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {cloudflare_token}'
    }

    response = requests.get(
        'https://api.cloudflare.com/client/v4/zones?per_page=5&direction=asc',
        headers = headers
    )

    response = response.json()
    total_pages = response['result_info']['total_pages']

    for page in range(total_pages):
        response = requests.get(
            f'https://api.cloudflare.com/client/v4/zones?per_page=5&direction=asc&page={page}',
            headers = headers
        )
        response = response.json()
        for zone_id in response['result']:
            print(zone_id['id'], zone_id['name'])

            zone_export = requests.get(
                f'https://api.cloudflare.com/client/v4/zones/{zone_id["id"]}/dns_records/export',
                headers = headers
            )

            f = open(f'tmp/{zone_id["name"]}', 'w')
            f.write(zone_export.text)
            f.close()

def domainstoS3(s3_bucket, s3_path, day_number, backup_type, s3_quiet_sync):
    logging.info(f'Put domains to s3')
    if s3_quiet_sync:
        os.system(f's3cmd -c .env --quiet put --recursive tmp/ s3://{s3_bucket}/{s3_path}/{backup_type}/{day_number}/')
    else:
        os.system(f's3cmd -c .env put --recursive tmp/ s3://{s3_bucket}/{s3_path}/{backup_type}/{day_number}/')

def syncdomainS3(s3_bucket, s3_path, day_number, backup_type):
    logging.info(f'Sync domains with backup type: {backup_type}')
    if s3_quiet_sync:
        os.system(f's3cmd -c .env --quiet cp --recursive s3://{s3_bucket}/{s3_path}/daily/{day_number}/ s3://{s3_bucket}/{s3_path}/{backup_type}/{day_number}/')
    else:
        os.system(f's3cmd -c .env cp --recursive s3://{s3_bucket}/{s3_path}/daily/{day_number}/ s3://{s3_bucket}/{s3_path}/{backup_type}/{day_number}/')

def rotateDomain(rotate_path, rotate_type):
    s3_folder_list = subprocess.check_output(f's3cmd -c .env ls {rotate_path}/{rotate_type}/', shell=True).decode("utf-8")

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

            os.system(f's3cmd -c .env del --recursive {rotate_path}/{rotate_type}/{oldest_folder.strftime("%d_%m_%Y")}/')
        else:
            logging.info(f'All oldest backups was deleted')
            break

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
            s3_quiet_sync = backup_type['s3_quiet_sync']
            retain_daily = backup_type['retain_daily']
            retain_weekly = backup_type['retain_weekly']
            retain_monthly = backup_type['retain_monthly']

            day_number = datetime.datetime.today().strftime("%d_%m_%Y")
            if user == 'root':
                s3cfg = f'/{user}/.s3cfg'
            else:
                s3cfg = f'/home/{user}/.s3cfg'

            if backup_type['type'] == 'files':
                logging.info(f'{server}: Starting process for backup files')
                c = Connect(server, port, user)
                dirs = backup_type['dirs']
                use_tmp = backup_type['use_tmp']

                for folder in dirs:
                    logging.info(f'{server}: Backup folder {folder}')
                    c.put('.env', s3cfg)

                    if use_tmp:
                        folderBackup(c, folder, server)

                    foldertoS3(c, folder, server, s3_bucket, s3_path, s3cfg, s3_quiet_sync, use_tmp)

                    backup_type = 'daily'
                    syncS3(c, folder, server, s3_bucket, s3_path, s3cfg, s3_quiet_sync, backup_type, day_number)

                    if retain_weekly != 0:
                        if datetime.datetime.today().weekday() == 5:
                            backup_type = 'weekly'
                            syncS3(c, folder, server, s3_bucket, s3_path, s3cfg, s3_quiet_sync, backup_type, day_number)

                    if retain_monthly != 0:
                        if int(datetime.datetime.today().strftime("%d")) == 1:
                            backup_type = 'monthly'
                            syncS3(c, folder, server, s3_bucket, s3_path, s3cfg, s3_quiet_sync, backup_type, day_number)

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
                    dbtoS3(c, server, db, s3cfg, s3_bucket, s3_path, day_number, backup_type, s3_quiet_sync)

                    if retain_weekly != 0:
                        if datetime.datetime.today().weekday() == 5:
                            backup_type = 'weekly'
                            dbtoS3(c, server, db, s3cfg, s3_bucket, s3_path, day_number, backup_type, s3_quiet_sync)

                    if retain_monthly != 0:
                        if int(datetime.datetime.today().strftime("%d")) == 1:
                            backup_type = 'monthly'
                            dbtoS3(c, server, db, s3cfg, s3_bucket, s3_path, day_number, backup_type, s3_quiet_sync)

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

                    backupDB(c, server, db, user, 'postgres')

                    backup_type = 'daily'
                    dbtoS3(c, server, db, s3cfg, s3_bucket, s3_path, day_number, backup_type, s3_quiet_sync)

                    if retain_weekly != 0:
                        if datetime.datetime.today().weekday() == 5:
                            backup_type = 'weekly'
                            dbtoS3(c, server, db, s3cfg, s3_bucket, s3_path, day_number, backup_type, s3_quiet_sync)

                    if retain_monthly != 0:
                        if int(datetime.datetime.today().strftime("%d")) == 1:
                            backup_type = 'monthly'
                            dbtoS3(c, server, db, s3cfg, s3_bucket, s3_path, day_number, backup_type, s3_quiet_sync)

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

            elif backup_type['type'] == 'cloudflare':
                logging.info('Start process for backup cloudflare')
                if not os.path.exists('tmp'):
                    os.mkdir('tmp')
                cloudflare_token = backup_type['cloudflare_token']

                backupDomains(cloudflare_token)
                backup_type = 'daily'
                domainstoS3(s3_bucket, s3_path, day_number, backup_type, s3_quiet_sync)

                if retain_weekly != 0:
                    if datetime.datetime.today().weekday() == 5:
                        backup_type = 'weekly'
                        syncdomainS3(s3_bucket, s3_path, day_number, backup_type)

                if retain_monthly != 0:
                    if int(datetime.datetime.today().strftime("%d")) == 1:
                        backup_type = 'monthly'
                        syncdomainS3(s3_bucket, s3_path, day_number, backup_type)
                logging.info(f'Backup cloudflare was done')

                logging.info(f'Starting process for rotation domain')
                rotate_path = f's3://{s3_bucket}/{s3_path}'
                rotate_type = 'daily'
                rotateDomain(rotate_path, rotate_type)

                if retain_weekly != 0:
                    rotate_type = 'weekly'
                    rotateDomain(rotate_path, rotate_type)

                if retain_monthly != 0:
                    rotate_type = 'monthly'
                    rotateDomain(rotate_path, rotate_type)
                logging.info(f'Rotation domains was done')