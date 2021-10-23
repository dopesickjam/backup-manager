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
    c.sudo(f'cp -r {folder} {backup_dir}')

def foldertoS3(c, folder, server, s3_bucket, s3_path, s3cfg, backup_type, date):
    logging.info(f'Sync {folder} to S3 type is {backup_type}')
    c.sudo(f's3cmd -c {s3cfg} sync /tmp/{server}{folder} s3://{s3_bucket}/{s3_path}/{backup_type}/{date}{folder}')

def rotateBackup(s3cfg, c, rotate_path, rotate_type, folder):
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
            c.sudo(f's3cmd -c {s3cfg} del --recursive {rotate_path}/{rotate_type}/{oldest_folder.strftime("%d_%m_%Y")}{folder}')
        else:
            logging.info(f'All old folders was deleted')
            break

if args.backup_config:
    logging.info(f'Render config from {args.backup_config[0]}')

    with open(args.backup_config[0]) as f:
        data = yaml.load(f, Loader=SafeLoader)

        server = data['server']
        port = data['port']
        user = data['user']

        c = Connect(server, port, user)

        for backup_type in data['backup']:
            if backup_type['type'] == 'files':
                logging.info(f'{server}: Starting process for backup files')
                dirs = backup_type['dirs']
                s3_bucket = backup_type['s3_bucket']
                s3_path = backup_type['s3_path']
                retain_daily = backup_type['retain_daily']
                retain_weekly = backup_type['retain_weekly']
                retain_monthly = backup_type['retain_monthly']

                for folder in dirs:
                    logging.info(f'{server}: Backup folder {folder}')
                    s3cfg = '/tmp/.s3cfg'
                    c.put('.env', s3cfg)

                    folderBackup(c, folder, server)
                    day_number = datetime.datetime.today().strftime("%d_%m_%Y")
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
                    s3cfg = '/tmp/.s3cfg'
                    c.put('.env', s3cfg)

                    rotate_path = f's3://{s3_bucket}/{s3_path}'
                    rotate_type = 'daily'
                    rotateBackup(s3cfg, c, rotate_path, rotate_type, folder)

                    if retain_weekly != 0:
                        rotate_type = 'weekly'
                        rotateBackup(s3cfg, c, rotate_path, rotate_type, folder)

                    if retain_monthly != 0:
                        rotate_type = 'monthly'
                        rotateBackup(s3cfg, c, rotate_path, rotate_type, folder)

                c.sudo(f'rm -rf {s3cfg}')
                c.close()
                logging.info(f'{server}: Rotation files was done')