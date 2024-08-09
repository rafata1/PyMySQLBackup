import subprocess
import time

import boto3
import yaml
from dataclasses import dataclass


@dataclass
class MySQLConfig:
    host: str
    port: str
    user: str
    password: str
    database: str
    tables: list


@dataclass
class S3Config:
    bucket: str
    access_key: str
    secret_key: str
    region: str


@dataclass
class Backup:
    name: str
    cron: str
    database: MySQLConfig
    output_dir: str
    s3: S3Config


def dump_database(mysql_config: MySQLConfig, output_dir: str) -> str:
    dump_command = [
        "mysqldump",
        f"-h{mysql_config.host}",
        f"-P{mysql_config.port}",
        f"-u{mysql_config.user}",
        f"-p{mysql_config.password}",
        mysql_config.database,
        "-R", "-e", "--triggers", "--single-transaction",
    ]
    print(dump_command)
    timestamp = int(time.time())
    output_file = f"{output_dir}/{mysql_config.database}_{timestamp}.sql"
    with open(output_file, "w") as stdout:
        subprocess.run(dump_command, stdout=stdout)
    return output_file


def compress_file(file_path: str):
    compress_file = f"{file_path}.tar.gz"
    compress_command = ["tar", "-czvf", compress_file, file_path]
    subprocess.run(compress_command)
    return compress_file


def remove_file(file_path: str):
    remove_command = ["rm", file_path]
    subprocess.run(remove_command)


def upload_to_s3(file_path: str, s3_conf: S3Config):
    client = boto3.client(
        "s3",
        aws_access_key_id=s3_conf.access_key,
        aws_secret_access_key=s3_conf.secret_key,
        region_name=s3_conf.region
    )
    try:
        file_name = file_path.split("/")[-1]
        client.upload_file(file_path, s3_conf.bucket, file_name)
        print(f"Uploaded {file_path} to {s3_conf.bucket}")
    except Exception as e:
        print(f"Failed to upload {file_name} to {s3_conf.bucket}")
        print(e)


with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

backups = []
for backup in config['backups']:
    database_config = MySQLConfig(**backup['mysql'])
    s3_config = S3Config(**backup['s3'])
    backup_obj = Backup(
        name=backup['name'],
        cron=backup['cron'],
        database=database_config,
        output_dir=backup['output_dir'],
        s3=s3_config
    )
    dump_file = dump_database(backup_obj.database, backup_obj.output_dir)
    zip_file = compress_file(dump_file)
    remove_file(dump_file)
    upload_to_s3(zip_file, backup_obj.s3)
    remove_file(zip_file)
    print(zip_file)
