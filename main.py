import os
import subprocess
import time
from typing import Optional
import boto3
import yaml
from dataclasses import dataclass

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger


@dataclass
class MySQLConfig:
    host: str
    port: str
    user: str
    password: str
    database: str
    tables: Optional[list] = None


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


class Backuper:
    dumped_files = []
    failed_dumps = []
    compressed_files = []
    failed_compressions = []
    uploaded_files = []
    failed_uploads = []

    def dump_database(self, mysql_config: MySQLConfig, output_dir: str) -> str:
        dump_command = [
            "mysqldump",
            f"-h{mysql_config.host}",
            f"-P{mysql_config.port}",
            f"-u{mysql_config.user}",
            f"-p{mysql_config.password}",
            mysql_config.database,
            "-R", "-e", "--triggers", "--single-transaction",
        ]
        print(f"Dumping database {mysql_config.database}")
        output_file = f"{output_dir}/{mysql_config.database}.sql"

        try:
            with open(output_file, "w") as stdout:
                subprocess.run(dump_command, stdout=stdout)
            print(f"Dumped database {mysql_config.database}")
            self.dumped_files.append(output_file)
            return output_file
        except Exception as e:
            print(f"Failed to dump database {mysql_config.database}")
            print(e)
            self.failed_dumps.append(mysql_config.database)

    def dump_table(self, mysql_config: MySQLConfig, output_dir: str, table: str) -> str:
        print(f"Dumping table {mysql_config.database}/{table}")
        dump_command = [
            "mysqldump",
            f"-h{mysql_config.host}",
            f"-P{mysql_config.port}",
            f"-u{mysql_config.user}",
            f"-p{mysql_config.password}",
            mysql_config.database,
            table,
            "-R", "-e", "--triggers", "--single-transaction",
        ]
        output_file = f"{output_dir}/{mysql_config.database}_{table}.sql"
        try:
            with open(output_file, "w") as stdout:
                subprocess.run(dump_command, stdout=stdout)
            print(f"Dumped table {mysql_config.database}/{table}")
            self.dumped_files.append(output_file)
            return output_file
        except Exception as e:
            print(f"Failed to dump table {mysql_config.database}/{table}")
            print(e)
            self.failed_dumps.append(f"{mysql_config.database}/{table}")

    def compress_file(self, file_path: str) -> str:
        print(f"Compressing {file_path}")
        compressed_file_path = f"{file_path}.tar.gz"
        compress_command = ["tar", "-czvf", compressed_file_path, file_path]
        try:
            subprocess.run(compress_command)
            print(f"Compressed {compressed_file_path}")
            self.compressed_files.append(compressed_file_path)
            return compressed_file_path
        except Exception as e:
            print(f"Failed to compress {file_path}")
            print(e)
            self.failed_compressions.append(file_path)
            return

    @staticmethod
    def remove_dir(dir: str):
        print(f"Removing {dir}")
        remove_command = ["rm", "-rf", dir]
        try:
            subprocess.run(remove_command)
            print(f"Removed {dir}")
        except Exception as e:
            print(f"Failed to remove {dir}")
            print(e)

    def upload_to_s3(self, file_path: str, s3_conf: S3Config):
        print(f"Uploading {file_path} to {s3_conf.bucket}")
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
            self.uploaded_files.append(file_path)
        except Exception as e:
            print(f"Failed to upload to {s3_conf.bucket}")
            print(e)
            self.failed_uploads.append(file_path)

    def do_backup_database(self, backup: Backup):
        timestamp = int(time.time())
        dump_folder = f"{backup.output_dir}/{backup.name}_{timestamp}"
        os.makedirs(dump_folder, exist_ok=True)
        self.dump_database(backup.database, dump_folder)
        compressed_file_path = self.compress_file(dump_folder)
        self.remove_dir(dump_folder)
        self.upload_to_s3(compressed_file_path, backup.s3)
        self.remove_dir(compressed_file_path)

    def do_backup_table(self, backup: Backup):
        timestamp = int(time.time())
        dump_folder = f"{backup.output_dir}/{backup.name}_{timestamp}"
        os.makedirs(dump_folder, exist_ok=True)
        for table in backup.database.tables:
            self.dump_table(backup.database, dump_folder, table)
        compressed_file_path = self.compress_file(dump_folder)
        self.remove_dir(dump_folder)
        self.upload_to_s3(compressed_file_path, backup.s3)
        self.remove_dir(compressed_file_path)

    def do_backup(self, backup: Backup):
        os.makedirs(backup.output_dir, exist_ok=True)
        if backup.database.tables:
            self.do_backup_table(backup)
        else:
            self.do_backup_database(backup)
        self.remove_dir(backup.output_dir)

    def start(self):
        with open("config.yaml", "r") as f:
            config = yaml.safe_load(f)

        scheduler = BlockingScheduler()
        for backup in config["backups"]:
            database_config = MySQLConfig(**backup['mysql'])
            s3_config = S3Config(**backup['s3'])
            backup_obj = Backup(
                name=backup['name'],
                cron=backup['cron'],
                database=database_config,
                output_dir=backup['output_dir'],
                s3=s3_config
            )

            cron_parts = backup_obj.cron.split(" ")
            if len(cron_parts) != 5:
                print(f"Invalid cron format for {backup_obj.name}")
                return

            trigger = CronTrigger.from_crontab(backup_obj.cron)
            scheduler.add_job(
                self.do_backup,
                trigger=trigger,
                args=[backup_obj]
            )
            print(f"Added backup job for {backup_obj.name}: ", cron_parts)

        try:
            scheduler.start()
        except (KeyboardInterrupt, SystemExit):
            pass


backuper = Backuper()
backuper.start()
