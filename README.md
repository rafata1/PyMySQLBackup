# PyMySQLBackup

PyMySQLBackup is a Python script that allows you to backup your MySQL databases to AWS S3.
## Configuration
Use the template from **__config.tmp.yaml__** to create your own configuration file. The configuration file should be named config.yaml and should be placed in the same directory as the script.
```yaml
backups:
  - name: daily_backup # Name of the backup
    cron: "0 2 * * *" # Cron expression for the backup schedule
    mysql:
      host: 127.0.0.1 # MySQL host
      port: 3306 # MySQL port
      user: backup # MySQL user
      password: backup # MySQL password
      database: tc-api # MySQL database
      tables: # list of tables to back up, empty list means all tables
        - my_table1
        - my_table2
    output_dir: ./tmp # Output directory for the backup
    s3:
      bucket: my_bucket # S3 bucket name to store the backup
      access_key: secret # AWS access key
      secret_key: my_secret_key # AWS secret key
      region: ap-southeast-1 # AWS region
      sender_email: "sender@sample.com" # Sender email for the notification
      recipient_email: "recipient@sample.com" # Recipient email for the notification
```

## Running the script
To run the script, simply execute the following command:
```bash
python main.py
```
