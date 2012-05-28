#!/usr/bin/env python

import os
import sys
import re
import time
import json
import syslog
import traceback
from datetime import datetime

from fabric.api import *
from boto.ec2.connection import EC2Connection

import smtplib
from email.mime.text import MIMEText

ZONE = 'us-east-1a'

_js = json.load(open("/home/backupbot/fabric.json"))
###########
# DB Slave backup settings
BACKUP_SERVER_INSTANCE = '' # backup server instance id
LIVE_MYSQL_VOLUME_ID = '' # MySQL slave volume ID
BACKUPBOT_PASSWORD = _js['backupbot_password']

INNODB_ROLLBACK_STR = "InnoDB: Rolling back trx with id"
INNODB_SUCCESS_STR = "InnoDB: Rollback of non-prepared transactions completed"
INNODB_READY_STR = "/usr/sbin/mysqld: ready for connections."
TIME_STR = "%y%m%d %H:%M:%S"
LOG_UPDATE_DELAY = 60 # Seconds to wait after last log update

USER_TIME_QUERY = "mysql -u root --password=%s " % _js['db_password'] + \
        "-sse 'SELECT MAX(date_joined) FROM auth_user' lightbox"
MYSQL_TIME_STR = "%Y-%m-%d %H:%M:%S"
MAX_NEWEST_USER_DELAY = 600 # Newest user must be within this period (in seconds) of snapshot

TMP_VOL_SIZE = 100
TMP_SNAPSHOT_DESCR = "Temporary snapshot of db_slave for backup"
GOOD_SNAPSHOT_DESCR = "Backupbot - db_slave: "
MOUNT_POINT = "/dev/sdk"
MOUNT_DEVICE = "/dev/sdk1"

###########
# Logs backup settings
LOGS_VOLUME_ID = "" # logs volume id
LOGS_FILE_PATH = "/home/volume/log/nginx-access*.log"

# regex matches time in logs e.g: [26/Aug/2011:14:30:15 +0000]
# and returns 26/Aug/2011:14:30:15 in the first group
LOGS_TIME_REGEX = "\[(\\d+/[a-zA-Z]+/\\d+.*?:\\d+:\\d+:\\d+).*?\]"
#extract the time from string
LOGS_TIME_STR = "%d/%b/%Y:%H:%M:%S"
MAX_LOGS_DELAY = 300 # last log update must be within this (seconds)
LOGS_SNAPSHOT_PREFIX = "Backupbot - logs: "
LOGS_HOST = '' # logs hostname

###########
# General settings
EMAIL_TO = "sysadmin@FIXME" # Add sysadmin email address
LOG_PREFIX = "[BACKUPBOT]"
MAX_ATTEMPTS = 1

OS_USER =  os.environ.get('USER')

# Assume this is a production server if user is backupbot
REQD_USER = "backupbot"
PROD_SERVER = OS_USER == REQD_USER

if PROD_SERVER:
    # Read sensitive config data from /etc/keys.py
    sys.path.append('/etc')
    from keys import *
else:
    syslog.syslog(syslog.LOG_ERR, "Script not executed by %s" % REQD_USER)
    raise Exception("Script not executed by %s" % REQD_USER)

def do_backup():
    global syslog_output
    env.password = BACKUPBOT_PASSWORD

    start_time = time.strftime(MYSQL_TIME_STR)

    #Backup db slave
    backup_server = start_backup_server()
    env.hosts = [backup_server]
    env.roledefs.update({'backup_server': [backup_server]})

    try:
        slave_details = run_slave_backup().values()[0]
    except:
        slave_details = {
                'success': False,
                'traceback': traceback.format_exc(),
                'syslog': syslog_output,
        }
    try:
        conn = EC2Connection(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
        conn.stop_instances([BACKUP_SERVER_INSTANCE])
    except:
        pass

    # reset syslog capture
    syslog_output = ""

    # Backup logs drive
    env.hosts = [LOGS_HOST]
    env.roledefs.update({'logs': [LOGS_HOST]})
    env.password = _js['logs_password']

    try:
        log_details = run_logs_backup().values()[0]
    except:
        log_details = {
            'success': False,
            'syslog': syslog_output,
            'traceback': traceback.format_exc(),
        }

    send_report_email(start_time, slave_details, log_details)
    log(syslog.LOG_INFO, "Backup Completed")

@roles(["backup_server"])
def run_slave_backup():
    """Backup strategy:

    Turn on test server
    Take a snapshot of db_slave
    Create a volume from snapshot
    Connect volume to test server
    Start MySQL on test server
    Wait for MySQL to clean database transactions
    Check database integrity
    If ok
        - take a snapshot of test volume
        else:
        - try again with new snapshot
        until:
        max number of retry attempts reached

    Destroy temporary volume and original snapshot
    Stop test server
    Send email with details of backup

    USAGE: fab -f {filename} backup_server do_backup
    """

    start_time = time.localtime()
    start_time_dt = datetime.now()
    log(syslog.LOG_INFO, "Start backup of db slave")

    conn = EC2Connection(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)

    log(syslog.LOG_INFO, "Connection to AWS: %s" % conn)
    cleanup_server(force=True)
    database_ok = False

    for retry_attempt in range(MAX_ATTEMPTS):

        start_time = time.localtime()

        log(syslog.LOG_INFO, "Taking snapshot")
        original_snapshot = get_live_snapshot(conn)
        test_volume = create_volume(conn, original_snapshot)

        log(syslog.LOG_INFO, "Mounting volume")
        sudo('/bin/mount %s /home/volume' % MOUNT_DEVICE)
        mysql_start = sudo('start mysql')

        # Test integrity
        if mysql_start.succeeded:
            database_ok, snapshot_details = test_db_repaired(start_time)

            log(syslog.LOG_INFO, "Database ok?: %s" % database_ok)
            if database_ok:
                description = GOOD_SNAPSHOT_DESCR + time.strftime(TIME_STR,
                                                                  start_time)
                repaired_snapshot = test_volume.create_snapshot(description)
                repaired_snapshot.add_tag("Name", "db_slave: %s" %
                                          time.strftime("%Y-%m-%d"))

        else:
            log(syslog.LOG_ERR, "MySQL failed to start")
            database_ok = False

        # cleanup - unmount drive and delete volume
        cleanup_server()
        destroy_volume(test_volume)
        original_snapshot.delete()

        if database_ok:
            break

    log(syslog.LOG_INFO, 'Snapshot integrity ok?: %s' % database_ok)
    with settings(
        hide('warnings', 'running', 'stdout', 'stderr')
    ):
        error_log = sudo("tail -n 200 /var/log/mysql/error.log")

    duration = datetime.now() - start_time_dt
    details = {
        'live_db_id': LIVE_MYSQL_VOLUME_ID,
        'success': database_ok,
        'start_time': start_time_dt.strftime(TIME_STR),
        'duration': "%s:%s" % (duration.seconds / 60, duration.seconds % 60),
        'syslog': syslog_output,
        'error_log': error_log,
        }

    if database_ok:
        details['snapshot_id'] = repaired_snapshot.id
        details.update(snapshot_details)

    return details

def start_backup_server():
    # fabric requires a seperate method to dynamically set env.hosts
    try:
        log(syslog.LOG_INFO, "Connect to Backup Server")
        conn = EC2Connection(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
        conn.start_instances([BACKUP_SERVER_INSTANCE])

        instance_list = conn.get_all_instances([BACKUP_SERVER_INSTANCE])
        instance = instance_list[0].instances[0]

        log(syslog.LOG_INFO, "Instance found: %s" % instance)

        while not instance.state == "running":
            instance.update()
            time.sleep(3)

        log(syslog.LOG_INFO, "Backup Server at %s" % instance.public_dns_name)
        return instance.public_dns_name

    except:
        log(syslog.LOG_ERR, traceback.print_exc())
        return None

@roles(['logs'])
def run_logs_backup():
    log(syslog.LOG_INFO, "Start backup of logs")
    start_time = datetime.now()

    # Check the logs are updating and current
    with settings(
        hide('warnings', 'running', 'stdout', 'stderr')
    ):
        result = sudo("tail -n 2 %s" % LOGS_FILE_PATH)
    search = re.search(LOGS_TIME_REGEX, result)

    if search is None:
        log(syslog.LOG_ERR, "Could not find log file with matching time stamp")
        details = {
                    "success": False,
                    'syslog': syslog_output,
                  }
        return details

    match_str = search.group(1)
    last_log = datetime.strptime(match_str, LOGS_TIME_STR)
    log_ok = (datetime.now() - last_log).seconds < MAX_LOGS_DELAY

    log_ok = True

    if not log_ok:
        log(syslog.LOG_ERR, "last time stamp found was too old: %s" % match_str)
        details = {
                    "success": False,
                    'syslog': syslog_output,
                  }
        return details

    conn = EC2Connection(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
    volume = conn.get_all_volumes([LOGS_VOLUME_ID])[0]

    log(syslog.LOG_INFO, "Creating snapshot of logs volume")
    description = LOGS_SNAPSHOT_PREFIX + time.strftime('%Y-%m-%d')
    snapshot = volume.create_snapshot(description)
    time.sleep(3)
    duration = datetime.now() - start_time

    details = {
        'logs_volume_id': LOGS_VOLUME_ID,
        'snapshot_id': snapshot.id,
        'success': True,
        'duration': "%s:%s" % (duration.seconds / 60, duration.seconds % 60),
        'latest_log_time': last_log,
        }

    return details

###########################

def get_live_snapshot(conn):
    # Attach to live MySQL volume
    live_volume = conn.get_all_volumes(volume_ids=[LIVE_MYSQL_VOLUME_ID])[0]
    snapshot = live_volume.create_snapshot(description=TMP_SNAPSHOT_DESCR)

    log(syslog.LOG_INFO,
        'Creating snapshot from volume: %s' % LIVE_MYSQL_VOLUME_ID)

    wait_for_aws(snapshot, "pending")
    return snapshot

def create_volume(conn, snapshot_name):
    snapshot_volume =  conn.create_volume(TMP_VOL_SIZE, ZONE, snapshot_name)
    log(syslog.LOG_INFO, 'Creating test volume')
    wait_for_aws(snapshot_volume, "creating")

    try:
        snapshot_volume.attach(BACKUP_SERVER_INSTANCE, MOUNT_POINT)
    except:
        log(syslog.LOG_INFO, 'Old volume found mounted at %s' % MOUNT_POINT)

        # If the script didn't shutdown cleanly, an old snapshot may still
        # be attached. Attempt to detach it.
        cleanup_server(force=True)

        volumes = [v for v in conn.get_all_volumes() \
            if v.attach_data.instance_id == BACKUP_SERVER_INSTANCE]
        for volume in volumes:
            if volume.attach_data.device == MOUNT_POINT:
                volume.detach(BACKUP_SERVER_INSTANCE)
                wait_for_aws(volume, "in-use")
                snapshot_volume.attach(BACKUP_SERVER_INSTANCE, MOUNT_POINT)
                snapshot_volume.update()

    log(syslog.LOG_INFO, 'Attaching %s' % snapshot_volume)
    wait_for_aws(snapshot_volume, "available")
    time.sleep(10)     # appears to be required to attach drive reliably

    return snapshot_volume

def destroy_volume(volume):
    volume.detach(BACKUP_SERVER_INSTANCE)
    wait_for_aws(volume, "in-use")
    volume.delete()

def wait_for_aws(volume, status):
    while volume.status == status:
        volume.update()
        time.sleep(3)

def test_db_repaired(start_time):
    start_time_epoch = time.mktime(start_time)

    error_result = False, {}

    log(syslog.LOG_INFO, 'Starting MySQL')

    for i in range(5):
        is_doing_rollback = find_in_log(INNODB_ROLLBACK_STR) > start_time_epoch
        if is_doing_rollback:
            break
        time.sleep(60)

    if is_doing_rollback:
        log(syslog.LOG_INFO, 'Database repairing')
        for i in range(60):
            rollback_finished = not recent_log_update()
            if rollbacks_finished:
                break
            time.sleep(60)

        if not rollback_finished:
            log(syslog.LOG_ERR, 'Rollback timed out')
            return error_result

        log(syslog.LOG_INFO, 'Database completed repair')

    log(syslog.LOG_INFO, 'Waiting for database to come online')

    for i in range(10):
        database_ready = find_in_log(INNODB_READY_STR) > start_time_epoch
        if database_ready:
            break
        time.sleep(60)

    if not database_ready:
        log(syslog.LOG_ERR, 'Database timed out coming online')
        return error_result

    newest_user_ok, new_user_time = check_newest_user_time(start_time_epoch)
    details = {}
    details['newest_user_time'] = new_user_time

    if not newest_user_ok:
        log(syslog.LOG_ERR, 'Newest user too old: %s' %
                new_user_time)

    return newest_user_ok, details

def recent_log_update():
    last_rollback_time = find_in_log(INNODB_ROLLBACK_STR)
    if last_rollback_time < 0:
        return False

    time_diff = time.time() - last_rollback_time
    return time_diff <= LOG_UPDATE_DELAY

def rollback_success(start_time):
    success_time = find_in_log(INNODB_SUCCESS_STR)
    return success_time > start_time

def find_in_log(search_str):
    # Find the last line matching search_string
    with settings(hide('warnings', 'running', 'stdout', 'stderr')):
        grep_output = sudo("grep '%s' /var/log/mysql/error.log | tail -n 1" %
                           search_str)

    try:
        log_time = time.mktime(time.strptime(grep_output[:15], TIME_STR))
    except ValueError:
        return -1
    return log_time

def check_newest_user_time(start_time_epoch):
    with settings(hide=['everything']):
        user_time_str = run(USER_TIME_QUERY)
    newest_user_time = time.mktime(time.strptime(user_time_str, MYSQL_TIME_STR))

    within_delay = (start_time_epoch - newest_user_time) < MAX_NEWEST_USER_DELAY

    return within_delay, user_time_str

def cleanup_server(force=False):
    with settings(
        hide('warnings', 'running', 'stdout', 'stderr'),
        warn_only=True
    ):
        if force:
            sudo('pkill -9 -f mysqld')
        sudo('stop mysql')
        sudo('/bin/umount %s' % MOUNT_DEVICE)

def send_report_email(start_time, db_slave, logs):

    to_result = lambda success: "SUCCESS" if success else "FAILED"
    summary = {
                'start_time': start_time,
                'db_slave_result': to_result(db_slave['success']),
                'logs_result': to_result(logs['success']),
              }

    email_text = \
        """Daily backup summary - Started at %(start_time)s

        DB Slave: %(db_slave_result)s
        Logs: %(logs_result)s

        """ % summary

    if db_slave['success']:
        email_text += """
        DB Slave Details
        -------
        Live DB Volume ID: \t\t%(live_db_id)s
        Backup snapshot id: \t\t%(snapshot_id)s
        Duration: \t\t\t\t%(duration)s
        Newest user signup time: \t%(newest_user_time)s
        """ % db_slave

    if logs['success']:
        email_text += """
        Logs Details
        -------
        Logs Volume ID: \t\t %(logs_volume_id)s
        Backup snapshot id: \t\t%(snapshot_id)s
        Duration: \t\t\t\t%(duration)s
        Latest log time: \t\t  %(latest_log_time)s
        """ % logs

    if not db_slave['success']:
        email_text += """
        DB Slave failure report
        """
        email_text += report_error(db_slave)

    if not logs['success']:
        email_text += """
        Logs failure report
        """
        email_text += report_error(logs)

    # Remove leading tabs from string
    content = ''
    for line in email_text.split('\n'):
        content = '\n'.join([content, line.lstrip()])

    msg = MIMEText(content)

    sender = "backupbot@FIXME" # Add sender email address
    to = EMAIL_TO
    msg['Subject'] = 'Daily Backup'
    msg['From'] = sender
    msg['To'] = to

    server = smtplib.SMTP(EMAIL_HOST, EMAIL_PORT) #port 465 or 587
    server.ehlo()
    server.starttls()
    server.ehlo()
    server.login(EMAIL_HOST_USER, EMAIL_HOST_PASSWORD)
    server.sendmail(sender, [to], msg.as_string())
    server.close()

def report_error(details):
    email_text = ''
    try:
        # Python raised error
        email_text += \
        """========================
        Traceback:
        %(traceback)s
        """ % details
    except:
        log(syslog.LOG_INFO, "No traceback")

    try:
        # Python raised error
        email_text += \
        """
        ========================
        Syslog output:
        %(syslog)s
        """ % details
    except:
        log(syslog.LOG_INFO, "No syslog")

    try:
        email_text += \
        """
        ========================
        Error.log output:
        %(error_log)s
        """ % details
    except:
        log(syslog.LOG_INFO, "No error log")

    return email_text

# A global containing output to log
syslog_output = ""

def log(level, msg):
    global syslog_output
    out_msg = LOG_PREFIX + ": " + msg

    syslog.syslog(level, out_msg)
    syslog_output = '\n'.join([syslog_output, out_msg])

