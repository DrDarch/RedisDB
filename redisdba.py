#!/usr/bin/python
"""
# ---------------------------------------------------------------------------------------------------------- #
# A script for administration of redis server.                                                               #
#                                                                                                            #
# License: GPLv3                                                                                             #
#											                     #
# For your own responsibility, no warranty for using.                                                        #
#                                                                                                            #
# At present: only binary backup and enter to redis-shell is implemented.                                    #
#                                                                                                            #
# Tested on Centos7, on redis-server 3.2.	                                                             #
# ---------------------------------------------------------------------------------------------------------- #
"""

"""
Base system tuning:
vm.overcommit_memory = 1 in /etc/sysctl.conf
appendonly yes
appendfsync everysec # for some performance compromise

Bash completion - add this file and logout and login again
/etc/bash_completion.d/redisdba.py
#!/bin/bash
eval "$(register-python-argcomplete redisdba.py)"
"""

import argcomplete
import argparse
import datetime
import logging
import os
import psutil
import sys
import subprocess
import time
from argcomplete.completers import EnvironCompleter
from argparse import RawTextHelpFormatter

class Color: # pylint: disable=too-few-public-methods
    """
    Provides the colors definitions.
    """
    Header = '\033[95m'
    Blue = '\033[94m'
    Green = '\033[92m'
    Warning = '\033[93m'
    Fail = '\033[91m'
    Endc = '\033[0m'
    Bold = '\033[1m'
    Underline = '\033[4m'
    BackgroundRed = '\033[41m'
    BackgroundGreen = '\033[42m'
    BackgroundBlue = '\033[44m'
    BackgroundYellow = '\033[43m'

class General:
    """
    The general class.

    Methods:
        check_if_proc_run - checking if process is already running in system
        exec_command - execute input command
        test - for testing purposes
    """

    def __init__(self, action):
        """
        The class constructor.
        """
        self.action = action

    def check_if_proc_run(self, name, arg_id, arg_name):
        """
        Check if process is already running.
        Args:
            Name of process, argument id, argument_name.
        Returns:
            Count number.
        """
        count = 0

        for proc in psutil.process_iter():
            if proc.name() == name:
                logging.debug('Running process: %s', proc.cmdline())
                if proc.cmdline()[arg_id] == arg_name:
                    count = count + 1

        return count

    def exec_command(self, command):
        """
        Execute input command.
        Args:
            command: The command to execute.
        Returns:
            None.
        """
        pii = subprocess.Popen(command, stdout=subprocess.PIPE, shell=True)
        output, err = pii.communicate()

        if pii.returncode == 1:
            msg = "Executed command NOT succeded at all"
            sys.exit(msg)

        return output

    def test(self):
        """
        Testing function.
        Args:
            None.
        Returns:
            None.
        """
        print "Code Testing..."
        print Color.Green, "Color test.", Color.Endc
        print Color.BackgroundRed, "Color test.", Color.Endc

class Db:
    """
    The methods related with database administration.

    Methods:
        binary_backup - full binary backup
        doenter - login to redis shell
    """

    def __init__(self, action):
        """
        The class constructor.
        """
        self.action = action

    def binary_backup(self, backup_dir):
        """
        Make a backup of database.
        Args:
            The path to backup directory.
        Returns:
            None.
        """

        # Check if another backup is running already, if yes not start the new one.
        Gcp1 = General("check_if_proc_run")
        count = Gcp1.check_if_proc_run('redisdba.py', 2, '--backup')

        if count > 1:
            sys.exit(Color.Warning + "The backup process is already running" + Color.Endc)

        logging.debug('Backup directory: %s', backup_dir)

        # Added a time mark to the directory name.
        Gc1 = General("exec_command")
        date = Gc1.exec_command('date +\"%Y-%m-%d_%H-%M-%S\"').rstrip()

        data_dir = backup_dir + '/' + date

        if not os.path.exists(data_dir):
            os.makedirs(data_dir)

        # Saving a program output to the file.
        file = open('{0}/last_backup.log'.format(data_dir), 'w')
        file.write(Color.Blue + "The backup process was started at date: {0} \n".
                   format(datetime.datetime.now()) + Color.Endc)

        # Removing the old local backup.
        file.write("Removing old local backup \n")
        appendonlygz_path = '{0}/appendonly.aof.gz'.format(data_dir)
        dump_path = '{0}/dump.rdb'.format(data_dir)

        if os.path.isfile(appendonlygz_path):
            os.unlink(appendonlygz_path)
        if os.path.isfile(dump_path):
            os.unlink(dump_path)

        # Runnig BGSAVE command.
        rdb_bgsave_in_progress = Gc1.exec_command('redis-cli info persistence | grep rdb_bgsave_in_progress')
        rdb_bgsave_in_progress = rdb_bgsave_in_progress.split(':')[1].rstrip()

        rdb_last_bgsave_status = Gc1.exec_command('redis-cli info persistence | grep rdb_last_bgsave_status')
        rdb_last_bgsave_status = rdb_last_bgsave_status.split(':')[1].rstrip()

        if rdb_bgsave_in_progress == "0" and rdb_last_bgsave_status == "ok":
            Gc1.exec_command('redis-cli bgsave')
            file.write("Bgsave finished \n")

            argument = 'ionice -c 3 cp /var/lib/redis/dump.rdb {0}'.format(data_dir)
            Gc1.exec_command(argument)
            file.write("Copying dump.rdb to backup-dir finished \n")

            # If exists, backup append AOF file.
            appendonly_path = '/var/lib/redis/appendonly.aof'

            if os.path.isfile(appendonly_path) and os.access(appendonly_path, os.R_OK):
                # Check if self maintenance of aof file is done by redis db or if the script should rotate this.
                # If yes, copy only aof file, not truncate.
                rewrite_aof_perc = Gc1.exec_command('redis-cli config get auto-aof-rewrite-percentage').splitlines()
                rewrite_aof_size = Gc1.exec_command('redis-cli config get auto-aof-rewrite-min-size').splitlines()

                if int(rewrite_aof_perc[1]) > 0 and int(rewrite_aof_size[1]) > 0:
                    logging.debug('Rewrite_aof_perc: %s', rewrite_aof_perc)
                    logging.debug('Rewrite_aof_size: %s', rewrite_aof_size)

                    argument = 'ionice -c 3 cp -a {0} {0}.arch'.format(appendonly_path)
                    Gc1.exec_command(argument)
                else:
                    argument = 'ionice -c 3 cp -a {0} {0}.arch && cat /dev/null > {0}'.format(appendonly_path)
                    Gc1.exec_command(argument)

                argument = 'gzip -9 -c {0}.arch > {0}.gz'.format(appendonly_path)
                Gc1.exec_command(argument)
                os.unlink('{0}.arch'.format(appendonly_path))

                argument = 'ionice -c 3 cp -a {0}.gz {1}'.format(appendonly_path, data_dir)
                Gc1.exec_command(argument)

                os.unlink('{0}.gz'.format(appendonly_path))
                file.write("Copying appendonly.aof.gz to backup-dir finished \n")

            else:
                print "The appendonly file is missing."

            file.write(Color.Blue + "The backup process was finished at date: {0} \n".
                       format(datetime.datetime.now()) + Color.Endc)

        file.write("{} completed OK!\n".format(date))
        file.close()

    def doenter(self):
        """
        Enter to redis-cli console.
        Args:
            None.
        Returns:
            None.
        """
        print Color.Green, "Enter to shell of Redis Server.", Color.Endc
        os.system("redis-cli")

def main():
    """
    The main funtion.
    """
    parser = argparse.ArgumentParser(description='', formatter_class=RawTextHelpFormatter)

    parser.add_argument('--backup', dest='backup', action='store_true', default=False,
                        help='Perform backup.').completer = EnvironCompleter

    parser.add_argument('--backup_dir', dest='backup_dir', type=str,
                        help='The backup directory.').completer = EnvironCompleter

    parser.add_argument('--debug', dest='debug', action='store_true',
                        default=False, help='Set debug.').completer = EnvironCompleter

    parser.add_argument('--test', dest='test', help=argparse.SUPPRESS, action="store_true").completer = EnvironCompleter

    parser.add_argument('--enter', dest='enter', action='store_true', default=False,
                        help='Enter to redis-cli.').completer = EnvironCompleter

    argcomplete.autocomplete(parser)
    args = parser.parse_args()

    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.ERROR)

    if args.test:
        Gt1 = General("test")
        Gt1.test()
    elif args.backup and args.backup_dir:
        Dbb1 = Db("binary_backup")
        Dbb1.binary_backup(args.backup_dir)
    elif args.enter:
        Dbe1 = Db("doenter")
        Dbe1.doenter()


if __name__ == '__main__':
    main()
    
