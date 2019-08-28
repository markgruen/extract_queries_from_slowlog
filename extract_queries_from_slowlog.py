#!/usr/bin/env python2.7
"""
usage: 
extract_queries_from_slowlog1.py [<table>...]

  tables filter slow logs by table

"""

from docopt import docopt
from schema import Schema, And, Use, Optional
import sys
import re
import numpy as np
import itertools
import json
from cStringIO import StringIO
from collections import deque

__version__ = 0.12

_debug_ = False

test_data = """
#other stuff
#other stuff
# Time: 2019-07-04T13:16:26.816409Z
# User@Host: stern_user[stern_user] @ [10.216.3.27] Id: 12301860
# Query_time: 0.015636 Lock_time: 0.000000 Rows_sent: 91 Rows_examined: 1972
use telehealth;
SET timestamp=1562246186;
SELECT `affiliations`.`id` FROM `sms_messages` WHERE `affiliations`.`insurance_payer_id` = 27;
# Time: 2019-07-04T13:16:26.816409Z
# User@Host: stern_user[stern_user] @ [10.216.3.27] Id: 12301860
# Query_time: 0.015636 Lock_time: 0.000000 Rows_sent: 91 Rows_examined: 1972
use telehealth;
SET timestamp=1562246186;
SELECT `affiliations`.`id`
FROM `sms_messages` WHERE
`affiliations`.`insurance_payer_id` = 27;
# Time: 2019-08-04T18:37:57.613353Z
# User@Host: stern_user[stern_user] @  [10.216.3.20]  Id: 113466
# Query_time: 0.015629  Lock_time: 0.000000 Rows_sent: 0  Rows_examined: 0
SET timestamp=1564943877;
BEGIN;
# Time: 2019-08-04T18:32:17.403046Z
# User@Host: stern_user[stern_user] @  [10.216.3.27]  Id: 111550
# Query_time: 0.015657  Lock_time: 0.000000 Rows_sent: 0  Rows_examined: 0
SET timestamp=1564943537;
COMMIT;
# Time: 2019-07-04T13:16:26.816409Z
# User@Host: stern_user[stern_user] @ [10.216.3.27] Id: 12301860
# Query_time: 0.015636 Lock_time: 0.000000 Rows_sent: 91 Rows_examined: 1972
use telehealth;
SET timestamp=1562246186;
SELECT `affiliations`.`id`
FROM `users` WHERE
`affiliations`.`insurance_payer_id` = 27;
# Time: 2019-07-04T13:16:26.816409Z
# User@Host: stern_user[stern_user] @ [10.216.3.27] Id: 12301860
# Query_time: 0.015636 Lock_time: 0.000000 Rows_sent: 91 Rows_examined: 1972
use telehealth;
SET timestamp=1562246186;
DELETE 
FROM `users` WHERE
`affiliations`.`insurance_payer_id` = 27;
# Time: 2019-07-04T13:16:26.816409Z
# User@Host: stern_user[stern_user] @ [10.216.3.27] Id: 12301860
# Query_time: 0.015636 Lock_time: 0.000000 Rows_sent: 91 Rows_examined: 1972
use telehealth;
SET timestamp=1562246186;
SELECT * from `users`;
# Time: 2019-07-04T13:16:26.816409Z
# User@Host: stern_user[stern_user] @ [10.216.3.27] Id: 12301860
# Query_time: 0.015636 Lock_time: 0.000000 Rows_sent: 91 Rows_examined: 1972
use telehealth;
SET timestamp=1562246186;
select * from 
`users`;
# Time: 2019-07-17T17:18:25.699616Z
# User@Host: stern_user[stern_user] @  [10.216.3.11]  Id: 1419634
# Query_time: 0.015613  Lock_time: 0.000000 Rows_sent: 0  Rows_examined: 3
SET timestamp=1563383905;
SELECT 
      cust_appointments.id,
      cust_appointments.customer_on_call_queue_id as cocq_id,
      states.name as location_name,
      users.fullname,
      users.first_name,
      users.last_name,
      affiliations.description as affiliation_name,
      cust_appointments.state_id,
      cust_appointments.physician_type_id,
      cust_appointments.created_at,
      cust_appointments.id as cust_appointment_id,
      cust_appointments.appointment_status_id,
      cust_appointments.user_id,
      cust_appointments.appointment_method_id,
      cust_appointments.physician_type_id,
      appointment_statuses.name as on_call_status_name,
      cust_appointments.user_id as customer_id,
      cust_appointments.chief_complaint,
      IF(cust_appointments.appointment_status_id = 16, 'video', 'phone') as on_call_type_name,
      TIMESTAMPDIFF(YEAR, users.birthdate, CURDATE()) as age,
      users.gender,
      cust_appointments.created_at,
      TIMESTAMPDIFF(MINUTE, cust_appointments.start_time, now()) as minutes,
      LPAD(MOD(TIMESTAMPDIFF(Second, cust_appointments.start_time, now()), 60), 2, 0) as seconds,
      cust_appointments.affiliation_id,
      concat(TIMESTAMPDIFF(MINUTE, cust_appointments.start_time, now()), ' : ', LPAD(MOD(TIMESTAMPDIFF(Second, cust_appointments.start_time, now()), 60), 2, 0)) as time_in_queue
     FROM `cust_appointments` INNER JOIN `states` ON `states`.`id` = `cust_appointments`.`state_id` INNER JOIN `appointment_statuses` ON `appointment_statuses`.`id` = `cust_appointments`.`appointment_status_id` INNER JOIN `users` ON `users`.`id` = `cust_appointments`.`user_id` AND `users`.`user_type_id` IN (9) LEFT OUTER JOIN `affiliations` ON `affiliations`.`id` = `cust_appointments`.`affiliation_id` WHERE `cust_appointments`.`id` IN (19099852, 19099851, 19099876) AND `cust_appointments`.`appointment_status_id` IN (11, 16) ORDER BY minutes DESC, seconds DESC;
                                                    AND ((appointment_method_id = 1 AND start_time >= '2019-07-17 11:18:22.796048') OR (appointment_method_id = 2))) ORDER BY `cust_appointments`.`start_time` ASC;
"""

test_data1 = """
c:\mysql\bin\mysqld-sbs.exe, Version: 5.7.24-log (MySQL Community Server (GPL)). started with:
TCP Port: 20047, Named Pipe: /tmp/mysql.sock
Time                 Id Command    Argument
# Time: 2019-07-17T17:18:22.871506Z
# User@Host: provider_user[provider_user] @  [10.216.3.16]  Id: 1425389
# Query_time: 0.031269  Lock_time: 0.000000 Rows_sent: 4  Rows_examined: 1205
use telehealth;
SET timestamp=1563383902;
SELECT `cust_appointments`.* FROM `cust_appointments` WHERE (physicianId = 8784251 AND appointment_status_id IN (1,5,21)
# Time: 2019-07-17T17:18:25.699616Z
# User@Host: stern_user[stern_user] @  [10.216.3.11]  Id: 1419634
# Query_time: 0.015613  Lock_time: 0.000000 Rows_sent: 0  Rows_examined: 3
SET timestamp=1563383905;
SELECT 
      cust_appointments.id,
      cust_appointments.customer_on_call_queue_id as cocq_id,
      states.name as location_name,
      users.fullname,
      users.first_name,
      users.last_name,
      affiliations.description as affiliation_name,
      cust_appointments.state_id,
      cust_appointments.physician_type_id,
      cust_appointments.created_at,
      cust_appointments.id as cust_appointment_id,
      cust_appointments.appointment_status_id,
      cust_appointments.user_id,
      cust_appointments.appointment_method_id,
      cust_appointments.physician_type_id,
      appointment_statuses.name as on_call_status_name,
      cust_appointments.user_id as customer_id,
      cust_appointments.chief_complaint,
      IF(cust_appointments.appointment_status_id = 16, 'video', 'phone') as on_call_type_name,
      TIMESTAMPDIFF(YEAR, users.birthdate, CURDATE()) as age,
      users.gender,
      cust_appointments.created_at,
      TIMESTAMPDIFF(MINUTE, cust_appointments.start_time, now()) as minutes,
      LPAD(MOD(TIMESTAMPDIFF(Second, cust_appointments.start_time, now()), 60), 2, 0) as seconds,
      cust_appointments.affiliation_id,
      concat(TIMESTAMPDIFF(MINUTE, cust_appointments.start_time, now()), ' : ', LPAD(MOD(TIMESTAMPDIFF(Second, cust_appointments.start_time, now()), 60), 2, 0)) as time_in_queue
     FROM `cust_appointments` INNER JOIN `states` ON `states`.`id` = `cust_appointments`.`state_id` INNER JOIN `appointment_statuses` ON `appointment_statuses`.`id` = `cust_appointments`.`appointment_status_id` INNER JOIN `users` ON `users`.`id` = `cust_appointments`.`user_id` AND `users`.`user_type_id` IN (9) LEFT OUTER JOIN `affiliations` ON `affiliations`.`id` = `cust_appointments`.`affiliation_id` WHERE `cust_appointments`.`id` IN (19099852, 19099851, 19099876) AND `cust_appointments`.`appointment_status_id` IN (11, 16) ORDER BY minutes DESC, seconds DESC;
                                                    AND ((appointment_method_id = 1 AND start_time >= '2019-07-17 11:18:22.796048') OR (appointment_method_id = 2))) ORDER BY `cust_appointments`.`start_time` ASC;
# Time: 2019-07-17T17:18:30.465245Z
# User@Host: stern_user[stern_user] @  [10.216.3.25]  Id: 1425777
# Query_time: 0.000000  Lock_time: 0.000000 Rows_sent: 0  Rows_examined: 9
SET timestamp=1563383910;
SELECT     cust_appointments.id as id,
     concat('Dr.', users.fullname) as physician_fullname,
     phys_availabilities.start_date as start_date,
     appointment_method_id,
     appointment_methods.name as consultation,
     physicianId as provider_id,
     physicianId,
     physician_type_id,
     appointment_status_id,
     cust_appointments.customer_call_in_number,
     cust_appointments.chief_complaint,
    users.photo_file_name as photo_file_name
 FROM `cust_appointments` INNER JOIN `appointment_methods` ON `appointment_methods`.`id` = `cust_appointments`.`appointment_method_id` INNER JOIN `phys_availabilities` ON `phys_availabilities`.`id` = `cust_appointments`.`phys_availability_id` INNER JOIN `users` ON `users`.`id` = `phys_availabilities`.`user_id` AND `users`.`user_type_id` IN (3) WHERE `cust_appointments`.`user_id` = 8701976 AND `cust_appointments`.`appointment_status_id` IN (5, 1, 21) AND (start_date >= '2019-07-17 12:18:30.400296') AND (users.user_status_id = 1) ORDER BY start_date;
"""


def read_section(file):
    """
    section starts with 
    # Time: .*
    bla
    djdj
    use telehealth;
    select * from
    sms_messages;
    # Time:
    """

    data = []

    # speedups
    start_pat = re.compile(r'^# Time: ')
    isstart = start_pat.search
    #append = data.append

    N = 0
    capture = False
    while True:
        line = file.readline(1048576)
        # start end of section
        #if start_pat.search(line):
        if isstart(line):
            capture = True
            # end of section
            if len(data)>0 and data[-1].endswith(';'):
                capture = False
                yield data
                data = []
                data.append(line.rstrip('\n'))
                capture = True
            else:
                data = []
                data.append(line.rstrip('\n'))
        elif capture:
            if not line:
                yield data
                break
            data.append(line.rstrip('\n'))
        if not line:
            yield data
            break
        N += 1


def main3(filein, tables):
    """
    optimize by using lazy qualifiers *? not *.
    this didn't help

    trying to add anchors

    allow capture of all sections with no tables
    """

    flatten_chain = lambda lst: list(itertools.chain.from_iterable(lst))

    if len(tables)>0:
        table_pat_str = r'({})'.format('|'.join(tables))
        #join_pat_str = r'(from|join) .*({})'.format(table_pat_str)
        sec_pat_str = (
            r"^# (?P<time>Time: [0-9-T:Z.]+)\n"
            "^# User@Host: (?P<user>.*[0-9]+)\n"
            "^# Query_time: (?P<query_time>[0-9.]+) +"
            "Lock_time: (?P<lock_time>[0-9.]+) +"
            "Rows_sent: (?P<rows_sent>[0-9]+) +"
            "Rows_examined: (?P<rows_examined>[0-9]+)\n"
            ".*"
            "^SET timestamp=(?P<epoch>[0-9]+);\n"
            "(?P<sql_str>^(SELECT|select|INSERT|insert|UPDATE|update|DELETE|delete|REPLACE|replace|SET|set) .*?(?P<table>{}).*?;$)"
        )
    else:
        table_pat_str = ''
        sec_pat_str = (
            r"^# (?P<time>Time: [0-9-T:Z.]+)\n"
            "^# User@Host: (?P<user>.*[0-9]+)\n"
            "^# Query_time: (?P<query_time>[0-9.]+) +"
            "Lock_time: (?P<lock_time>[0-9.]+) +"
            "Rows_sent: (?P<rows_sent>[0-9]+) +"
            "Rows_examined: (?P<rows_examined>[0-9]+)\n"
            ".*"
            "^SET timestamp=(?P<epoch>[0-9]+);\n"
            "(?P<sql_str>^(?P<sql_command>(SELECT|select|INSERT|insert|UPDATE|update|DELETE|delete|REPLACE|replace|SET|set|BEGIN|COMMIT|begin|commit)).*;$)"
        )
        #"(?P<sql_str>^(SELECT|select|INSERT|insert|UPDATE|update|DELETE|delete|REPLACE|replace|SET|set|COMMIT|commit|BEGIN|begin)\b.*;$)"

    if _debug_ == True:
        from pprint import pprint
        print('table_pat: {}'.format(table_pat_str))
        #print('join_table: {}'.format(join_pat_str))
        print('pat_str: {}'.format(sec_pat_str).format(table_pat_str))
        print('')


    #re.purge()

    sec_pat = re.compile(sec_pat_str.format(table_pat_str), re.DOTALL|re.MULTILINE)
    #join_pat = re.compile(join_pat_str, re.I | re.M)
    #join_pat = re.compile(join_pat_str, re.I )
    join_pat = re.compile(r'(from|join) *(\S+)', re.I )


    # speedups
    is_section = sec_pat.search
    find_tables = join_pat.findall
    jdump = json.dumps

    # --------------------------------
    # not trapping update <table>
    # check replace
    # insert into
    # update <table>
    # update <join>
    # --------------------------------

    for section in read_section(filein):
        #print('SECTION')
        #print('\n'.join(section))
        #print('')
        m = is_section('\n'.join(section))
        if m:
            #print('MATCH')
            o_dic = m.groupdict()
            #"""
            tabs = find_tables(o_dic['sql_str'])
            tables = [e.strip('`') for e in list(np.unique(flatten_chain(tabs))) if e.upper() not in ['FROM','JOIN']]
            #o_dic['table'] = '{},'.format(','.join(tables).rstrip(','))
            o_dic['table'] = ',{},'.format(','.join(tables).rstrip(','))
            o_dic['sql_command'] = o_dic['sql_command'].upper()

            #"""
            if _debug_ == True:
                print(json.dumps(o_dic, indent=4))
                #print('')
            else:
                print(jdump(o_dic))


if __name__ == "__main__":

    args = docopt(__doc__, version = __version__)
    if _debug_ == True:
        print(args)

    """
    schema = Schema({
      '<table>': 
      })
    """

    #filein = StringIO(test_data)
    filein = sys.stdin

    #ps = "SET timestamp.*?{}.*?;$".format(table)
    #print("DEBUG: {}".format(ps))

    main3(filein, args['<table>'])

