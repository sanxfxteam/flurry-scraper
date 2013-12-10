#!/usr/bin/python
import requests # pip install requests
import csv
import time
import datetime
import json
import getopt
import sys
import urllib

#Variables to set for your project (or using commandline)
projectId = 0
versionId = 0
flurry_email = ''
flurry_password = ''
outFilename = 'events.json'
test_mode = False # stops at first
sessionIDs = {}
sessionFilename = 'sessions.json'
sessionf = open(sessionFilename, 'a')

def get_session(email,password):
    s = requests.session()
    url = 'https://dev.flurry.com/secure/loginAction.do'
    resp = s.post(url,data={'loginEmail': email,'loginPassword': password})
    resp.raise_for_status()
    return s

def params_json(params):
    # example: { use : boost;  boost : magnet}
    if params == '{}':
        return None
    s = params.replace('{ ', '{"').replace('}', '"}').replace(' : ', '":"').replace(';  ', '","')
    return json.loads(s)

def sessionid_from_timestamp(timestamp):
    # example: Nov 22, 2013 11:59 PM
    date = datetime.datetime.strptime(timestamp, "%b %d, %Y %I:%M %p")
    s = date.strftime("%Y%m%d-%H%M")
    count = sessionIDs.get(s)
    if not count:
        count = 1
    else:
        count += 1
    res = s + ('-%03d') % (count)
    sessionIDs[s] = count
    return res

def store_session(session):
    sessionf.write(json.dumps(session) + '\n')

def close_session():
    if sessionf:
        sessionf.close()

def get_events(session,projectId, date, offset):
    url = 'https://dev.flurry.com/eventsLogCsv.do'

    params = {'projectID': projectId,
              'versionCut': versionId,
              'intervalCut': ('customInterval%04d_%02d_%02d-%04d_%02d_%02d') % ( date.year, date.month, date.day, date.year, date.month, date.day),
              'stream' : 'true',
              'direction' : 1,
              'offset' : offset,
              'limits' : 100}

    resp = None
    while resp is None:
        print 'Fetching', url, urllib.urlencode(params)
        resp = session.get(url,params = params, allow_redirects=False)
        if resp.status_code == 302:
            location = resp.headers['location']
            if location == 'https://dev.flurry.com/secure/login.do':
                raise Exception('Invalid email/password')
            if location == 'http://www.flurry.com/rateLimit.html':
                print 'Throttled. Sleeping.'
                time.sleep(60)
            resp = None

    return process_events(resp.content)

def process_events(content):
    reader = csv.reader(content.split('\n'))
    lines = list(reader)
    header = [l.strip() for l in lines[0]]
    rows = [row for row in lines[1:] if len(row) > 0]
    print 'Number of Entries:', len(rows)

    # overrides with limited headers
    iEvent = header.index('Event')
    iParams = header.index('Params')
    iSessionIndex = header.index('Session Index')
    iTimestamp = header.index('Timestamp')
    iUserID = header.index('User ID')
    iDevice = header.index('Device')

    res = []
    sessionID = "Unset"
    for row in rows:
        r = {
            'SIDX' : row[iSessionIndex].strip(),
            'E' : row[iEvent].strip(),
            'P' : params_json(row[iParams].strip()),
        }
        if r['SIDX'] == '1':
            s = {
                'UID': row[iUserID].strip(),
                'SID': sessionid_from_timestamp(row[iTimestamp].strip()),
                'Timestamp': row[iTimestamp].strip(),
                'Device': row[iDevice].strip(),
            }
            sessionID = s['SID']
            store_session(s)
            #print s
        r['SID'] = sessionID
        #print r
        res.append(r)

    return res


def dump(projectId,email,password,startDate,endDate,offset):
    session = get_session(email, password)
    cur_date = startDate

    with open(outFilename,'a', 50 * 1024) as file:
        while cur_date <= endDate:
            events = get_events(session, projectId, cur_date, offset)
            events_count = len(events)

            for event in events:
                file.write(json.dumps(event) + '\n')

            if events_count > 0:
                sessions_count = len([event for event in events if event['SIDX'] == '1'])
                print {'date' : cur_date, 'events' : events_count, 'sessions' : sessions_count, 'offset' : offset}
                offset += sessions_count
            else:
                print {'date' : cur_date, 'events' : events_count, 'offset' : offset}
                offset = 0
                cur_date += datetime.timedelta(days=1)
            if test_mode:
                file.flush()
                file.close()
                print 'test mode on, stops after first query'
                return
            time.sleep(5)

if __name__ == '__main__':

    # default variables
    offset = 0
    startDate = datetime.datetime(2013,11,22)
    endDate = datetime.datetime.today()

    # options
    optlist, args = getopt.getopt(sys.argv[1:], '', 
        ['start-offset=', 'end-date=', 'start-date=', 'test-mode', 'email=', 'password=', 'project-id=', 'version-id='])
    for option, value in optlist:
        if option == '--email':
            flurry_email = value
        elif option == '--password':
            flurry_password = value
        elif option == '--project-id':
            projectId = int(value)
        elif option == '--version-id':
            versionId = int(value)
        elif option == '--start-offset':
            offset = int(value)
        elif option == '--start-date':
            startDate = datetime.datetime.strptime(value, '%Y-%m-%d').date()
        elif option == '--end-date':
            endDate = datetime.datetime.strptime(value, '%Y-%m-%d').date()
        elif option == '--test-mode':
            test_mode = True

    if False:#test_mode:
        print 'running tests...'
        sid = sessionid_from_timestamp('Nov 22, 2013 11:59 PM')
        store_session({'test':'go','tata':sid})
        sid = sessionid_from_timestamp('Nov 22, 2013 11:59 PM')
        store_session({'test':'go','tata':sid})
        process_events("""Event,Params,Session Index,Timestamp,User ID,Device
            Pouet,{ extraballused : 0},1,"Nov 22, 2013 12:01 AM",Lolo,iPhoune
            Pouet,{ extraballused : 0},2,"Nov 22, 2013 12:01 AM",Lolo,iPhoune""")

    dump(projectId, flurry_email, flurry_password, startDate, endDate, offset)
    close_session()
