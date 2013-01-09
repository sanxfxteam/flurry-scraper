#!/bin/env python

#Variables to set for your project
startDate = (2010,8,1)
projectId = 1234
flurry_email = 'your@email.com'
flurry_password = 'YOURPASSWORD'

import requests
import csv
import time
import datetime
import json

def get_session(email,password):
    s = requests.session()
    url = 'https://dev.flurry.com/secure/loginAction.do'
    resp = s.post(url,data={'loginEmail': email,'loginPassword': password})
    resp.raise_for_status()
    return s


def get_events(session,projectId, date, offset):
    url = 'https://dev.flurry.com/eventsLogCsv.do'

    params = {'projectID':projectId,
              'versionCut':'versionsAll',
              'intervalCut': ('customInterval%04d_%02d_%02d-%04d_%02d_%02d') % ( date.year, date.month, date.day, date.year, date.month, date.day),
              'stream' : 'true',
              'direction' : 1,
              'offset' :offset}



    resp = None
    while resp is None:
        resp = session.get(url,params = params, allow_redirects=False)
        if resp.status_code == 302:
            location = resp.headers['location']
            if location == 'https://dev.flurry.com/secure/login.do':
                raise Exception('Invalid email/password')
            if location == 'http://www.flurry.com/rateLimit.html':
                print 'Throttled. Sleeping.'
                time.sleep(60)
            resp = None

    reader = csv.reader(resp.content.split('\n'))
    lines = list(reader)
    rows = [row for row in lines[1:] if len(row) > 0]
    header = [l.strip() for l in lines[0]]

    return [{header[i] : row[i].strip() for i in range(len(row))} for row in rows]


def dump(projectId,email,password,startDate,endDate):
    session = get_session(email,password)
    cur_date = startDate
    offset = 0

    with open('out.json','w') as file:
        while cur_date <= endDate:
            events = get_events(session,projectId, cur_date,offset)
            events_count = len(events)

            for event in events:
                file.write(json.dumps(event) + '\n')

            if events_count > 0:
                sessions_count = len([event for event in events if event['Session Index'] == '1'])
                print {'date' : cur_date, 'events' : events_count, 'sessions' : sessions_count, 'offset' : offset}
                offset += sessions_count
            else:
                print {'date' : cur_date, 'events' : events_count, 'offset' : offset}
                offset = 0
                cur_date += datetime.timedelta(days=1)
            time.sleep(5)


if __name__ == '__main__':
    endDate = datetime.datetime.today()
    dump(projectId,flurry_email,flurry_password,datetime.datetime(*startDate),endDate)
