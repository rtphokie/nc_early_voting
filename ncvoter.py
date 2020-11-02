import unittest
from pprint import pprint
from tqdm import tqdm
# import requests, requests_cache  # https://requests-cache.readthedocs.io/en/latest/
# requests_cache.install_cache('test_cache', backend='sqlite', expire_after=3600)

import sqlite3
import json
import simple_cache

'''
source: https://www.ncsbe.gov/results-data/absentee-data
https://dl.ncsbe.gov/?prefix=ENRS/2020_11_03/
'''
class db():
    def __init__(self, db_file='ncvoter.db'):
        self.db_file = db_file
        self.conn =  self.create_connection()
        self.conn.execute('pragma journal_mode=wal')
        # self.conn.execute('pragma synchronous = 0')
        self.cur = self.conn.cursor()

    def __del__(self):
        self.conn.close()

    def reindex(self, year=None):
        if year is None:
            start = 2008
            end = 2020
        else:
            start=year
        end+=4
        for year in range(2008,2024,4):
            print(year)
            sql = f'CREATE INDEX "idx_{year}_ncid" ON "NC{year}" ( "ncid", "voter_reg_num", "ballot_rtn_status");'
            print(sql)
            try:
                self.conn.execute(f'DROP INDEX "idx_{year}_ncid"')
            except:
                pass
            self.conn.execute(sql)
            self.conn.commit()

    def create_connection(self):
        """ create a database connection to the SQLite database
            specified by the db_file
        :param db_file: database file
        :return: Connection object or None
        """
        conn = None
        try:
            conn = sqlite3.connect(self.db_file, isolation_level=None)
        except Error as e:
            print(e)

        return conn

    def table_info(self):
        sql=''' SELECT
      m.name AS table_name, 
      p.cid AS col_id,
      p.name AS col_name,
      p.type AS col_type,
      p.pk AS col_is_pk,
      p.dflt_value AS col_default_val,
      p.[notnull] AS col_is_not_null
    FROM sqlite_master m
    LEFT OUTER JOIN pragma_table_info((m.name)) p
      ON m.name <> p.name
    WHERE m.type = 'table'
    ORDER BY table_name, col_id
        '''
        self.cur.execute(sql)

        rows = self.cur.fetchall()


        result = {}
        for row in rows:
            if row[0] not in result.keys():
                result[row[0]]={}
            result[row[0]][row[2]]={'col_name': row[2],
                                    'col_id': row[1],
                                    'col_type': row[3],
                                    'col_is_pk': row[4]==1,
                                    }
        return result

    # @simple_cache.cache_it(filename="sqlquery.cache", ttl=14400)
    def query(self, sql, cols):
        self.cur.execute(sql)

        rows = self.cur.fetchall()


        result = []
        for row in rows:
            result.append(dict(zip(cols, row)))

        return result

    def rejected_voters_list(self):
        # find voter ids of all voters with more than one entry, ignoring those marked cancelled
        sql = '''SELECT ncid, voter_reg_num, COUNT(*) as count FROM (select * from NC2020 where ballot_rtn_status != "CANCELLED") GROUP BY ncid, voter_reg_num HAVING COUNT(*) > 1 order by count asc '''
        # SELECT ncid, voter_reg_num, COUNT(*) as count FROM NC2020 GROUP BY ncid, voter_reg_num HAVING COUNT(*) > 1 order by count asc
        data = self.query(sql, ['ncid', 'voter_reg_num', 'count'])
        with open('dupes.json', 'w') as outfile:
            # save to a JSON file, just 'cause
            json.dump(data, outfile, indent=4)
        return data

    def rejected_voters_table(self):
        self.create_rejected_voters_table()
        cnt = 0
        data = self.rejected_voters_list()
        for row in tqdm(data):
            cnt += 1
            # if row['count'] <= 2 or row['count'] > 4:
            #     continue
            id = row['ncid']
            cols = ["county_desc", "ballot_rtn_status", "ballot_rtn_dt", "site_name", "ballot_req_type"]
            sql = f'''
            select {', '.join(cols)} from NC2020 where ncid = "{id}" order by ballot_rtn_dt ASC, ballot_rtn_status DESC
            '''
            data = self.query(sql, cols)
            linestr = f"{data[0]['county_desc']}"
            accepted_ballot_rtn_dt, accepted_req_type, accepted_site_name, failed_ballot_rtn_dt, failed_ballot_rtn_status, failed_req_types, failed_site_names = self.process_line(
                data)

            dates = sorted([sub['ballot_rtn_dt'] for sub in data])
            req_types = [sub['ballot_req_type'] for sub in data]
            rtn_statuses = [sub['ballot_rtn_status'] for sub in data]
            rtn_site_names = [sub['site_name'].replace("'", "''") for sub in data]

            story = "unknown"
            accepted = -1
            if 'ACCEPTED' in rtn_statuses:
                accepted = 1
                if len(set(dates)) == 1:
                    story = f'same day'
                    continue
                elif len(set(req_types)) == 1:
                    story = f'cured via {req_types[0]}'
                else:
                    if len(set(failed_req_types)) > 1:
                        story = f'{",".join(set(failed_req_types))} cured via {accepted_req_type}'
                    else:
                        story = f'{failed_req_types[0]} cured via {accepted_req_type}'
            else:
                accepted = 0
                story = 'not accepted'
            failed_cnt = len(set(failed_ballot_rtn_dt))
            self.insert_into_rejected_voters_table(accepted, accepted_ballot_rtn_dt, accepted_req_type,
                                                   accepted_site_name, data, failed_ballot_rtn_dt,
                                                   failed_ballot_rtn_status, failed_req_types, failed_site_names, id)

            if story == 'unknown':
                pprint(data)
                print(sql)
                print(e)
                raise

    def create_rejected_voters_table(self):
        sql = '''drop table if exists rejected_voters_2020;
        CREATE TABLE rejected_voters_2020 (county, ncid, accepted, accepted_method, accepted_site_name, date_first_rejection, date_accepted, failed_attempts, failed_methods, failed_dates, failed_ballot_rtn_statuses, failed_site_names);
        '''
        self.conn.executescript(sql)
        self.conn.commit()

    def insert_into_rejected_voters_table(self, accepted, accepted_ballot_rtn_dt, accepted_req_type, accepted_site_name,
                                          data, failed_ballot_rtn_dt, failed_ballot_rtn_status, failed_req_types,
                                          failed_site_names, id):
        sql = f'''INSERT OR REPLACE INTO rejected_voters_2020 (county, ncid, 
                                                         accepted, accepted_method, accepted_site_name, date_first_rejection, date_accepted, 
                                                         failed_attempts, failed_methods, failed_dates,
                                                         failed_ballot_rtn_statuses, failed_site_names)
                 VALUES ('{data[0]["county_desc"]}', '{id}', 
                          {accepted}, '{accepted_req_type}' , '{accepted_site_name}', '{failed_ballot_rtn_dt[0]}', '{accepted_ballot_rtn_dt}',
                          {len(failed_req_types)}, '{",".join(set(failed_req_types))}','{",".join(set(failed_ballot_rtn_dt))}',
                         '{",".join(set(failed_ballot_rtn_status))}',
                         '{",".join(set(failed_site_names))}'
                        );
                   '''
        try:
            count = self.cur.execute(sql)
            self.conn.commit()
        except Exception as e:
            pprint(data)
            print(sql)
            print(e)
            raise
        return sql

    def process_line(self, data):
        accepted_req_type = None
        accepted_ballot_rtn_dt = None
        accepted_site_name = None
        failed_req_types = []
        failed_site_names = []
        failed_ballot_rtn_dt = []
        failed_ballot_rtn_status = []
        for attempt in data:
            if attempt['ballot_req_type'] == 'MAIL':
                attempt['site_name'] = 'MAIL'
            if attempt['ballot_rtn_status'] == 'ACCEPTED':
                accepted_req_type = attempt['ballot_req_type']
                accepted_site_name = attempt['site_name'].replace("'", "''")
                accepted_ballot_rtn_dt = attempt['ballot_rtn_dt']
            else:
                failed_req_types.append(attempt['ballot_req_type'])
                failed_site_names.append(attempt['site_name'].replace("'", "''"))
                failed_ballot_rtn_dt.append(attempt['ballot_rtn_dt'])
                failed_ballot_rtn_status.append(attempt['ballot_rtn_status'].replace("'", "''"))
        if accepted_ballot_rtn_dt is not None and accepted_ballot_rtn_dt in failed_ballot_rtn_dt:
            # strip date ballot was accepted from list of failed dates
            while accepted_ballot_rtn_dt in failed_ballot_rtn_dt:
                failed_ballot_rtn_dt.remove(accepted_ballot_rtn_dt)
        return accepted_ballot_rtn_dt, accepted_req_type, accepted_site_name, failed_ballot_rtn_dt, failed_ballot_rtn_status, failed_req_types, failed_site_names


class MyTestCase(unittest.TestCase):
    def test_columns(self):
        uut=db()
        data = uut.table_info()
        prev = None
        for year in sorted(data.keys(), reverse=True):
            print(year, prev)
            if prev is None:
                prev=year
                continue
            this = set(data[year].keys())
            that = set(data[prev].keys())
            print(this-that)
            prev=year

    def test_rejected_voters_list(self):
        uut=db()
        uut.rejected_voters_list()

    def test_age_demo(self):
        uut=db()
        data = uut.query('select age,  count(distinct ncid) from NC2016 group by age', ['age', 'cnt'])
        pprint (data)

    def test_rejected_voters_table(self):
        uut=db()
        uut.rejected_voters_table()



    def test_reindex(self):
        uut=db()
        uut.reindex()

    def test_previous_years(self):
        uut = db()
        for year in range (2008,2024,4):
            sql = f'select count(DISTINCT ncid) from NC{year} where ballot_rtn_status in ("ACCEPTED", "OK")'
            print(sql)
            try:
                accpted = uut.query(sql, ['cnt'])[0]['cnt']
            except:
                accpted = None
            print(f"{year} {accpted}")

    def test_story(self):
        uut=db()
        ###################
        # outstanding votes
        ###################
        earlyintotal=4560358

        # rejected
        sql = '''select count(distinct ncid) as count from NC2020 where ballot_req_type like "%MAIL%" '''
        # data = uut.query(sql, ['cnt'])
        # mailintotal=data[0]['cnt']
        mailintotal = 955809
        print(f"mail-in voters: {mailintotal}")

        sql = '''select count(distinct(ncid)) from rejected_voters_2020'''
        # data = uut.query(sql, ['cnt'])
        # rejectedtotal=data[0]['cnt']
        rejectedtotal=14893
        print(f"rejected voters: {rejectedtotal}")

        sql = '''select count(distinct ncid) as count from rejected_voters_2020 where accepted_method = "None" '''
        data = uut.query(sql, ['cnt'])
        rejectednotfixed = data[0]['cnt']
        print(f"rejected voters not fixed: {rejectednotfixed} {round(((rejectednotfixed/rejectedtotal)*100),2)}%")

        sql = '''select count(distinct ncid) as count from rejected_voters_2020 where accepted_method != "None" '''
        data = uut.query(sql, ['cnt'])
        rejectedfixed = data[0]['cnt']
        print(f"rejected voters fixed: {rejectedfixed} {round(((rejectedfixed/rejectedtotal)*100),2)}%")

        self.assertTrue(rejectednotfixed+rejectedfixed ==rejectedtotal )

        sql = '''select count(distinct ncid) as count from rejected_voters_2020 where failed_methods like "%MAIL%" and accepted_method = "MAIL" '''
        data = uut.query(sql, ['cnt'])
        mailfixedbymail = data[0]['cnt']
        print(f"rejected voters fixed by mail: {mailfixedbymail} {round(((mailfixedbymail/rejectedfixed)*100),2)}%")


        sql = '''select count(distinct ncid) as count from rejected_voters_2020 where failed_methods like "%MAIL%" and accepted_method = "ONE-STOP" '''
        data = uut.query(sql, ['cnt'])
        mailfixedbymail = data[0]['cnt']
        print(f"rejected voters fixed by mail: {mailfixedbymail} {round(((mailfixedbymail/rejectedfixed)*100),2)}%")


    def test_voters(self):
        voters_reg=7345481
        uut = db()
        sql = "SELECT count (distinct ncid) from NC2020 as cnt "
        voters_unique = uut.query(sql, ['cnt'])[0]['cnt']
        sql = "SELECT count (distinct ncid) from rejected_voters_2020as cnt "
        voters_multi = uut.query(sql, ['cnt'])[0]['cnt']
        sql = 'select count(distinct ncid) from NC2020 where SDR = "Y" and ballot_rtn_status = "ACCEPTED"'
        voters_SDR_total = uut.query(sql, ['cnt'])[0]['cnt']
        # sql = 'select ballot_request_party, count(distinct ncid) from NC2020 where SDR = "Y" and ballot_rtn_status = "ACCEPTED" group by ballot_request_party'
        # voters_SDR = uut.query(sql, ['party', 'cnt'])
        # pprint(voters_SDR)

        voters_one_and_done = voters_unique-voters_multi
        rate_one_and_done = voters_one_and_done/voters_unique
        rate_reject = voters_multi/voters_unique
        print(f"voted:        {voters_unique} of {voters_reg} {round(((voters_unique/voters_reg)*100),2):.2f}%")
        print(f"one and done: {voters_one_and_done} {rate_one_and_done*100:.2f}%")
        print(f"multi       : {voters_multi} {rate_reject*100:.2f}%")
        print(f"same day    : {voters_SDR_total}")

if __name__ == '__main__':
    unittest.main()
