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

    def import(self, year=2020):
        sql='''
        
        '''

    def reindex(self):
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

    def rejected_voters(self):
        # find voter ids of all voters with more than one entry, ignoring those marked cancelled
        sql = '''SELECT ncid, voter_reg_num, COUNT(*) as count FROM (select * from NC2020 where ballot_rtn_status != "CANCELLED") GROUP BY ncid, voter_reg_num HAVING COUNT(*) > 1 order by count asc '''
        # SELECT ncid, voter_reg_num, COUNT(*) as count FROM NC2020 GROUP BY ncid, voter_reg_num HAVING COUNT(*) > 1 order by count asc
        data = self.query(sql, ['ncid', 'voter_reg_num', 'count'])
        with open('dupes.json', 'w') as outfile:
            # save to a JSON file, just 'cause
            json.dump(data, outfile, indent=4)
        return data

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


    def test_dupes(self):
        self.rejected_voters()



    def test_inv(self):
        with open('dupes.json') as json_file:
            data = json.load(json_file)
        cnt =0
        uut = db()
        for row in tqdm(data):
            cnt+=1
            # if row['count'] <= 2 or row['count'] > 4:
            #     continue
            id = row['ncid']
            cols = ["county_desc", "ballot_rtn_status", "ballot_rtn_dt", "site_name", "ballot_req_type"]
            sql = f'''
            select {', '.join(cols)} from NC2020 where ncid = "{id}" order by ballot_rtn_dt ASC, ballot_rtn_status DESC
            '''
            data = uut.query(sql, cols)
            linestr = f"{data[0]['county_desc']}"
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
                    failed_req_types.append( attempt['ballot_req_type'])
                    failed_site_names.append( attempt['site_name'].replace("'", "''"))
                    failed_ballot_rtn_dt.append(attempt['ballot_rtn_dt'])
                    failed_ballot_rtn_status.append(attempt['ballot_rtn_status'].replace("'", "''"))
            if accepted_ballot_rtn_dt is not None and accepted_ballot_rtn_dt in failed_ballot_rtn_dt:
                # strip date ballot was accepted from list of failed dates
                while accepted_ballot_rtn_dt in failed_ballot_rtn_dt:
                    failed_ballot_rtn_dt.remove(accepted_ballot_rtn_dt)

            dates = sorted([sub['ballot_rtn_dt'] for sub in data])
            req_types = [sub['ballot_req_type'] for sub in data]
            rtn_statuses = [sub['ballot_rtn_status'] for sub in data]
            rtn_site_names = [sub['site_name'].replace("'", "''") for sub in data]

            story = "unknown"
            accepted=-1
            if 'ACCEPTED' in rtn_statuses:
                accepted=1
                if len(set(dates)) == 1:
                    story = f'same day'
                    continue
                elif len(set(req_types)) == 1:
                    story=f'cured via {req_types[0]}'
                else:
                    if len(set(failed_req_types)) > 1:
                        story=f'{",".join(set(failed_req_types))} cured via {accepted_req_type}'
                    else:
                        story=f'{failed_req_types[0]} cured via {accepted_req_type}'
            else:
                accepted=0
                story ='not accepted'
            failed_cnt=len(set(failed_ballot_rtn_dt))
            sql=None
            try:
                sql = f'''INSERT OR REPLACE INTO multivotes2020 (county, ncid, 
                                                                 accepted, accepted_method, accepted_site_name, date_first_rejection, date_accepted, 
                                                                 failed_attempts, failed_methods, failed_dates,
                                                                 failed_ballot_rtn_statuses,
                                                                 failed_site_names)
                     VALUES ('{data[0]["county_desc"]}', '{id}', 
                              {accepted}, '{accepted_req_type}' , '{accepted_site_name}', '{failed_ballot_rtn_dt[0]}', '{accepted_ballot_rtn_dt}',
                              {len(failed_req_types)}, '{",".join(set(failed_req_types))}','{",".join(set(failed_ballot_rtn_dt))}',
                             '{",".join(set(failed_ballot_rtn_status))}',
                             '{",".join(set(failed_site_names))}'
                            );
                   '''
                count = uut.cur.execute(sql)
                if cnt % 10 == 0:
                    uut.conn.commit()
            except Exception as e:
                pprint(data)
                print(sql)
                print(e)

                raise


            # print(f"{cnt} {id} {story} failed {failed_cnt}")
            if story == 'unknown':
                pprint(data)
                print(sql)
                print(e)
                raise


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

    def test_multis(self):
        uut = db()
        sql = "SELECT count (distinct ncid) from NC2020 as cnt "
        voters_unique = uut.query(sql, ['cnt'])[0]['cnt']
        sql = "SELECT count (distinct ncid) from multivotes2020 as cnt "
        multi_voters = uut.query(sql, ['cnt'])[0]['cnt']
        sql = "SELECT count (distinct ncid) from multivotes2020 as cnt  where accepted = 1"
        multi_voters_accepted = uut.query(sql, ['cnt'])[0]['cnt']
        multi_voters_rejected = multi_voters- multi_voters_accepted

        print(f"corrected ballots:   {multi_voters_accepted:6,} of {voters_unique:6,} {round(((multi_voters_accepted/voters_unique)*100),2):.2f}%")
        print(f"outstanding ballots: {multi_voters_rejected:6,} of {voters_unique:6,} {round(((multi_voters_rejected/voters_unique)*100),2):.2f}%")

        # print(f"{voters_accepted} {rate_reject*100:.2f}%")
        # print(f"{voters - voters_accepted} not yet accepted")


    def test_voters(self):
        voters_reg=7345481
        uut = db()
        sql = "SELECT count (distinct ncid) from NC2020 as cnt "
        voters_unique = uut.query(sql, ['cnt'])[0]['cnt']
        sql = "SELECT count (distinct ncid) from multivotes2020 as cnt "
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
