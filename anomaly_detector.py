import pandas

from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL


# sqlalchemy engine
engine = create_engine(URL(
    drivername="mysql",
    username="root",
    password="root",
    host="localhost",
    database="testQA"
))

cnx = engine.connect()

def action():
    intervals_in_day = 24*4
    
    meta_from_sql = pandas.read_sql('select * from test_meta', con=cnx)

    pairs = meta_from_sql.groupby(['api_name', 'http_method']).size().reset_index(name='counts')
    count_of_pairs = len(pairs)
    pair_nums = [i for i in range(count_of_pairs)]
    for num in pair_nums:
        print(pairs['api_name'][num]+" : "+pairs['http_method'][num])

        pair_in_sql = meta_from_sql[(meta_from_sql['api_name'] == pairs['api_name'][num]) &
            (meta_from_sql['http_method'] == pairs['http_method'][num])]

        count_of_zero_intervals = intervals_in_day - pair_in_sql.count_http_code_5xx.count()

        list_of_zero = [0]*count_of_zero_intervals
        zero_intervals = pandas.DataFrame.from_items([('count_http_code_5xx', list_of_zero)])

        cont_for_each_interval = pair_in_sql.count_http_code_5xx.append(zero_intervals.count_http_code_5xx, ignore_index=True)
    
        code_name_mean = cont_for_each_interval.mean()
        code_name_std = cont_for_each_interval.std()
    
        third_sigma = 3*code_name_std
    
        mean_plus_th_sigma = code_name_mean + third_sigma
        
        print(mean_plus_th_sigma)
        print('\n')
    
        meta_from_sql.loc[(meta_from_sql['api_name'] == pairs['api_name'][num]) &
            (meta_from_sql['http_method'] == pairs['http_method'][num]) &
            (meta_from_sql['count_http_code_5xx'] > mean_plus_th_sigma), 'is_anomaly'] = 1
    meta_from_sql.to_sql(name='test_meta', con=cnx, if_exists = 'replace', index=False)
    
    
if __name__ == '__main__':
    action()

'''algoritm
df = pandas.read_csv("test.csv")
df.loc[df.ID == 103, 'FirstName'] = "Matt"
df.loc[df.ID == 103, 'LastName'] = "Jones"
'''