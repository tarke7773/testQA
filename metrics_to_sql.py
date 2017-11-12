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
    raw_data = pandas.read_csv("raw_data.csv")

    min_ts = raw_data['ts'].min()

    max_ts = raw_data['ts'].max()

    start_ts = min_ts
    while True:
        stamps = pandas.date_range(start_ts, periods=2, freq='15min')
        print('\n\nPeriod: '+str(stamps[0])+' -- '+str(stamps[1]))
        interval = raw_data[(raw_data['ts'] >= str(stamps[0])) & (raw_data['ts'] < str(stamps[1]))]
        
        if interval.empty:
            if str(stamps[1]) >= max_ts:
                print('\n\nFinish')
                return
            else:
                start_ts = stamps[1]
                continue
            
        lins_with_cod = interval[interval['cod'] > 499]
        pairs_req_count = lins_with_cod.groupby(['api_name', 'mtd']).size().reset_index(name='counts')
        num_of_pairs = len(pairs_req_count.api_name)
        nums = [i for i in range(num_of_pairs)]
        for num in nums:
            pair_all_info = lins_with_cod[(lins_with_cod['api_name'] == pairs_req_count['api_name'][num]) &
                (lins_with_cod['mtd'] == pairs_req_count['mtd'][num])]
        
            timeframe_start = pair_all_info['ts'].min()
            api_name =pairs_req_count['api_name'][num]
            http_method = pairs_req_count['mtd'][num]
            count_http_code_5xx = pairs_req_count['counts'][num]
            is_anomaly = False
        
            res_dict = [('timeframe_start', [timeframe_start]), ('api_name', [api_name]), ('http_method', [http_method]),
               ('count_http_code_5xx', [count_http_code_5xx]), ('is_anomaly', [is_anomaly])]  
        
            res_df = pandas.DataFrame.from_items(res_dict)
            
            res_df.to_sql(name='test_meta', con=cnx, if_exists = 'append', index=False)
            
            print(res_df)
            
        start_ts = stamps[1]

            
if __name__=='__main__':
    action()