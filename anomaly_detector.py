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
    meta_from_sql = pandas.read_sql('select * from test_meta', con=cnx)
    print(meta_from_sql.describe())
    pairs = meta_from_sql.groupby(['api_name', 'http_method']).size().reset_index(name='counts')
    count_of_pairs = len(pairs)
    print(count_of_pairs)
    print(pairs)
    pairs_in_sql = meta_from_sql[(meta_from_sql['api_name'] == pairs['api_name'][1]) &
        (meta_from_sql['http_method'] == pairs['http_method'][1])]
    print(pairs_in_sql.describe())
    
if __name__ == '__main__':
    action()

'''algoritm
df = pandas.read_csv("test.csv")
df.loc[df.ID == 103, 'FirstName'] = "Matt"
df.loc[df.ID == 103, 'LastName'] = "Jones"
'''