import psycopg2
from config import config
import pandas as pd
from itertools import product
from gspread_pandas import Spread, Client
import gspread
from datetime import datetime
from google.oauth2 import service_account





def get_data():
    """ query data from the vendors table """
    conn = None
    scoped_credentials = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']
    credentials = service_account.Credentials.from_service_account_file('./service_account.json', scopes=scoped_credentials)
    gc = gspread.service_account(filename='./service_account.json')
    sh = gc.create("OLX_Ihor_Nikolenko_"+datetime.today().strftime('%Y-%m-%d'))
    sh.share('ihor.nikolenko99@gmail.com', perm_type='user', role='writer')
    spread = Spread(spread="OLX_Ihor_Nikolenko_"+datetime.today().strftime('%Y-%m-%d'), creds=credentials)

    df_overlap = pd.DataFrame(columns=('category_row', 'category_col', 'overlap'))
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute('WITH Counted_Ads AS ( '
                    'SELECT DISTINCT user_id, '
                    'category_name, '
                    'category_id, '
                    'COUNT(*) AS Ads ' 
                    'FROM ads '
                    'group by user_id, ' 
                    'category_id, category_name '
                    ') '
                    'SELECT *, RANK() OVER(PARTITION BY user_id ORDER BY Ads DESC) AS Rank_Category '  
                    'FROM Counted_Ads')
        print("The number of parts: ", cur.rowcount)
        row = cur.fetchall()
        df = pd.DataFrame(row, columns=('user_id', 'category', 'category_id', 'Ads', 'Rank_Category'))
        attributs = set(df.category)
        for attribut1, attribut2 in product(attributs, attributs):
                    df1 = df[df.category == attribut1]["user_id"]
                    df2 = df[df.category == attribut2]["user_id"]
                    intersection = len(set(df1).intersection(set(df2)))
                    df_overlap.loc[len(df_overlap) + 1] = [attribut1, attribut2, intersection]

        pvt = df_overlap.pivot_table(index=['category_row'], columns=['category_col'], values='overlap', aggfunc='sum')
        spread.df_to_sheet(pvt, index=True, sheet='Test task', start='A1', replace=True)
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


if __name__ == '__main__':
    get_data()