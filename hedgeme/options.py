import pandas as pd


NASDAQ_ROOT = 'https://www.nasdaq.com/symbol/%s/option-chain?dateindex=-1&page=%d'
MONTHS = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']


def trimMonths(df, field='CDate'):
    ret = df[field].str.contains(MONTHS[0])
    for month in MONTHS[1:]:
        ret = ret | df[field].str.contains(month)
    return ~ret


def fetch(ticker='aapl'):
    dfs = []
    i = 1

    while True:
        print('fetching page %d' % i)
        df = pd.read_html(NASDAQ_ROOT % ('aapl', i))[2]
        df.columns = ['CDate', 'CLast', 'CChg', 'CBid', 'CAsk', 'CVol', 'COpenInt', 'Symbol', 'Strike', 'PDate', 'PLast', 'PChg', 'PBid', 'PAsk', 'PVol', 'POpenInt']
        df = df[df.index > 0]
        df = df[trimMonths(df)]

        # wide to long
        df1 = df[['Symbol', 'Strike', 'CDate', 'CLast', 'CChg', 'CBid', 'CAsk', 'CVol', 'COpenInt']]
        df1.columns = ['Symbol', 'Strike', 'Date', 'Last', 'Chg', 'Bid', 'Ask', 'Vol', 'OpenInt']
        df1['Type'] = 'C'

        df2 = df[['Symbol', 'Strike', 'PDate', 'PLast', 'PChg', 'PBid', 'PAsk', 'PVol', 'POpenInt']]
        df2.columns = ['Symbol', 'Strike', 'Date', 'Last', 'Chg', 'Bid', 'Ask', 'Vol', 'OpenInt']
        df2['Type'] = 'P'

        df = pd.concat([df1, df2])

        if i == 1:
            dfs.append(df)
            i += 1
        elif df.equals(dfs[-1]):
            # duplicate page
            break
        else:
            dfs.append(df)
            i += 1

    df = pd.concat(dfs).reset_index(drop=True)
    return df
