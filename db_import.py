from arango import ArangoClient
from arango.http import DefaultHTTPClient

from urllib3.util.retry import Retry
from time import time
import pandas as pd
import os

from requests import Session
from requests.adapters import HTTPAdapter

class RetryHTTPClient(DefaultHTTPClient):
    """Default HTTP client implementation."""

    REQUEST_TIMEOUT = 60
    RETRY_ATTEMPTS = 3
    BACKOFF_FACTOR = 1
    
    def create_session(self, host: str) -> Session:
        """Create and return a new session/connection.

        :param host: ArangoDB host URL.
        :type host: str
        :returns: requests session object
        :rtype: requests.Session
        """
        retry_strategy = Retry(
            total=self.RETRY_ATTEMPTS,
            backoff_factor=self.BACKOFF_FACTOR,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"],
        )
        http_adapter = HTTPAdapter(max_retries=retry_strategy)

        session = Session()
        session.mount("https://", http_adapter)
        session.mount("http://", http_adapter)

        return session

def import_data():
    # dict must be accessed with CURRENCY_RATES["2022-09-01"]["BRL"]
    CURRENCY_RATES ={
    "2022-09-01":{
        "AUD":1.471646,
        "BRL":5.241964,
        "CAD":1.314774,
        "CHF":0.981368,
        "CNY":6.90707,
        "EUR":1.004838,
        "GBP":0.865952,
        "ILS":3.397908,
        "INR":79.718162,
        "JPY":140.085934,
        "MXN":20.182196,
        "RUB":60.211011,
        "USD":1
    },
    "2022-09-02":{
        "AUD":1.468255,
        "BRL":5.172534,
        "CAD":1.313383,
        "CHF":0.98123,
        "CNY":6.900132,
        "EUR":1.004268,
        "GBP":0.868834,
        "ILS":3.373476,
        "INR":79.714323,
        "JPY":140.214323,
        "MXN":19.949052,
        "RUB":60.276605,
        "USD":1
    },
    "2022-09-03":{
        "AUD":1.468228,
        "BRL":5.172518,
        "CAD":1.313388,
        "CHF":0.981135,
        "CNY":6.900116,
        "EUR":1.004279,
        "GBP":0.868832,
        "ILS":3.373507,
        "INR":79.714576,
        "JPY":140.214582,
        "MXN":19.9489,
        "RUB":60.276479,
        "USD":1
    },
    "2022-09-04":{
        "AUD":1.472786,
        "BRL":5.172637,
        "CAD":1.314371,
        "CHF":0.983419,
        "CNY":6.900075,
        "EUR":1.009201,
        "GBP":0.871535,
        "ILS":3.372685,
        "INR":79.725745,
        "JPY":140.49288,
        "MXN":19.9743,
        "RUB":60.210918,
        "USD":1
    },
    "2022-09-05":{
        "AUD":1.468033,
        "BRL":5.155499,
        "CAD":1.313182,
        "CHF":0.978622,
        "CNY":6.934276,
        "EUR":1.004968,
        "GBP":0.86478,
        "ILS":3.408269,
        "INR":79.791528,
        "JPY":140.444262,
        "MXN":19.98085,
        "RUB":61.01115,
        "USD":1
    },
    "2022-09-06":{
        "AUD":1.485959,
        "BRL":5.248833,
        "CAD":1.315559,
        "CHF":0.984581,
        "CNY":6.954192,
        "EUR":1.010375,
        "GBP":0.868731,
        "ILS":3.418286,
        "INR":79.896754,
        "JPY":143.156927,
        "MXN":20.13615,
        "RUB":61.275769,
        "USD":1
    },
    "2022-09-07":{
        "AUD":1.48125,
        "BRL":5.248299,
        "CAD":1.313087,
        "CHF":0.977056,
        "CNY":6.966386,
        "EUR":1.000486,
        "GBP":0.868123,
        "ILS":3.430407,
        "INR":79.651966,
        "JPY":144.134197,
        "MXN":19.997252,
        "RUB":64.300931,
        "USD":1
    },
    "2022-09-08":{
        "AUD":1.478107,
        "BRL":5.214853,
        "CAD":1.308188,
        "CHF":0.968724,
        "CNY":6.95777,
        "EUR":0.998977,
        "GBP":0.86807,
        "ILS":3.434778,
        "INR":79.707577,
        "JPY":143.86176,
        "MXN":19.948927,
        "RUB":61.5012,
        "USD":1
    },
    "2022-09-09":{
        "AUD":1.460084,
        "BRL":5.147871,
        "CAD":1.303619,
        "CHF":0.959028,
        "CNY":6.926575,
        "EUR":0.985062,
        "GBP":0.862014,
        "ILS":3.409571,
        "INR":79.656944,
        "JPY":142.507959,
        "MXN":19.891134,
        "RUB":60.786101,
        "USD":1
    },
    "2022-09-10":{
        "AUD":1.460074,
        "BRL":5.147956,
        "CAD":1.305478,
        "CHF":0.958982,
        "CNY":6.926789,
        "EUR":0.984879,
        "GBP":0.862012,
        "ILS":3.409595,
        "INR":79.65703,
        "JPY":142.50927,
        "MXN":19.89099,
        "RUB":61.501114,
        "USD":1
    },
    "2022-09-11":{
        "AUD":1.460947,
        "BRL":5.147903,
        "CAD":1.302564,
        "CHF":0.959987,
        "CNY":6.926664,
        "EUR":0.992971,
        "GBP":0.860702,
        "ILS":3.409274,
        "INR":79.658207,
        "JPY":142.602479,
        "MXN":19.874419,
        "RUB":62.220979,
        "USD":1
    },
    "2022-09-12":{
        "AUD":1.450636,
        "BRL":5.093645,
        "CAD":1.298171,
        "CHF":0.953012,
        "CNY":6.92664,
        "EUR":0.987264,
        "GBP":0.855508,
        "ILS":3.364782,
        "INR":79.356768,
        "JPY":142.591141,
        "MXN":19.835088,
        "RUB":60.070834,
        "USD":1
    },
    "2022-09-13":{
        "AUD":1.483103,
        "BRL":5.191425,
        "CAD":1.316424,
        "CHF":0.96056,
        "CNY":6.925196,
        "EUR":1.001971,
        "GBP":0.869159,
        "ILS":3.424107,
        "INR":79.603667,
        "JPY":144.409828,
        "MXN":20.047476,
        "RUB":63.750893,
        "USD":1
    },
    "2022-09-14":{
        "AUD":1.481161,
        "BRL":5.164298,
        "CAD":1.316147,
        "CHF":0.962118,
        "CNY":6.962077,
        "EUR":1.001765,
        "GBP":0.86626,
        "ILS":3.425553,
        "INR":79.456064,
        "JPY":142.927207,
        "MXN":19.966927,
        "RUB":59.72577,
        "USD":1
    },
    "2022-09-15":{
        "AUD":1.495473,
        "BRL":5.247722,
        "CAD":1.324971,
        "CHF":0.962027,
        "CNY":6.994654,
        "EUR":1.001222,
        "GBP":0.873218,
        "ILS":3.429735,
        "INR":79.851591,
        "JPY":143.370679,
        "MXN":20.095134,
        "RUB":59.626719,
        "USD":1
    },
    "2022-09-16":{
        "AUD":1.489851,
        "BRL":5.252259,
        "CAD":1.326791,
        "CHF":0.964967,
        "CNY":6.983946,
        "EUR":0.99849,
        "GBP":0.876088,
        "ILS":3.431081,
        "INR":79.691241,
        "JPY":142.92944,
        "MXN":20.037763,
        "RUB":60.500953,
        "USD":1
    },
    "2022-09-17":{
        "AUD":1.477362,
        "BRL":5.253463,
        "CAD":1.326525,
        "CHF":0.965319,
        "CNY":6.983987,
        "EUR":0.998485,
        "GBP":0.866839,
        "ILS":3.431095,
        "INR":79.65151,
        "JPY":142.958368,
        "MXN":20.038266,
        "RUB":60.501276,
        "USD":1
    },
    "2022-09-18":{
        "AUD":1.487022,
        "BRL":5.252327,
        "CAD":1.325903,
        "CHF":0.963311,
        "CNY":6.980114,
        "EUR":0.997819,
        "GBP":0.87472,
        "ILS":3.433668,
        "INR":79.6909,
        "JPY":142.770869,
        "MXN":20.034615,
        "RUB":64.370941,
        "USD":1
    },
    "2022-09-19":{
        "AUD":1.485185,
        "BRL":5.172494,
        "CAD":1.324433,
        "CHF":0.964154,
        "CNY":7.006814,
        "EUR":0.99711,
        "GBP":0.873943,
        "ILS":3.444978,
        "INR":79.710024,
        "JPY":143.230112,
        "MXN":19.91864,
        "RUB":60.076441,
        "USD":1
    },
    "2022-09-20":{
        "AUD":1.493893,
        "BRL":5.143122,
        "CAD":1.336586,
        "CHF":0.963926,
        "CNY":7.018436,
        "EUR":1.002979,
        "GBP":0.87875,
        "ILS":3.448626,
        "INR":79.764286,
        "JPY":143.605604,
        "MXN":19.991663,
        "RUB":64.401823,
        "USD":1
    },
    "2022-09-21":{
        "AUD":1.512891,
        "BRL":5.171995,
        "CAD":1.34798,
        "CHF":0.968035,
        "CNY":7.049118,
        "EUR":1.01728,
        "GBP":0.888479,
        "ILS":3.461271,
        "INR":80.053103,
        "JPY":144.321764,
        "MXN":20.047524,
        "RUB":61.601748,
        "USD":1
    },
    "2022-09-22":{
        "AUD":1.504914,
        "BRL":5.117464,
        "CAD":1.347191,
        "CHF":0.97592,
        "CNY":7.078179,
        "EUR":1.016278,
        "GBP":0.887815,
        "ILS":3.494197,
        "INR":81.064733,
        "JPY":142.296707,
        "MXN":19.924117,
        "RUB":61.501564,
        "USD":1
    },
    "2022-09-23":{
        "AUD":1.531847,
        "BRL":5.26566,
        "CAD":1.358807,
        "CHF":0.98165,
        "CNY":7.128583,
        "EUR":1.032053,
        "GBP":0.920955,
        "ILS":3.509297,
        "INR":81.238042,
        "JPY":143.344473,
        "MXN":20.199491,
        "RUB":57.876426,
        "USD":1
    },
    "2022-09-24":{
        "AUD":1.531894,
        "BRL":5.26483,
        "CAD":1.360309,
        "CHF":0.981103,
        "CNY":7.128534,
        "EUR":1.032053,
        "GBP":0.920965,
        "ILS":3.509263,
        "INR":81.256612,
        "JPY":143.379694,
        "MXN":20.200144,
        "RUB":57.876167,
        "USD":1
    },
    "2022-09-25":{
        "AUD":1.531343,
        "BRL":5.260434,
        "CAD":1.358174,
        "CHF":0.982486,
        "CNY":7.128584,
        "EUR":1.032174,
        "GBP":0.926282,
        "ILS":3.510835,
        "INR":81.257143,
        "JPY":143.515998,
        "MXN":20.216605,
        "RUB":57.250777,
        "USD":1
    },
    "2022-09-26":{
        "AUD":1.544683,
        "BRL":5.391141,
        "CAD":1.371147,
        "CHF":0.992256,
        "CNY":7.149134,
        "EUR":1.03928,
        "GBP":0.928397,
        "ILS":3.515704,
        "INR":81.616997,
        "JPY":144.466266,
        "MXN":20.375477,
        "RUB":58.501749,
        "USD":1
    },
    "2022-09-27":{
        "AUD":1.553784,
        "BRL":5.379753,
        "CAD":1.372429,
        "CHF":0.991734,
        "CNY":7.178086,
        "EUR":1.042134,
        "GBP":0.932314,
        "ILS":3.500797,
        "INR":81.726634,
        "JPY":144.696653,
        "MXN":20.373967,
        "RUB":58.901207,
        "USD":1
    }
}
    
    # Add BTC manually
    CURRENCY_RATES["2022-09-27"]["BTC"] = 1 / 19221.84	
    CURRENCY_RATES["2022-09-26"]["BTC"] = 1 / 18803.90
    CURRENCY_RATES["2022-09-25"]["BTC"] = 1 / 18936.31
    CURRENCY_RATES["2022-09-24"]["BTC"] = 1 / 19296.99	
    CURRENCY_RATES["2022-09-23"]["BTC"] = 1 / 19412.40
    CURRENCY_RATES["2022-09-22"]["BTC"] = 1 / 18534.65
    CURRENCY_RATES["2022-09-21"]["BTC"] = 1 / 18891.28
    CURRENCY_RATES["2022-09-20"]["BTC"] = 1 / 19545.59	
    CURRENCY_RATES["2022-09-19"]["BTC"] = 1 / 19418.57	
    CURRENCY_RATES["2022-09-18"]["BTC"] = 1 / 20127.23
    CURRENCY_RATES["2022-09-17"]["BTC"] = 1 / 19777.03	
    CURRENCY_RATES["2022-09-16"]["BTC"] = 1 / 19704.01
    CURRENCY_RATES["2022-09-15"]["BTC"] = 1 / 20242.29
    CURRENCY_RATES["2022-09-14"]["BTC"] = 1 / 20184.55
    CURRENCY_RATES["2022-09-13"]["BTC"] = 1 / 22371.48
    CURRENCY_RATES["2022-09-12"]["BTC"] = 1 / 21770.15
    CURRENCY_RATES["2022-09-11"]["BTC"] = 1 / 21678.54
    CURRENCY_RATES["2022-09-10"]["BTC"] = 1 / 21376.91
    CURRENCY_RATES["2022-09-09"]["BTC"] = 1 / 19328.14
    CURRENCY_RATES["2022-09-08"]["BTC"] = 1 / 19289.94
    CURRENCY_RATES["2022-09-07"]["BTC"] = 1 / 18837.68
    CURRENCY_RATES["2022-09-06"]["BTC"] = 1 / 19817.72
    CURRENCY_RATES["2022-09-05"]["BTC"] = 1 / 19988.79
    CURRENCY_RATES["2022-09-04"]["BTC"] = 1 / 19832.47
    CURRENCY_RATES["2022-09-03"]["BTC"] = 1 / 19969.72
    CURRENCY_RATES["2022-09-02"]["BTC"] = 1 / 20126.07
    CURRENCY_RATES["2022-09-01"]["BTC"] = 1 / 20050.50

    CURRENCY_RATES["2022-09-27"]["SAR"] = 0.2658
    CURRENCY_RATES["2022-09-26"]["SAR"] = 0.2659
    CURRENCY_RATES["2022-09-25"]["SAR"] = 0.2657
    CURRENCY_RATES["2022-09-24"]["SAR"] = 0.2657
    CURRENCY_RATES["2022-09-23"]["SAR"] = 0.2657
    CURRENCY_RATES["2022-09-22"]["SAR"] = 0.2657
    CURRENCY_RATES["2022-09-21"]["SAR"] = 0.2657
    CURRENCY_RATES["2022-09-20"]["SAR"] = 0.2658
    CURRENCY_RATES["2022-09-19"]["SAR"] = 0.2659
    CURRENCY_RATES["2022-09-18"]["SAR"] = 0.2660
    CURRENCY_RATES["2022-09-17"]["SAR"] = 0.2660
    CURRENCY_RATES["2022-09-16"]["SAR"] = 0.2660
    CURRENCY_RATES["2022-09-15"]["SAR"] = 0.2661
    CURRENCY_RATES["2022-09-14"]["SAR"] = 0.2659
    CURRENCY_RATES["2022-09-13"]["SAR"] = 0.2660
    CURRENCY_RATES["2022-09-12"]["SAR"] = 0.2660
    CURRENCY_RATES["2022-09-11"]["SAR"] = 0.2661
    CURRENCY_RATES["2022-09-10"]["SAR"] = 0.2661
    CURRENCY_RATES["2022-09-09"]["SAR"] = 0.2661
    CURRENCY_RATES["2022-09-08"]["SAR"] = 0.2660
    CURRENCY_RATES["2022-09-07"]["SAR"] = 0.2661
    CURRENCY_RATES["2022-09-06"]["SAR"] = 0.2661
    CURRENCY_RATES["2022-09-05"]["SAR"] = 0.2660
    CURRENCY_RATES["2022-09-04"]["SAR"] = 0.2660
    CURRENCY_RATES["2022-09-03"]["SAR"] = 0.2660
    CURRENCY_RATES["2022-09-02"]["SAR"] = 0.2660
    CURRENCY_RATES["2022-09-01"]["SAR"] = 0.2661

    LONG_CURRENCY_TO_SHORT = {
        "UK Pound": "GBP",
        "Rupee": "INR",
        "US Dollar": "USD",
        "Bitcoin": "BTC",
        "Euro": "EUR",
        "Canadian Dollar": "CAD",
        "Shekel": "ILS",
        "Yuan": "CNY",
        "Swiss Franc": "CHF",
        "Australian Dollar": "AUD",
        "Ruble": "RUB",
        "Saudi Riyal": "SAR",
        "Mexican Peso": "MXN",
        "Yen": "JPY",
        "Brazil Real": "BRL"
    }


    client = ArangoClient(hosts="http://arangodb_db_container:8529", http_client=RetryHTTPClient())

    # Connect to "_system" database as root user.
    sys_db = None
    retry = True
    print("Connecting to ArangoDB...")
    while retry:
        try:
            sys_db = client.db("_system", username=str("root"), password=str("Blogchain"))
            sys_db.has_database("Transactions")
            retry = False
        except BaseException:
            print("Waiting for ArangoDB to start...")
            time.sleep(1)

    df = pd.read_csv("/data/data.csv")
    # turn From Bank from int to string
    df["From Bank"] = df["From Bank"].astype(str)
    df["To Bank"] = df["To Bank"].astype(str)
    # turn Timestamp into datetime
    df["Timestamp"] = pd.to_datetime(df["Timestamp"])

    # Create a new database named "DDI_MC2".
    DB_NAME = os.environ.get("DB_NAME")

    if not sys_db.has_database(DB_NAME):
        sys_db.create_database(DB_NAME)
    trx_db = client.db(DB_NAME, username=str("root"), password=str("Blogchain"))

    if not trx_db.has_graph("bank"):
        trx_db.create_graph("bank")
    graph = trx_db.graph("bank")

    if not trx_db.has_collection("accounts"):
        accounts = trx_db.create_collection("accounts")
        # get all accounts from df
        account_dict = dict()
        for index, row in df.iterrows():
            bank = row["From Bank"]
            acc = row["Account"]
            if acc not in account_dict.keys():
                account_dict[acc] = bank
            bank = row["To Bank"]
            acc = row["Account.1"]
            if acc not in account_dict:
                account_dict[acc] = bank

        print(f"Found {len(account_dict)} accounts")

        for acc, bank in account_dict.items():
            accounts.insert({"_key": acc, "bank": bank})
    
    if not graph.has_edge_definition("transactions"):
        transactions = graph.create_edge_definition(
            edge_collection="transactions",
            from_vertex_collections=["accounts"],
            to_vertex_collections=["accounts"]
        )
        
        for index, row in df.iterrows():
            from_acc = row["Account"]
            to_acc = row["Account.1"]
            receiving_currency = row["Receiving Currency"]
            payment_currency = row["Payment Currency"]
            date_str = row["Timestamp"].strftime("%Y-%m-%d")
            try:
                amount = row["Amount Paid"] / CURRENCY_RATES[date_str][LONG_CURRENCY_TO_SHORT[payment_currency]]
            except KeyError:
                amount = row["Amount Paid"]
            payment_format = row["Payment Format"]
            is_laundering = row["Is Laundering"]
            weekday = row["Timestamp"].weekday()
            transactions.insert({
                "_from": f"accounts/{from_acc}",
                "_to": f"accounts/{to_acc}",
                # TODO in CHF umwechseln "amount": amount
                "amount": amount,
                "receiving_currency": receiving_currency,
                "payment_currency": payment_currency,
                "payment_format": payment_format,
                "is_laundering": is_laundering,
                # encode date into different features
                "monday": weekday == 0,
                "tuesday": weekday == 1,
                "wednesday": weekday == 2,
                "thursday": weekday == 3,
                "friday": weekday == 4,
                "saturday": weekday == 5,
                "sunday": weekday == 6,
                "hour": row["Timestamp"].hour + row["Timestamp"].minute / 60,
                "day": row["Timestamp"].day
            })
            if index % 1000 == 0:
                print(f"Inserted {index} transactions")


if __name__ == '__main__':
    import_data()