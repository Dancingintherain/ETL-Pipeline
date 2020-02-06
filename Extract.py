import pandas
import requests
import json

class Extract:

    def __init__(self):
       # loading json file to use it across different class methods
        self.data_sources = json.load(open('data_config.json'))
        self.api = self.data_sources['data_sources']['api']
        self.csv_path = self.data_sources['data_sources']['csv']

    def getCSVData(self, csv_name):
        # csv name as argument - since multiple csv files in the future
        # TODO: made df generic when sep etc. differs
        df = pandas.read_csv(self.csv_path[csv_name], sep = ';', encoding='latin1',
                 compression='infer')
        return df

    def getAPISData(self, api_name):

        api_url = self.api[api_name]
        response = requests.get(api_url)
        return response.json()
