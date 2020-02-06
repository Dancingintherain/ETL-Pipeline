from Extract import Extract
import pandas as pd
#import numpy as np

# TODO: add helper Class for helper methods - more generic!
# convert df types to smaller types for
def convert_columns(df):

  for col in df.columns:
    col_type = df[col].dtype
    if (col_type == 'int64'):
            df[col] = df[col].astype('int32')
    if (col_type == 'float64'):
            df[col] = df[col].astype('float32')
  return df

class Transformation:

    def __init__(self, dataSource, dataSet):
        extractObj = Extract()
        if dataSource == 'api':
            self.data = extractObj.getAPISData(dataSet)
        elif dataSource == 'csv':
            self.data = extractObj.getCSVData(dataSet)
        else:
            self.data = extractObj.databases(dataSet)

    # Wohndauer data transformation
    # example of some cleaning steps of csv data
    def csvWohndauer(self):

        # drop unnecessary columns
        self.data.drop(['ZEIT','BEZ','PGR','BZR','PLR','STADTRAUM'], axis = 1, inplace = True, errors = 'ignore')

        # change dataype from object to float for useful calculations
        self.data['PDAU10'] = self.data['PDAU10'].apply(lambda x: pd.to_numeric(x.replace(',','.'), errors='coerce'))
        self.data['PDAU5'] = self.data['PDAU5'].apply(lambda x: pd.to_numeric(x.replace(',','.'), errors='coerce'))

        # convert datatypes to smaller ones for better storage
        print(self.data.dtypes)
        df = convert_columns(self.data)
        print(df.dtypes)

        # TODO: save to database
        # saving new csv file
        df.to_csv('WHNDAUER2017_Matrix_cleaned.csv')
