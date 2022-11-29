import env

from sklearn.model_selection import train_test_split
from sklearn.preprocessing import MinMaxScaler

import numpy as np
import pandas as pd

########################## ACQUIRE ##########################
def get_mall_df():
    '''get_mall_df acquires data from codeup database and returns mall dataframe'''
    mall_query = '''
    SELECT *
    FROM customers
    '''
    mall_df = pd.read_sql(mall_query, env.get_db_url('mall_customers'))
    
    return mall_df

######################### SPLIT DATA    #############################
def split_mall_df(df):
    ''' split_mall_customers splits mall_df  into train, validate and test '''
    # split the data
    train_validate, test = train_test_split(df, test_size=0.2, random_state=123)
    
    train, validate = train_test_split(train_validate, test_size=0.3, random_state=123)
    
    return train, validate, test

############################### DUMMIES #############################
def get_dummies(df):
    ''' get_dummies takes dataframe and creates dummies from columns , drops column gender_Male
        returns dataframe'''
    new_df = pd.get_dummies(df)

    new_df.drop(columns=['gender_Male'], inplace=True)

    return new_df

################################ scaler ##############################
def mall_scaler(df):
    '''mall_scaler takes a mall dataframe and scales columns age, annual_income 
    returns data frame'''
    
    mms = MinMaxScaler()

    df[['age', 'annual_income']] = mms.fit_transform(df[['age', 'annual_income']])
    
    return df