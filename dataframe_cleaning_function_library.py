import pandas as pd
import numpy as np

def dataframe_importer(url):
    """This function aims to import the specific csv dataset for the workbook by prompting for the url of the CSV from the github repo.
    This function will then specify the preset specific column names for the workbooks in order to load the csv into the dataframe into df.
     """
    #url = input("Please enter the csv url: ")
    df = pd.read_csv(url)
    return df

def column_renamer(df):
    """This function will use the lamba function to first iterate through all column names to bring them to lowercase, and then replace all spaces with _underscores_
    and renames st column to state before returning the updated dataframe."""
    df.columns = [x.lower() for x in df.columns]
    df.columns = df.columns.str.replace(' ', '_')
    df.rename(columns={"st":"state"}, inplace=True)
    return df

def invalid_value_cleaner(df):
    """This function will scan through specific columns with a preset dictionary of values and replace any matching criteria with the updated element in order to have
    a cleaner dataset. """
    gender_dict = {'Femal':'F', 'female':'F', 'Male':'M'}
    df['gender'] = df['gender'].replace(gender_dict)
    state_dict = {'AZ':'Arizona', 'Cali':'California', 'WA':'Washington'}
    df['state'] = df['state'].replace(state_dict)
    df['education'] = df['education'].replace({'Bachelors':'Bachelor'})
    car_dict = {'Sports Car':'Luxury', 'Luxury SUV':'Luxury', 'Luxury Car':'Luxury'}
    df['vehicle_class'] = df['vehicle_class'].replace(car_dict)
    if df['customer_lifetime_value'].dtype == 'O':
        df['customer_lifetime_value'] = df['customer_lifetime_value'].str.replace('%','')
    return df

def datatype_formatter(df):
    """This function will first set CLV as a float datatype column, and then removes the first two characters from open complaints, and then removing the last 3
    characters, so that 1/5/00 becomes 5; representing the number of open complaints for the particular customer."""
    df['customer_lifetime_value'] = df['customer_lifetime_value'].astype(float)
    if df['number_of_open_complaints'].dtype == 'O' and len(df['number_of_open_complaints'][0]) > 2:
        df['number_of_open_complaints'] = df['number_of_open_complaints'].str.split('/').str[1]
    else:
        print("pass")
    return df

def null_value_method(df):
    """This function will first populate the gender column NaN rows with the mode value, as gender has the most null values in the dataset, and then dropping any
     additional rows containing NaN elements in order to get the most value from the dataset before removing null rows. This means the dataset has 1068 rows instead
     of 952 for further analysis."""
    df['gender'] = df['gender'].fillna(df['gender'].mode()[0])
    df = df.dropna()
    if df['number_of_open_complaints'].dtype != 'int64':
        df['number_of_open_complaints'] = df['number_of_open_complaints'].astype(int)
    return df

def duplicated_formatter(df):
    """This function will take a slice of the dataframe where no duplicated values are detected in the rows. """
    df = df.loc[df.duplicated() == False]
    return df

def column_cleaner_pipeline(url):
    """This function is a pipeline of all the above functions to clean this specific dataframe from start to finish starting with the entry of the url."""
    df = dataframe_importer(url)
    df = column_renamer(df)
    df = invalid_value_cleaner(df)
    df = datatype_formatter(df)
    df = null_value_method(df)
    df = duplicated_formatter(df)
    return df