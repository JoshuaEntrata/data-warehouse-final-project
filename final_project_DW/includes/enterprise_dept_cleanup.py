import pandas as pd
import numpy as np
import html5lib
import lxml
from bs4 import BeautifulSoup


def clean_enterprise():
    # Loading Merchant Data

    df_merchant_data = pd.read_html('Enterprise Department/merchant_data.html')
    

    # Drop Unnamed column
    df_merchant_data[0] = df_merchant_data[0].drop(columns=['Unnamed: 0'])

    # Fix the naming of name, street, state, city, and country
    columns_to_title_case = ['name', 'street', 'state', 'city', 'country']
    
    for column in columns_to_title_case:
        df_merchant_data[0][column] = df_merchant_data[0][column].str.title()

    # Remove future dates
    df_merchant_data[0]['creation_date'] = pd.to_datetime(
        df_merchant_data[0]['creation_date'])
    df_merchant_data[0] = df_merchant_data[0][df_merchant_data[0]
                                            ['creation_date'] <= pd.to_datetime('now')]

    ### Fix the format of contact number

    # Remove special characters in contact number
    df_merchant_data[0]['contact_number'] = df_merchant_data[0]['contact_number'].str.replace(
        r'\D', '', regex=True)
    # Check if there are contact number values that are not 10 digits
    df_merchant_data[0]['contact_number_length'] = df_merchant_data[0]['contact_number'].apply(
        len)
    rows_with_invalid_contact_numbers = df_merchant_data[0][df_merchant_data[0]
                                                            ['contact_number_length'] != 10]
    
    # Update values of contact number that are not 10 digits and start with 1 by removing the 1 in the beginning
    mask = (df_merchant_data[0]['contact_number_length'] != 10) & (
        df_merchant_data[0]['contact_number'].str.startswith('1'))
    df_merchant_data[0].loc[mask,
                            'contact_number'] = df_merchant_data[0].loc[mask, 'contact_number'].str[1:]

    df_merchant_data[0]['contact_number_length'] = df_merchant_data[0]['contact_number'].apply(
        len)

    # Drop contact number length column
    df_merchant_data[0] = df_merchant_data[0].drop(
        columns=['contact_number_length'])
    df_merchant_data[0].head()

    # Update the format of contact number
    def format_contact_number(number):
        return f"({number[:3]}) {number[3:6]}-{number[6:]}"

    df_merchant_data[0]['contact_number'] = df_merchant_data[0]['contact_number'].apply(
        format_contact_number)

    # Adding 1 to the duplicated merchant_id
    df_merchant_data[0]['merchant_id'] = df_merchant_data[0]['merchant_id'].str.replace(
        'MERCHANT', '').astype(int)

    df_merchant_data[0]['merchant_id'] = df_merchant_data[0].groupby('merchant_id').cumcount().add(
        1).astype(str).radd('MERCHANT') + df_merchant_data[0]['merchant_id'].astype(str)

    duplicate_check = df_merchant_data[0].duplicated('merchant_id', keep=False)

    if duplicate_check.any():
        print("There are still duplicate merchant IDs.")
    else:
        print("There are no duplicate merchant IDs.")

    # Convert to parquet
    df_merchant_data[0].to_parquet(
        'Enterprise Department/merchant_data.parquet', index=False)

    # ## Staff Data
    df_staff_data = pd.read_html('Enterprise Department/staff_data.html')

    # Dropping unnamed column
    df_staff_data[0] = df_staff_data[0].drop(columns=['Unnamed: 0'])

    # ## Convert name, job level, street, state, city, and country to title case
    columns_to_title_case = ['name', 'job_level',
                            'street', 'state', 'city', 'country']

    for column in columns_to_title_case:
        df_staff_data[0][column] = df_staff_data[0][column].str.title()

    # ## Fix contact number values
    df_staff_data[0]['contact_number'] = df_staff_data[0]['contact_number'].str.replace(
        r'\D', '', regex=True)
    
    # Update the values of contact number that are not 10 digits and start with 1
    df_staff_data[0]['contact_number_length'] = df_staff_data[0]['contact_number'].apply(
    len)
    mask = (df_staff_data[0]['contact_number_length'] == 11) & (
        df_staff_data[0]['contact_number'].str.startswith('1'))
    df_staff_data[0].loc[mask, 'contact_number'] = df_staff_data[0].loc[mask,
                                                                        'contact_number'].str[1:]

    df_staff_data[0]['contact_number_length'] = df_staff_data[0]['contact_number'].apply(
        len)
    rows_with_invalid_contact_numbers = df_staff_data[0][df_staff_data[0]
                                                        ['contact_number_length'] != 10]


    # Remove future dates
    df_staff_data[0]['creation_date'] = pd.to_datetime(
        df_staff_data[0]['creation_date'])
    df_staff_data[0] = df_staff_data[0][df_staff_data[0]
                                        ['creation_date'] <= pd.to_datetime('now')]

    # ## Adding 1 to staff_id to make it unique
    # df_staff_data[0]['staff_id'] = df_staff_data[0]['staff_id'].str.replace(
    #     'STAFF', '').astype(int)
    # df_staff_data[0].head()

    # df_staff_data[0]['staff_id'] = df_staff_data[0].groupby('staff_id').cumcount().add(
    #     1).astype(str).radd('STAFF') + df_staff_data[0]['staff_id'].astype(str)

    # duplicate_check = df_staff_data[0].duplicated('staff_id', keep=False)

    # if duplicate_check.any():
    #     print("There are still duplicate staff IDs.")
    # else:
    #     print("There are no duplicate staff IDs.")

    # Remove contact number length column
    df_staff_data[0] = df_staff_data[0].drop(columns=['contact_number_length'])

    # Update format of contact number
    def format_contact_number(number):
        return f"({number[:3]}) {number[3:6]}-{number[6:]}"

    df_staff_data[0]['contact_number'] = df_staff_data[0]['contact_number'].apply(
        format_contact_number)

    print(df_staff_data[0].head())

    # ## Convert to parquet
    df_staff_data[0].to_parquet(
        'Enterprise Department/staff_data.parquet', index=False)
    print('Successfully saved the parquet file.')


if __name__ == '__main__':
    clean_enterprise()
