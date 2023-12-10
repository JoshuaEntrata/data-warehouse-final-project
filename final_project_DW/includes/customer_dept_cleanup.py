import pandas as pd
import numpy as np

def clean_customer_data():
    # # Load User Credit Card
    df_user_cc = pd.read_pickle(
        'Customer Management Department/user_credit_card.pickle')

    # ## Fix the naming of name and issuing_bank

    # #### Fix the name of the banks
    df_user_cc['issuing_bank'] = df_user_cc['issuing_bank'].replace({'chinabank': 'China Bank', 'bpi': 'BPI', 'bdo': 'BDO', 'metrobank': 'Metrobank',
                                                                    'mayabank': 'Maya Bank', 'robinsonsbank': 'Robinsons Bank', 'securitybank': 'Security Bank', 'eastwest': 'EastWest'})

    unique_issuing_banks = df_user_cc['issuing_bank'].unique()
    print(unique_issuing_banks)

    # #### Convert name to title case
    df_user_cc['name'] = df_user_cc['name'].str.title()
    unique_issuing_names = df_user_cc['name'].unique()
    print(unique_issuing_names)

    # ## Remove invalid credit card number

    # Remove rows that are not 10 digits in credit card number

    df_user_cc = df_user_cc[df_user_cc['credit_card_number'].astype(
        str).apply(len) == 10]

    df_user_cc['credit_card_number'] = df_user_cc['credit_card_number'].astype(str)
    not_ten_digits_check = ~df_user_cc['credit_card_number'].str.match(r'^\d{10}$')
    print(df_user_cc[not_ten_digits_check])

    # ## Check for duplicate user id
    duplicate_user_ids = df_user_cc[df_user_cc.duplicated(
        subset='user_id', keep=False)]

    duplicate_user_ids.sort_values(by='user_id', inplace=True)
    print(duplicate_user_ids)

    # Adding 1 to duplicated user_id to make it unique
    df_user_cc['user_id'] = df_user_cc['user_id'].str.replace(
        'USER', '').astype(int)

    df_user_cc['user_id'] = df_user_cc.groupby('user_id').cumcount().add(
        1).astype(str).radd('USER') + df_user_cc['user_id'].astype(str)

    duplicate_user_ids = df_user_cc[df_user_cc.duplicated(
        subset='user_id', keep=False)]

    duplicate_user_ids.sort_values(by='user_id', inplace=True)
    duplicate_check = df_user_cc.duplicated('user_id', keep=False)

    if duplicate_check.any():
        print("There are still duplicate user IDs.")
    else:
        print("There are no duplicate user IDs.")

    # ## Convert to parquet
    df_user_cc.to_parquet(
        'Customer Management Department/user_credit_card.parquet', index=False)

    # # User Data
    df_user_data = pd.read_json('Customer Management Department/user_data.json')

    # ## Fix the naming of name, street, state, city, country, gender, and user type

    # #### Convert them to title case

    df_user_data['name'] = df_user_data['name'].str.title()
    df_user_data['street'] = df_user_data['street'].str.title()
    df_user_data['state'] = df_user_data['state'].str.title()
    df_user_data['city'] = df_user_data['city'].str.title()
    df_user_data['country'] = df_user_data['country'].str.title()
    df_user_data['gender'] = df_user_data['gender'].str.title()
    df_user_data['user_type'] = df_user_data['user_type'].str.title()

    print('Unified the character casing:\n', df_user_data)

    # ## Check if creation date and birthdate is valid
    date_columns = ['creation_date', 'birthdate']

    for column in date_columns:
        try:
            df_user_data[column] = pd.to_datetime(df_user_data[column])
            print(f"{column} is a valid datetime.")
        except ValueError:
            print(f"{column} contains invalid datetime values.")

    # Remove future dates
    df_user_data = df_user_data[(df_user_data['creation_date'] <= pd.to_datetime(
        'now')) & (df_user_data['birthdate'] <= pd.to_datetime('now'))]

    # ## Fix for duplicate user id
    duplicate_user_ids = df_user_data[df_user_data.duplicated(
        subset=['user_id'], keep=False)]

    sorted_duplicate_user_ids = duplicate_user_ids.sort_values(by=['user_id'])

    df_user_data['user_id'] = df_user_data['user_id'].str.replace(
        'USER', '').astype(int)

    df_user_data['user_id'] = df_user_data.groupby('user_id').cumcount().add(
        1).astype(str).radd('USER') + df_user_data['user_id'].astype(str)

    duplicate_check = df_user_data.duplicated('user_id', keep=False)

    if duplicate_check.any():
        print("There are still duplicate user IDs.")
    else:
        print("There are no duplicate user IDs.")

    # ## Convert to parquet
    df_user_data.to_parquet(
        'Customer Management Department/user_data.parquet', index=False)

    # # load User Job
    df_user_job = pd.read_csv('Customer Management Department/user_job.csv')

    # ## Drop Unnamed column
    df_user_job = df_user_job.drop(columns=['Unnamed: 0'])

    # ## Check for duplicate rows with different user id
    duplicate_rows = df_user_job[df_user_job.duplicated(
        subset=df_user_job.columns.difference(['user_id']), keep=False)]

    print(duplicate_rows)

    # #### Remove duplicate row with different user id
    df_user_job = df_user_job.drop_duplicates(
        subset=['name', 'job_title', 'job_level'], keep='first')

    duplicate_rows = df_user_job[df_user_job.duplicated(
        subset=df_user_job.columns.difference(['user_id']), keep=False)]

    print('Remaining duplicated rows:\n',duplicate_rows)

    # ## Check for duplicate user id
    duplicate_user_jobs = df_user_job[df_user_job.duplicated(
        subset=['user_id'], keep=False)]

    sorted_duplicate_user_jobs = duplicate_user_jobs.sort_values(by=['user_id'])

    print('Duplicated user_id:\n', sorted_duplicate_user_jobs)

    # # Adding 1 to make user_id unique
    # df_user_job['user_id'] = df_user_job['user_id'].str.replace(
    #     'USER', '').astype(int)

    # df_user_job['user_id'] = df_user_job.groupby('user_id').cumcount().add(
    #     1).astype(str).radd('USER') + df_user_job['user_id'].astype(str)

    # duplicate_check = df_user_job.duplicated('user_id', keep=False)

    # if duplicate_check.any():
    #     print("There are still duplicate user IDs.")
    # else:
    #     print("There are no duplicate user IDs.")

    # Convert to parquet file
    df_user_job.to_parquet(
        'Customer Management Department/user_job.parquet', index=False)
    print('Successfully saved to parquet file.')

if __name__ == '__main__':
    clean_customer_data()