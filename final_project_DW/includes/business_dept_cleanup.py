import pandas as pd
import numpy as np
import openpyxl

def clean_product_list():
    df_products_lists = pd.read_excel('Business Department/product_list.xlsx', engine='openpyxl')
    products = df_products_lists.set_index('product_id')

    # Fix the naming of product names and product types

    # Convert product name to title case
    df_products_lists['product_name'] = df_products_lists['product_name'].str.title()
    unique_product_names = df_products_lists['product_name'].unique()
    print(unique_product_names)

    # Check for special characters
    special_chars_check = df_products_lists['product_name'].str.contains(
        r'[^A-Za-z0-9\s]', regex=True)
    print(df_products_lists[special_chars_check])

    df_products_lists['product_name'] = df_products_lists['product_name'].replace(
        {"Children'S Pants": "Children's Pants"})

    special_chars_check = df_products_lists['product_name'].str.contains(
        r'[^A-Za-z0-9\s]', regex=True)
    print(df_products_lists[special_chars_check])

    # Convert product type to title case

    df_products_lists['product_type'] = df_products_lists['product_type'].str.title()
    unique_product_types = df_products_lists['product_type'].unique()
    print(unique_product_types)

    # Fix wrong product type
    df_products_lists['product_type'] = df_products_lists['product_type'].replace({"Toolss": "Tools"})
    df_products_lists['product_type'] = df_products_lists['product_type'].str.replace('_', ' ')
    unique_product_types = df_products_lists['product_type'].unique()

    # Changing the product type of "stationary", "school supplies", and nan into "Stationary And School Supplies"
    df_products_lists['product_type'] = df_products_lists['product_type'].replace({"Stationary": "Stationary And School Supplies"})
    df_products_lists['product_type'] = df_products_lists['product_type'].replace({"School Supplies": "Stationary And School Supplies"})
    df_products_lists['product_type'] = df_products_lists['product_type'].fillna("Stationary And School Supplies")

    # Changing the product type of "Appliances" and "Technology" into "Electronics And Technology"
    df_products_lists['product_type'] = df_products_lists['product_type'].replace({"Appliances": "Electronics And Technology"})
    df_products_lists['product_type'] = df_products_lists['product_type'].replace({"Technology": "Electronics And Technology"})
    print(unique_product_types)

    # Check if there are values with three or more decimal places

    greater_than_two_decimal_place = df_products_lists['price'].astype(
        str).str.contains(r'\.\d{3,}')
    print(df_products_lists[greater_than_two_decimal_place])

    # Drop "Unnamed" column
    df_products_lists = df_products_lists.drop('Unnamed: 0', axis=1)

    # Check for duplicated product_id
    duplicated_rows = df_products_lists[df_products_lists.duplicated(
        subset='product_id', keep=False)]

    duplicate_counts = duplicated_rows['product_id'].value_counts()
    # Getting all duplicated produc_id
    for product_id, count in duplicate_counts.items():
        print(f"Product ID {product_id} has {count} duplicate rows.")

    duplicate_product_ids = ['PRODUCT10592', 'PRODUCT61036', 'PRODUCT47439',
                            'PRODUCT62030', 'PRODUCT50527', 'PRODUCT38223', 'PRODUCT19599']
    duplicate_rows = df_products_lists[df_products_lists['product_id'].isin(
        duplicate_product_ids)]
    duplicate_rows.sort_values(by='product_id', inplace=True)
    # Converting the product_id into an integer
    df_products_lists['product_id'] = df_products_lists['product_id'].str.replace('PRODUCT', '').astype(int)

    duplicate_product_ids = [10592, 61036, 47439,
                            62030, 50527, 38223, 19599]
    duplicate_rows = df_products_lists[df_products_lists['product_id'].isin(
        duplicate_product_ids)]
    duplicate_rows.sort_values(by='product_id', inplace=True)

    # Add the index value to the product id to make it unique
    duplicate_rows = df_products_lists[df_products_lists.duplicated(
        'product_id', keep=False)].sort_values(by='product_id')

    for index, row in duplicate_rows.iterrows():
        df_products_lists.loc[index, 'product_id'] = df_products_lists.loc[index, 'product_id'] + index

    # Check if there are still duplicate product id
    result_one = df_products_lists[df_products_lists['product_name'] == 'Slipper']
    result_two = df_products_lists[df_products_lists['product_name'] == 'Sofa']
    print(result_one, ' ', result_two)

    duplicate_check = df_products_lists.duplicated('product_id', keep=False)

    if duplicate_check.any():
        print("There are still duplicate product IDs.")
    else:
        print("There are no duplicate product IDs.")

    # Add the PRODUCT again
    df_products_lists['product_id'] = 'PRODUCT' + \
        df_products_lists['product_id'].astype(str)
    print(df_products_lists)
    # Convert to parquet
    df_products_lists.to_parquet(
        'Business Department/product_list.parquet', index=False)
    
if __name__ == "__main__":
    clean_product_list