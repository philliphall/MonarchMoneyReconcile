########################################################
# Monarch Money Reconciliation Script
# -----------------------------------
#
# Designed by Phil Hall
# Last updated 5/29/2024
#
# Instructions:
# 1) Fill in the ### Configuration ### section below - update these paths to reflect where you will be storing the exports for the script to look for.
# 2) Go to https://app.monarchmoney.com/accounts (logged in) and click "Download CSV" under the Summary widget. Don't change the name from balances.csv, just save it wherever you configured the script to look for exports. 
# 3) Go to https://app.monarchmoney.com/settings/data and click "Download transactions" and save it in the same place.
# 4) Run the script, answer questions, and hope you don't have a mess to clean up :-)
# 
########################################################

import sqlite3
import pandas as pd
import os
import sys
import glob
from datetime import datetime, timedelta
from itertools import combinations
import multiprocessing # Used to more quickly identify combinations of potentially problematic transactions
from decimal import Decimal, getcontext
from math import comb  # Import the comb function for binomial coefficient calculation


### Configuration ###
db_path = "C:\\Users\\Music\\OneDrive\\Documents\\reconciliation.db" # Where you would like our reconiliation database persistenly stored
import_folder = "C:\\Users\\Music\\Downloads\\" # Where to find export tiles from Monarch Money
earliest_reconcile_date = "2020-01-01"  # Specify the earliest date to reconcile


### Initialization ###
# Decimal places suck in computers
getcontext().prec = 28  # Set precision for Decimal operations (2 for actual precision, additional for intermediate calculations)
getcontext().rounding = 'ROUND_HALF_UP'  # Set rounding mode to round to the nearest cent
cent = Decimal('.01')

# Register SQLite3 adapter
def adapt_decimal(d):
    return str(d)
sqlite3.register_adapter(Decimal, adapt_decimal)

# Register SQLite3 converter
def convert_decimal(s):
    return Decimal(s)
sqlite3.register_converter("decimal", convert_decimal)



##########
## MAIN ##
##########
def main():
    # Initialize the database tables
    initialize_db(db_path)

    # Import transactions from the most recent or user-specified CSV file
    transaction_file_path = verify_or_request_file(import_folder, 'transactions*.csv', 'transaction')
    import_transactions(transaction_file_path, db_path, earliest_reconcile_date)

    # Import balance history from the most recent or user-specified CSV file
    balance_file_path = verify_or_request_file(import_folder, '*balances*.csv', 'balance')
    daily_balances_df = load_daily_balances(balance_file_path)

    # Set initial balances if necessary
    set_initial_balances(db_path, balance_df=daily_balances_df, earliest_reconcile_date=earliest_reconcile_date)

    # Attempt to reconcile transactions
    reconcile_accounts(db_path, balance_df=daily_balances_df)

    sys.exit(0)


### Step 1: Initialize Database and Tables
def initialize_db(db_path='reconciliation.db'):
    # Connect to SQLite database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Create a transactions table if it doesn't exist
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS transactions (
            id INTEGER PRIMARY KEY,
            transaction_date TEXT,
            merchant TEXT,
            category TEXT,
            account TEXT,
            original_statement TEXT,
            amount REAL,
            reconciled BOOLEAN DEFAULT 0,
            import_date TEXT,
            reconcile_date TEXT
        )
    ''')
    
    # Create an account_balances table if it doesn't exist
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS account_balances (
            account TEXT PRIMARY KEY,
            last_reconciled_balance REAL,
            last_reconciled_date TEXT
        )
    ''')
    
    # Commit changes and close the connection
    conn.commit()
    conn.close()



### Step 2: Implementing the CSV Import Function
def import_transactions(csv_path, db_path='reconciliation.db', earliest_reconcile_date=None):
    # Load transactions from CSV with consistent formatting
    try:
        transactions = pd.read_csv(csv_path, parse_dates=['Date'], dtype={'Amount': float})
        transactions['Date'] = transactions['Date'].dt.strftime('%Y-%m-%d')  # Normalize date format
    except Exception as e:
        print(f"Failed to load CSV file: {e}")
        sys.exit(1)

    # Define the mapping of CSV column headers to database column names
    column_mapping = {
        'Date': 'transaction_date',
        'Merchant': 'merchant',
        'Category': 'category',
        'Account': 'account',
        'Original Statement': 'original_statement',
        'Amount': 'amount'
    }

    # Verify required columns exist in the CSV
    if not set(column_mapping.keys()).issubset(set(transactions.columns)):
        missing_columns = set(column_mapping.keys()) - set(transactions.columns)
        print(f"Error: CSV is missing required columns: {', '.join(missing_columns)}")
        sys.exit(1)

    # Rename columns to match the database field names
    transactions.rename(columns=column_mapping, inplace=True)

    # Remove any columns not in the column_mapping dictionary (not needed for the database)
    transactions = transactions[list(column_mapping.values())]

    # Filter out transactions before the earliest reconcile date
    if earliest_reconcile_date:
        transactions = transactions[transactions['transaction_date'] >= earliest_reconcile_date]

    # Connect to the SQLite database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Check for existing non-reconciled transactions before the earliest reconcile date
    if earliest_reconcile_date:
        cursor.execute('''
            SELECT id, transaction_date, merchant, amount, account
            FROM transactions
            WHERE reconciled = 0 AND transaction_date < ?
        ''', (earliest_reconcile_date,))
        old_transactions = cursor.fetchall()

        if old_transactions:
            print("There are existing non-reconciled transactions before the earliest reconcile date:")
            for trans in old_transactions:
                print(f"- ID: {trans[0]}, Date: {trans[1]}, Merchant: {trans[2]}, Amount: {trans[3]}, Account: {trans[4]}")

            response = input("These could cause difficulty during the reconcile stage. Do you want to remove these old transactions from the database? (yes/no): ")
            if response.lower() in ['yes', 'y']:
                cursor.execute('DELETE FROM transactions WHERE reconciled = 0 AND transaction_date < ?', (earliest_reconcile_date,))
                conn.commit()
                print(f" - Removed {len(old_transactions)} old non-reconciled transactions from the database.")
            else:
                print(" - Old non-reconciled transactions were not removed.")

    # Fetch all existing transactions for comparison
    cursor.execute('SELECT transaction_date, original_statement, amount, account FROM transactions')
    existing_transactions = pd.DataFrame(cursor.fetchall(), columns=['transaction_date', 'original_statement', 'amount', 'account'])
    
    # Determine new and existing transactions using merge
    comparison_df = pd.merge(transactions, existing_transactions, on=['transaction_date', 'original_statement', 'amount', 'account'], how='outer', indicator=True)
    new_transactions = comparison_df[comparison_df['_merge'] == 'left_only'][transactions.columns]
    unmatched_in_db = comparison_df[comparison_df['_merge'] == 'right_only'][existing_transactions.columns]
    matched_transactions = comparison_df[comparison_df['_merge'] == 'both']

    num_new_transactions = len(new_transactions)
    num_matched_transactions = len(matched_transactions)
    num_unmatched_in_db = len(unmatched_in_db)

    # Insert new transactions into the database if there are any
    if num_new_transactions > 0:
        new_transactions.to_sql('transactions', conn, if_exists='append', index=False)
        print(f" - {num_new_transactions} new transactions added.")
    else:
        print(" - No new transactions to import.")

    print(f" - {num_matched_transactions} transactions matched with existing records.")
    print(f" - {num_unmatched_in_db} transactions in the database not found in the current CSV export.")

    # Close the connection
    conn.close()




### Step 3: User Interaction for Initial Reconciliation
def set_initial_balances(db_path='reconciliation.db', balance_df=None, earliest_reconcile_date=None):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Fetch the earliest transaction date for each account that doesn't have an initial balance set
    cursor.execute('''
        SELECT account, MIN(transaction_date) AS earliest_date
        FROM transactions
        WHERE account NOT IN (SELECT account FROM account_balances)
        GROUP BY account
    ''')
    accounts = cursor.fetchall()

    # Process each account needing an initial balance
    for account, earliest_date in accounts:
        # Calculate the day before the earliest date
        previous_date = (datetime.strptime(earliest_date, '%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d')

        # Check if the balance for the previous date exists in balance_df
        previous_balance_row = balance_df[(balance_df['account'] == account) & (balance_df['date'] == previous_date)]
        if not previous_balance_row.empty:
            previous_balance = previous_balance_row['balance'].values[0]
            print(f"Using balance from {previous_date}, which is the day before the earliest transaction, for account {account}: {previous_balance}")
        else:
            # Display transactions on the earliest date to assist the user
            cursor.execute('''
                SELECT transaction_date, merchant, amount, original_statement
                FROM transactions
                WHERE account = ? AND transaction_date = ?
            ''', (account, earliest_date))
            transactions = cursor.fetchall()
            print(f"I need help identifying the initial balance to reconcile to for account {account}. Earliest transactions are on {earliest_date}:")
            for trans in transactions:
                print(f"- Date: {trans[0]}, Merchant: {trans[1]}, Amount: {trans[2]}, Statement: {trans[3]}")
            
            # Ask the user if they can provide the balance for the day before the earliest transaction date
            response = input(f"Can you provide the closing balance for account {account} as of {previous_date}? (yes/no): ")
            if response.lower() in ['yes', 'y']:
                balance_date = previous_date
            else:
                while True:
                    balance_date = input("Please specify the closing balance date (YYYY-MM-DD) you can provide: ")
                    try:
                        # Validate the date format
                        datetime.strptime(balance_date, '%Y-%m-%d')
                        break  # Exit the loop if the date is valid
                    except ValueError:
                        print("Invalid date format. Please use YYYY-MM-DD.")

            # Prompt user for the balance on the specified date
            while True:
                balance_input = input(f"Enter the closing balance for account {account} as of {balance_date}: ").replace('$', '').replace(',', '')
                try:
                    previous_balance = float(balance_input)  # Convert the cleaned input to float
                    break  # Exit the loop if the conversion is successful
                except ValueError:
                    print("Invalid input. Please enter a valid number. Example: 1234.56 or $1,234.56")
        
        # Insert the initial balance into account_balances
        cursor.execute('''
            INSERT INTO account_balances (account, last_reconciled_balance, last_reconciled_date)
            VALUES (?, ?, ?)
        ''', (account, previous_balance, previous_date))

        # Commit changes after processing each account
        conn.commit()

    # Close connection and wrap up
    conn.close()
    print(" - Last reconciled dates found for all accounts.")



### Step 4: Import current balances
def load_daily_balances(csv_path):
    try:
        # Load the daily balances from the CSV file
        daily_balances = pd.read_csv(csv_path)
        daily_balances.rename(columns={'Date': 'date', 'Balance': 'balance', 'Account': 'account'}, inplace=True)
        
        # Normalize date format if necessary
        daily_balances['date'] = pd.to_datetime(daily_balances['date']).dt.strftime('%Y-%m-%d')
        
        print(" - Daily balances have been loaded successfully.")
        return daily_balances
    except Exception as e:
        print(f"An error occurred while loading daily balances: {e}")
        sys.exit(1)



### Step 5: Reconcile balances
def reconcile_accounts(db_path='reconciliation.db', balance_df=None):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Fetch all accounts
    cursor.execute('SELECT DISTINCT account FROM transactions')
    accounts = [row[0] for row in cursor.fetchall()]

    for account in accounts:
        # Fetch the last reconciled balance and date for the account
        cursor.execute('SELECT last_reconciled_balance, last_reconciled_date FROM account_balances WHERE account = ?', (account,))
        row = cursor.fetchone()
        if row:
            last_reconciled_balance, last_reconciled_date = row
        else:
            print(f"No prior reconciliation data found for account {account}. Exiting the script.")
            conn.close()
            sys.exit(1)
        
        # Fetch the current balance for the account from balance_df
        current_balance_row = balance_df[balance_df['account'] == account].sort_values(by='date', ascending=False).iloc[0]
        current_balance = Decimal(current_balance_row['balance'])
        current_balance_date = current_balance_row['date']

        # Calculate online_balance_change and transaction_balance_change
        online_balance_change = current_balance - Decimal(last_reconciled_balance)
        
        cursor.execute('SELECT SUM(amount) FROM transactions WHERE account = ? AND reconciled = 0', (account,))
        transaction_balance_change = cursor.fetchone()[0]
        if transaction_balance_change is None:
            transaction_balance_change = Decimal('0.0')
        else:
            transaction_balance_change = Decimal(transaction_balance_change)

        # Calculate the discrepancy only to the cent. This compensates for arbitrary extra decimal places
        discrepancy = (online_balance_change - transaction_balance_change).quantize(cent)

        if discrepancy == Decimal('0.00'):
            print(f"All transactions for account {account} have been reconciled.")
            cursor.execute('UPDATE transactions SET reconciled = 1, reconcile_date = ? WHERE account = ? AND reconciled = 0 AND transaction_date <= ?', 
                           (current_balance_date, account, current_balance_date))
            cursor.execute('UPDATE account_balances SET last_reconciled_balance = ?, last_reconciled_date = ? WHERE account = ?', 
                           (current_balance, current_balance_date, account))
            conn.commit()
            continue

        print(f"Discrepancy of {discrepancy} found for account {account}.")

        # Check for discrepancies in the last 5 days and around the last reconciled date
        recent_and_surrounding_transactions = cursor.execute('''
            SELECT id, transaction_date, amount FROM transactions 
            WHERE account = ? AND (transaction_date >= ? OR 
                                   transaction_date BETWEEN ? AND ?) AND reconciled = 0
        ''', (account, 
              (datetime.strptime(current_balance_date, '%Y-%m-%d') - timedelta(days=5)).strftime('%Y-%m-%d'),
              (datetime.strptime(last_reconciled_date, '%Y-%m-%d') - timedelta(days=3)).strftime('%Y-%m-%d'),
              (datetime.strptime(last_reconciled_date, '%Y-%m-%d') + timedelta(days=3)).strftime('%Y-%m-%d'))).fetchall()

        potential_matches = find_matching_transactions_parallel(recent_and_surrounding_transactions, discrepancy)

        if not potential_matches:
            # Check for discrepancies across all unreconciled transactions
            all_unreconciled_transactions = cursor.execute('''
                SELECT id, transaction_date, amount FROM transactions 
                WHERE account = ? AND reconciled = 0
            ''', (account,)).fetchall()
            
            num_transactions = len(all_unreconciled_transactions)
            est_time_all, combinations_count_all = estimate_processing_time(num_transactions)

            # Determine if it's reasonable to just look through all missing transactions
            if est_time_all < 3:
                user_input = 'b'
            
            # Otherwise ask user what they want to do
            else:
                est_time_limited, combinations_count_limited = estimate_processing_time(num_transactions, limit_combos=3)
                user_input = input(f"No combination of transactions found in the last 5 days or around the last reconciled date which if excluded would result in a successful reconcile for account {account}.\n"
                                f"If you would like, I can search for additional combinations of all transactions which, if excluded, could result in a successful reconcile. There are {num_transactions} unreconciled transactions.\n"
                                f"Estimated time to search all {combinations_count_all} combinations: {est_time_all/60:.2f} minutes.\n"
                                f"Estimated time to search {combinations_count_limited} combinations of up to 3 transactions: {est_time_limited/60:.2f} minutes.\n"
                                "Would you like to (a) skip, (b) search all transactions, or (c) search up to 3 in a combo? (a/b/c): ").lower()
                
            if user_input == 'b':
                potential_matches = find_matching_transactions_parallel(all_unreconciled_transactions, discrepancy)
            elif user_input == 'c':
                potential_matches = find_matching_transactions_parallel(all_unreconciled_transactions, discrepancy, limit_combos=3)

        if potential_matches:
            print(f"Potential transactions matching the discrepancy for account {account}:")
            for trans in potential_matches:
                print(f"- Date: {trans[1]}, Amount: {trans[2]}")

            confirm = input("Do you want to mark these transactions as reconciled? (yes/no): ")
            if confirm.lower() in ['yes', 'y']:
                for trans in potential_matches:
                    cursor.execute('UPDATE transactions SET reconciled = 1, reconcile_date = ? WHERE id = ?', (current_balance_date, trans[0]))
                cursor.execute('UPDATE account_balances SET last_reconciled_balance = ?, last_reconciled_date = ? WHERE account = ?', 
                               (current_balance, current_balance_date, account))
                conn.commit()
                print(f"Transactions reconciled for account {account}.")
            else:
                print(f"Transactions not reconciled for account {account}.")
        else:
            print(f"No matching transactions found for the discrepancy in account {account}.")

    conn.close()

def estimate_processing_time(num_transactions, limit_combos=None):
    # Estimate processing time based on the number of combinations
    if limit_combos:
        # Calculate the number of combinations up to the specified limit
        combinations_count = sum(comb(num_transactions, r) for r in range(1, limit_combos + 1))
    else:
        # Use the mathematical approach to calculate the total number of combinations
        combinations_count = 2**num_transactions - 1
    
    est_time = combinations_count / 1e6  # Example: assuming 1 million combinations per second
    return est_time, combinations_count

def process_combinations(transactions, discrepancy, r):
    return [combo for combo in combinations(transactions, r) if sum(trans[2] for trans in combo) == -discrepancy]

def find_matching_transactions_parallel(transactions, discrepancy, limit_combos=None):
    num_transactions = len(transactions)
    pool = multiprocessing.Pool()
    if limit_combos:
        results = pool.starmap(process_combinations, [(transactions, discrepancy, r) for r in range(1, limit_combos + 1)])
    else:
        results = pool.starmap(process_combinations, [(transactions, discrepancy, r) for r in range(1, num_transactions + 1)])
    pool.close()
    pool.join()
    for result in results:
        if result:
            return result
    return None




### HELPER FUNCTIONS

# Confirm latest file in the defined folder matching the expected pattern
def find_most_recent_matching_file(directory, pattern):
    # Construct the full path pattern
    full_pattern = os.path.join(directory, pattern)
    
    # Find all files matching the pattern
    files = glob.glob(full_pattern)
    
    # Find the most recent file based on modification time
    if not files:
        raise Exception(f"No files found matching the pattern {pattern} in the directory: {directory}")
    else:
        latest_file = max(files, key=os.path.getmtime)
        return latest_file

def verify_or_request_file(import_folder, pattern, file_description):
    """
    Attempts to find the most recent file matching a pattern. If not found, prompts the user for the file path.
    Args:
        import_folder (str): The directory to search for files.
        pattern (str): The pattern to match files.
        file_description (str): Description of the file type for user prompts (e.g., 'transaction', 'balance').
    Returns:
        str: The path to the file.
    """
    try:
        file_path = find_most_recent_matching_file(import_folder, pattern)
        print(f"The most recent {file_description} file is: {file_path}")
        return file_path
    except Exception as e:
        print(e)
        while True:
            user_input = input(f"Please enter the full path of the {file_description} file you want to import, or type 'exit' to quit: ")
            if user_input.lower() == 'exit':
                print("Exiting the script.")
                sys.exit(0)
            # Normalize the path entered by the user
            normalized_path = os.path.normpath(user_input)
            if os.path.isfile(normalized_path):
                print(f"File found: {normalized_path}")
                return normalized_path
            else:
                print("File not found, please try again.")


if __name__ == "__main__":
    main()
