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
import time


### Configuration ###
db_path = "C:\\Users\\Music\\OneDrive\\Documents\\reconciliation.db" # Where you would like our reconiliation database persistenly stored
import_folder = "C:\\Users\\Music\\Downloads\\testing\\" # Where to find export tiles from Monarch Money
earliest_reconcile_date = "2023-01-01"  # Specify the earliest date to reconcile


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

# We'll use this to calculate estimated time to process potential missing transaction combinations
time_per_combination = None



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



### Step 2: Importing Transactionns
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

    # Fill missing values in 'original_statement' with an empty string
    transactions.fillna({'original_statement': ''}, inplace=True)

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
                print(" - Old non-reconciled transactions were NOT removed.")

    # Fetch all existing transactions for comparison
    cursor.execute('SELECT id, transaction_date, merchant, amount, account, original_statement, reconciled FROM transactions')
    existing_transactions = pd.DataFrame(cursor.fetchall(), columns=['id', 'transaction_date', 'merchant', 'amount', 'account', 'original_statement', 'reconciled'])
    
    # Fill missing values in 'original_statement' with an empty string in existing transactions as well
    existing_transactions.fillna({'original_statement': ''}, inplace=True)

    # Specify suffixes to identify columns from each dataframe
    comparison_df = pd.merge(
        transactions, existing_transactions,
        on=['transaction_date', 'original_statement', 'amount', 'account'],
        how='outer', indicator=True, suffixes=('_new', '_existing')
    )

    # Resolve the 'merchant' column conflict by choosing one or combining them
    comparison_df['merchant'] = comparison_df.apply(
        lambda x: x['merchant_new'] if pd.notna(x['merchant_new']) else x['merchant_existing'],
        axis=1
    )

    # Drop the original 'merchant_x' and 'merchant_y' columns
    comparison_df.drop(columns=['merchant_new', 'merchant_existing'], inplace=True)

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

    # Display unmatched transactions and prompt user for action
    if num_unmatched_in_db > 0:
        print("\nUnmatched transactions in the database:")
        print(f"{'ID':<5} {'Account':<15} {'Date':<15} {'Merchant':<25} {'Amount':<10} {'Reconciled':<10}")
        print("-" * 80)
        for idx, row in unmatched_in_db.sort_values(by=['account', 'transaction_date']).iterrows():
            print(f"{row['id']:<5} {row['account']:<15} {row['transaction_date']:<15} {row['merchant']:<25} {row['amount']:<10} {row['reconciled']:<10}")

        response = input("\nWould you like to delete all, none, or select individually? (all/none/select): ").lower()

        if response in ['all', 'a']:
            cursor.execute('DELETE FROM transactions WHERE id IN ({seq})'.format(seq=','.join(['?']*num_unmatched_in_db)), unmatched_in_db['id'].tolist())
            conn.commit()
            print(" - All unmatched transactions have been deleted from the database.")
        elif response in ['select', 'sel', 's']:
            for idx, row in unmatched_in_db.iterrows():
                action = input(f"Delete transaction ID {row['id']} (Account: {row['account']}, Date: {row['transaction_date']}, Merchant: {row['merchant']}, Amount: {row['amount']}, Reconciled: {row['reconciled']})? (yes/no): ").lower()
                if action in ['yes', 'y']:
                    cursor.execute('DELETE FROM transactions WHERE id = ?', (row['id'],))
                    conn.commit()
                    print(f" - Deleted transaction ID {row['id']}.")
                else:
                    print(f" - Kept transaction ID {row['id']}.")
        else:
            print(" - No transactions were deleted.")

    # Close the connection
    conn.close()



### Step 3: Import current balances
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
        
        
        
### Step 4: User Interaction for Initial Reconciliation
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
            print(f"\n\nAll transactions for account {account} add up and have been reconciled.")
            cursor.execute('UPDATE transactions SET reconciled = 1, reconcile_date = ? WHERE account = ? AND reconciled = 0', 
                           (current_balance_date, account))
            cursor.execute('UPDATE account_balances SET last_reconciled_balance = ?, last_reconciled_date = ? WHERE account = ?', 
                           (current_balance, current_balance_date, account))
            conn.commit()
            continue

        print(f"\n\nDiscrepancy of {discrepancy} found for account {account}.")

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

        # If we didn't get recent or surrounding matches
        if not potential_matches:
            # Check for discrepancies across all unreconciled transactions
            all_unreconciled_transactions = cursor.execute('''
                SELECT id, transaction_date, amount FROM transactions 
                WHERE account = ? AND reconciled = 0
            ''', (account,)).fetchall()
            
            num_transactions = len(all_unreconciled_transactions)
            est_all = estimate_processing_time(num_transactions)

            # Determine if it's reasonable to just look through all missing transactions
            if est_all[0] < 30 and (est_all[0] > 0 or num_transactions < 10):
                user_input = 'a'
            
            # Otherwise ask user what they want to do
            else:
                est_1 = estimate_processing_time(num_transactions, limit_combos=1)
                est_3 = estimate_processing_time(num_transactions, limit_combos=3)
                est_5 = estimate_processing_time(num_transactions, limit_combos=5)
                est_10 = estimate_processing_time(num_transactions, limit_combos=10)
                user_input = input(
                    f"No combination of transactions found in the last 5 days or around the last reconciled date which if excluded would result in a successful reconcile for account {account}.\n"
                    f"If you would like, I can search for additional combinations of all transactions which, if excluded, could result in a successful reconcile. There are {num_transactions} unreconciled transactions.\n"
                    f"Options:\n"
                    f" - (a)  Estimated time to search all {est_all[1]} combinations: {est_all[0] / 60:.2f} minutes.\n"
                    f" - (1)  Estimated time to search {est_1[1]} combinations of up to 1 transaction: {est_1[0] / 60:.2f} minutes.\n"
                    f" - (3)  Estimated time to search {est_3[1]} combinations of up to 3 transactions: {est_3[0] / 60:.2f} minutes.\n"
                    f" - (5)  Estimated time to search {est_5[1]} combinations of up to 5 transactions: {est_5[0] / 60:.2f} minutes.\n"
                    f" - (10) Estimated time to search {est_10[1]} combinations of up to 10 transactions: {est_10[0] / 60:.2f} minutes.\n"
                    f" - (0)  skip searching.\n\n"
                    f"Your selection? : ").lower()
                
            if user_input == 'a':
                potential_matches = find_matching_transactions_parallel(all_unreconciled_transactions, discrepancy)
            elif user_input.isdigit():
                combo_limit = int(user_input)
                if combo_limit > 0:
                    potential_matches = find_matching_transactions_parallel(all_unreconciled_transactions, discrepancy, limit_combos=combo_limit)
                else:
                    print(f"Skipping reconciliation for account {account}.\n\n")
                    continue
            else:
                print(f"Skipping reconciliation for account {account}.\n\n")
                continue
        
        # If we have potential matches...
        if potential_matches:
            print(f"Potential transactions matching the discrepancy for account {account}:")

            for i, combo in enumerate(potential_matches):
                print(f"Combination {i + 1}:")
                print(f"{'ID':<5} {'Date':<15} {'Merchant':<25} {'Amount':<10}")
                print("-" * 60)
                for trans in combo:
                    transaction_details = get_transaction_details_by_id(cursor, trans[0])
                    if transaction_details:
                        print(f"{trans[0]:<5} {transaction_details[0]:<15} {transaction_details[1]:<25} {transaction_details[2]:<10}")
                    else:
                        print(f"{trans[0]:<5} {trans[1]:<15} {'NOT FOUND':<25} {trans[2]:<10}")
                        print(f"  - Date: {trans[1]}, Amount: {trans[2]}")
                print("\n")

            # Get user selection
            selected_combo_index = input("Select the combination number to skip for a successful reconciliation (or -1 to not reconcile this account): ")
            try:
                selected_combo_index = int(selected_combo_index)
            except:
                selected_combo_index = -1
            
            # Begin acting on that selection
            if selected_combo_index > 0 and selected_combo_index <= len(potential_matches):
                selected_combo = potential_matches[selected_combo_index - 1]
                selected_ids = [trans[0] for trans in selected_combo]

                # Verify the sum of amounts matches the expected change in balance
                cursor.execute('SELECT SUM(amount) FROM transactions WHERE account = ? AND reconciled = 0 AND id NOT IN ({seq})'.format(seq=','.join(['?']*len(selected_ids))),
                               (account, *selected_ids))
                verified_sum = cursor.fetchone()[0]
                if verified_sum is None:
                    verified_sum = Decimal('0.0')
                else:
                    verified_sum = Decimal(verified_sum).quantize(cent)

                expected_change = online_balance_change.quantize(cent)

                if verified_sum == expected_change:
                    # Ask the user which transactions to exclude and which to delete
                    exclude_ids = []
                    delete_ids = []
                    for trans_id in selected_ids:
                        action = input(f"Transaction ID {trans_id} should be (e)xcluded from reconciliation or (d)eleted from the database? (e/d): ").lower()
                        if action == 'd':
                            delete_ids.append(trans_id)
                        else:
                            exclude_ids.append(trans_id)
                    
                    # Proceed with reconciliation, excluding selected transactions
                    if exclude_ids:
                        cursor.execute('UPDATE transactions SET reconciled = 1, reconcile_date = ? WHERE account = ? AND reconciled = 0 AND id NOT IN ({seq})'.format(seq=','.join(['?']*len(exclude_ids))), 
                                       (current_balance_date, account, *exclude_ids))
                    
                    # Delete transactions that were marked for deletion
                    if delete_ids:
                        cursor.execute('DELETE FROM transactions WHERE id IN ({seq})'.format(seq=','.join(['?']*len(delete_ids))), delete_ids)
                        print(f"Transaction ids {delete_ids} deleted. Be sure to delete them in Monarch as well, or they will just come back again.")
                        
                    cursor.execute('UPDATE account_balances SET last_reconciled_balance = ?, last_reconciled_date = ? WHERE account = ?', 
                                (current_balance, current_balance_date, account))
                    conn.commit()
                    print(f"All transactions except the selected ones have been reconciled for account {account}.")
                else:
                    print(f"Sum of amounts to be reconciled ({verified_sum}) does not match the expected change in balance ({expected_change}). No transactions were reconciled.")
            else:
                print(f"Transactions not reconciled for account {account}.")
        else:
            print(f"No matching transactions found for the discrepancy in account {account}.")

    conn.close()

def estimate_processing_time(num_transactions, limit_combos=None):
    global time_per_combination

    if limit_combos:
        combinations_count = sum(comb(num_transactions, r) for r in range(1, limit_combos + 1))
    else:
        combinations_count = 2**num_transactions - 1

    # Ensure we have time_per_combination calculated already.
    if time_per_combination is None:
        print(f"Can't accurately estimate processing time yet. Please execure find_matching_transactions_parallel at least once before calling this function.")
        return -1, combinations_count

    # Estimate total time based on time per combination
    est_time = combinations_count * time_per_combination

    print(f"Estimated processing time: {est_time / 60:.2f} minutes for {combinations_count} combinations.")
    return est_time, combinations_count

def process_combinations(transactions, discrepancy, r):
    # Create a list to hold the valid combinations
    valid_combinations = []
    
    # Generate all combinations of size 'r' from the 'transactions' list
    for combo in combinations(transactions, r):
        # Calculate the sum of the amounts in the combination
        combo_sum = Decimal(sum(Decimal(trans[2]) for trans in combo)).quantize(cent)
        
        # Check if the sum matches the negative of the discrepancy
        if combo_sum == -discrepancy:
            # If it matches, add the combination to the valid_combinations list
            valid_combinations.append(combo)
    
    # Return the list of valid combinations
    return valid_combinations

def find_matching_transactions(transactions, discrepancy, limit_combos=None):
    global time_per_combination
    num_transactions = len(transactions)
    if num_transactions < 1:
        return None

    # Calculate the number of combinations
    if limit_combos:
        number_of_combinations = sum(comb(num_transactions, r) for r in range(1, limit_combos + 1))
    else:
        number_of_combinations = 2**num_transactions - 1

    # Configurable threshold for using non-parallel processing
    non_parallel_threshold = 1000  # Adjust this value based on performance tests and your specific environment

    # Decide whether to use parallel or non-parallel based on the number of combinations
    if number_of_combinations < non_parallel_threshold:
        return find_matching_transactions_serial(transactions, discrepancy, limit_combos)
    else:
        return find_matching_transactions_parallel(transactions, discrepancy, limit_combos)

def find_matching_transactions_serial(transactions, discrepancy, limit_combos=None):
    if limit_combos:
        r_values = range(1, limit_combos + 1)
    else:
        r_values = range(1, len(transactions) + 1)

    for r in r_values:
        for combo in combinations(transactions, r):
            combo_sum = Decimal(sum(Decimal(trans[2]) for trans in combo)).quantize(cent)
            if combo_sum == -discrepancy:
                return [combo]
    return None

def find_matching_transactions_parallel(transactions, discrepancy, limit_combos=None):
    global time_per_combination
    num_transactions = len(transactions)
    if num_transactions < 1:
        return None

    pool = multiprocessing.Pool()
    if limit_combos:
        r_values = range(1, limit_combos + 1)
    else:
        r_values = range(1, num_transactions + 1)

    # Start measuring time if it's the first run and we have a meaningful number of transactions
    if time_per_combination is None and num_transactions > 3:
        start_time = time.time()

    # Submit tasks to the pool
    results = [pool.apply_async(process_combinations, args=(transactions, discrepancy, r)) for r in r_values]

    # Wait for all results to complete and retrieve them
    results = [res.get() for res in results]

    # Calculate time_per_combination on the first run
    if time_per_combination is None and num_transactions > 3:
        end_time = time.time()
        time_per_combination = (end_time - start_time) / sum(comb(num_transactions, r) for r in r_values)
    
    pool.close()
    pool.join()
    
    # Search for a valid result in the results list
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

# Initially used for looking up merchant details from the trans tuples in potential_matches
def get_transaction_details_by_id(cursor, transaction_id):
    cursor.execute('SELECT transaction_date, merchant, amount FROM transactions WHERE id = ?', (transaction_id,))
    return cursor.fetchone()


if __name__ == "__main__":
    main()
