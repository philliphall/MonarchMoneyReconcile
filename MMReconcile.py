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
import multiprocessing  # Used to more quickly identify combinations of potentially problematic transactions
from decimal import Decimal, getcontext
from math import comb, ceil  # Import the comb function for binomial coefficient calculation
import random
from timeit import default_timer as timer
import shutil


### Configuration ###
db_path = "C:\\Users\\Music\\OneDrive\\Documents\\MonarchReconciliation\\reconciliation.db" # Where you would like our reconiliation database persistenly stored
max_backups = 20
import_folder = "C:\\Users\\Music\\Downloads\\" # Where to find export tiles from Monarch Money
earliest_reconcile_date = "2023-01-01"  # Specify the earliest date to reconcile - format yyyy-mm-dd
combine_sofi_vaults = True # Okay, this is a weird special case. SoFi bank offers "Vaults" as part of their Savings Account, and some aggregators treat these weird. This will combine the transactions and balances of any accounts that match "SoFi Vault" into the account matching "SoFi Savings" - of which there must be only one or I don't know what will happen.


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
reasonable_time_threshold = 10 # seconds of estimated processing time the script will accept without first prompting the user.




##########
## MAIN ##
##########
def main():
    # Backup the database before making any changes
    backup_database(db_path, max_backups)

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

def backup_database(db_path, max_backups=20):
    if not os.path.exists(db_path):
        print("No database found to backup.")
        return

    # Determine the backup folder path
    backup_folder = os.path.join(os.path.dirname(db_path), "reconciliation_backups")

    # Create the backup folder if it doesn't exist
    if not os.path.exists(backup_folder):
        os.makedirs(backup_folder)

    # Create a dated backup of the database
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    backup_path = os.path.join(backup_folder, f"reconciliation_backup_{timestamp}.db")
    shutil.copy2(db_path, backup_path)
    print(f" - Database backed up to {backup_path}")

    # Get a list of existing backups
    backups = sorted(
        [f for f in os.listdir(backup_folder) if f.startswith("reconciliation_backup_")],
        key=lambda x: os.path.getmtime(os.path.join(backup_folder, x))
    )

    # If there are more backups than allowed, delete the oldest ones
    extra_backups = len(backups) - max_backups
    if extra_backups > 0:
        for old_backup in backups[:extra_backups]:
            old_backup_path = os.path.join(backup_folder, old_backup)
            os.remove(old_backup_path)
            print(f" - Deleted old backup {old_backup_path}")



### Step 2: Importing Transactions
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

    # Combine SoFi Vault transactions with SoFi Savings
    if combine_sofi_vaults:
        transactions = combine_SoFi_vault_transactions(transactions)   

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
        print("\nUnmatched transactions in the database (usually they were pending before):")
        print(f"{'ID':<5} {'Account':<20} {'Date':<12} {'Merchant':<25} {'Amount':<10} {'Reconciled':<10}")
        print("-" * 85)
        for idx, row in unmatched_in_db.sort_values(by=['account', 'transaction_date']).iterrows():
            print(f"{row['id']:<5} {row['account'][:19]:<20} {row['transaction_date']:<12} {row['merchant'][:24]:<25} {row['amount']:<10} {row['reconciled']:<10}")

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

def combine_SoFi_vault_transactions(transactions):
    # Identify all rows where the account contains "SoFi Vault"
    sofi_vault_transactions = transactions[transactions['account'].str.contains("SoFi Vault", case=False)]
    
    if not sofi_vault_transactions.empty:
        # Retrieve the exact name of the "SoFi Savings" account from the transactions DataFrame
        sofi_savings_account = transactions[transactions['account'].str.contains("SoFi Savings", case=False)]['account'].iloc[0]
        
        # Update the 'account' field to the exact "SoFi Savings" account name for these transactions
        transactions.loc[transactions['account'].str.contains("SoFi Vault", case=False), 'account'] = sofi_savings_account
        print(" - Associated SoFi Vault transactions with SoFi Savings account successfully.")
    else:
        print(" - No SoFi Vault transactions found to associate.")
    
    return transactions



### Step 3: Import current balances
def load_daily_balances(csv_path):
    try:
        # Load the daily balances from the CSV file
        daily_balances = pd.read_csv(csv_path)
        daily_balances.rename(columns={'Date': 'date', 'Balance': 'balance', 'Account': 'account'}, inplace=True)
        
        # Normalize date format if necessary
        daily_balances['date'] = pd.to_datetime(daily_balances['date']).dt.strftime('%Y-%m-%d')

        # Attempt combining SoFi Vaults if specified
        if combine_sofi_vaults:
            combine_SoFi_vault_balances(daily_balances)
        
        print(" - Daily balances have been loaded successfully.")
        return daily_balances
    except Exception as e:
        print(f"An error occurred while loading daily balances: {e}")
        sys.exit(1)
        
def combine_SoFi_vault_balances(daily_balances):
    # Identify all rows where the account contains "SoFi Vault"
    sofi_vaults = daily_balances[daily_balances['account'].str.contains("SoFi Vault", case=False)]
    
    # Sum the balances of these accounts by date
    vault_sums = sofi_vaults.groupby('date')['balance'].sum().reset_index()
    
    # Identify the SoFi Savings account
    sofi_savings = daily_balances[daily_balances['account'].str.contains("SoFi Savings", case=False)]
    
    if not sofi_savings.empty:
        # Merge the sums of the SoFi Vault accounts with the SoFi Savings balances by date
        merged = pd.merge(sofi_savings, vault_sums, on='date', how='left', suffixes=('_savings', '_vaults'))
        
        # Replace NaN values in vault balances with 0
        merged['balance_vaults'] = merged['balance_vaults'].fillna(0)
        
        # Add the balances of the SoFi Vault accounts to the SoFi Savings account balances
        merged['balance'] = merged['balance_savings'] + merged['balance_vaults']
        
        # Update the original daily_balances DataFrame with the new combined balances
        for idx, row in merged.iterrows():
            daily_balances.loc[
                (daily_balances['date'] == row['date']) & 
                (daily_balances['account'] == row['account']), 
                'balance'
            ] = row['balance']
        
        print(" - Combined SoFi Vault balances into SoFi Savings account successfully.")
    else:
        print(" - SoFi Savings account not found in the daily balances.")
    
    return daily_balances

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
    reconciliation_summary = {}  # Initialize
    
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
        
        cursor.execute('SELECT * FROM transactions WHERE account = ? AND reconciled = 0', (account,))
        all_unreconciled_transactions = cursor.fetchall()
        all_unreconciled_transactions_filtered = [(trans[0], trans[1], trans[6]) for trans in all_unreconciled_transactions]
        transaction_balance_change = sum(Decimal(trans[6]) for trans in all_unreconciled_transactions)

        # Calculate the discrepancy only to the cent. This compensates for arbitrary extra decimal places
        discrepancy = (online_balance_change - transaction_balance_change).quantize(cent)

        reconciliation_summary[account] = {
            "initial_balance": Decimal(last_reconciled_balance).quantize(cent),
            "online_balance": Decimal(current_balance).quantize(cent),
            "transaction_discrepancy": discrepancy
        }

        if discrepancy == Decimal('0.00'):
            cursor.execute('UPDATE transactions SET reconciled = 1, reconcile_date = ? WHERE account = ? AND reconciled = 0', 
                           (current_balance_date, account))
            cursor.execute('UPDATE account_balances SET last_reconciled_balance = ?, last_reconciled_date = ? WHERE account = ?', 
                           (current_balance, current_balance_date, account))
            conn.commit()
            print(f"\nAll transactions for account {account} add up and have been reconciled.")
            reconciliation_summary[account]["result"] = "Fully reconciled with no discrepancies."
            continue

        # There's a discrepancy. Start researching it.
        print(f"\n\nDiscrepancy of {discrepancy} found for account {account}.")
        resolved = False

        # Get all non-reconciled transactions as a DataFrame for later use
        all_unreconciled_df = pd.DataFrame(all_unreconciled_transactions, columns=['id', 'transaction_date', 'merchant', 'category', 'account', 'original_statement', 'amount', 'reconciled', 'import_date', 'reconcile_date'])

        ## SIMPLE SEARCHING ##
        # Check for transactions in the last 5 days that resolve the discrepancy
        last_5_days_transactions = cursor.execute('''
            SELECT id, transaction_date, amount FROM transactions 
            WHERE account = ? AND transaction_date >= ?
        ''', (account, (datetime.strptime(current_balance_date, '%Y-%m-%d') - timedelta(days=5)).strftime('%Y-%m-%d'))).fetchall()

        # Estimate processing time for last 5 days transactions
        num_last_5_days_transactions = len(last_5_days_transactions)
        est_last_5_days = estimate_processing_time(num_last_5_days_transactions)

        # Decide if we are going to process these
        proceed = False
        if est_last_5_days[0] < reasonable_time_threshold and (est_last_5_days[0] > 0 or num_last_5_days_transactions < 10):  
            proceed = True
        else: 
            print(f"Too many transactions ({num_last_5_days_transactions}) in the last 5 days to identify likely pending transactions that should be excluded from the reconcile balance efficiently. Estimated processing time: {est_last_5_days[0]:.2f} seconds.")
            user_input = input("Do you want to proceed with looking for matches within these transactions? (yes/no): ").lower()
            if user_input in ['yes', 'y']:
                proceed = True
                print(f"I will start looking!")
            else:
                print(f"Skipping search for pending transactions in the last 5 days.")
        
        # Send these transactions for processing
        if proceed == True:
            potential_matches = find_matching_transactions(last_5_days_transactions, discrepancy)
            if potential_matches:
                if len(potential_matches) == 1:
                    print("Identified transactions in the last 5 days that resolve the discrepancy, usually pending transactions:")
                    resolved = process_potential_matches(cursor, account, potential_matches, current_balance_date, current_balance, display_only=True)
                    if resolved:
                        conn.commit()
                        reconciliation_summary[account]["result"] = "Reconciled with transactions pending."
                        continue
                else:
                    # Send these transactions for more normal processing
                    potential_matches = find_matching_transactions(last_5_days_transactions, discrepancy)
                    if potential_matches:
                        print("Identified transactions in the last 5 days that may resolve the discrepancy, usually pending transactions. Watch the ids for duplicates that may result from pending transactions.")
                        resolved = process_potential_matches(cursor, account, potential_matches, current_balance_date, current_balance)
                        if resolved:
                            conn.commit()
                            reconciliation_summary[account]["result"] = "Reconciled with exclusions/deletions after simple searching."
                        continue
        
        # Fast check for single transaction exact matches
        exact_matches = [trans for trans in all_unreconciled_transactions if Decimal(trans[6]).quantize(cent) == -discrepancy]
        if exact_matches:
            print("Exact matches found that directly resolve the discrepancy:")
            
            # Process these exact matches
            resolved = process_potential_matches(cursor, account, [exact_matches], current_balance_date, current_balance)
            if resolved:
                conn.commit()
                reconciliation_summary[account]["result"] = "Reconciled directly with exact matches."
                continue

        # Check for discrepancies in the last 5 days AND around the last reconciled date
        if not resolved:
            recent_and_surrounding_transactions = cursor.execute('''
                SELECT id, transaction_date, amount FROM transactions 
                WHERE account = ? AND (transaction_date >= ? OR 
                                    transaction_date BETWEEN ? AND ?) AND reconciled = 0
            ''', (account, 
                (datetime.strptime(current_balance_date, '%Y-%m-%d') - timedelta(days=5)).strftime('%Y-%m-%d'),
                (datetime.strptime(last_reconciled_date, '%Y-%m-%d') - timedelta(days=3)).strftime('%Y-%m-%d'),
                (datetime.strptime(last_reconciled_date, '%Y-%m-%d') + timedelta(days=3)).strftime('%Y-%m-%d'))).fetchall()

        # Estimate processing time for last 5 days transactions
        num_recent_and_surrounding_transactions = len(recent_and_surrounding_transactions)
        est_recent_and_surrounding_transactions = estimate_processing_time(num_recent_and_surrounding_transactions)

        # Decide if we are going to process these
        proceed = False
        if est_recent_and_surrounding_transactions[0] < reasonable_time_threshold and (est_recent_and_surrounding_transactions[0] > 0 or num_recent_and_surrounding_transactions < 10):  
            proceed = True
        else: 
            print(f"Too many transactions ({num_recent_and_surrounding_transactions}) in the last 5 days and around the time of the last reconcile date to identify likely pending transactions that should be excluded from the reconcile balance efficiently. Estimated processing time: {est_recent_and_surrounding_transactions[0]:.2f} seconds.")
            user_input = input("Do you want to proceed with looking for matches within these transactions? (yes/no): ").lower()
            if user_input in ['yes', 'y']:
                proceed = True
                print(f"I will start looking!")
            else:
                print(f"Skipping search for pending transactions in the last 5 days.")
        
        # Send these transactions for processing
        if proceed == True:
            potential_matches = find_matching_transactions(recent_and_surrounding_transactions, discrepancy)
            if potential_matches:
                print("Adding in a check for discrpancies around the initial date found potential matches.")
                resolved = process_potential_matches(cursor, account, potential_matches, current_balance_date, current_balance)
                if resolved:
                    conn.commit()
                    reconciliation_summary[account]["result"] = "Reconciled with exclusions/deletions after simple searching."
                    continue

        ## EXTENSIVE SEARCHING ##
        if not resolved:
            
            # Maybe check for discrepancies across all unreconciled transactions
            num_transactions = len(all_unreconciled_transactions)
            est_all = estimate_processing_time(num_transactions)

            # Determine if it's reasonable to just look through all missing transactions
            if est_all[0] < 10 and (est_all[0] > 0 or num_transactions < 10):
                potential_matches = find_matching_transactions(all_unreconciled_transactions_filtered, discrepancy)
                if potential_matches:
                    print("I went ahead and searched ALL combinations of unreconciled transactions and found a potential match.")
                    resolved = process_potential_matches(cursor, account, potential_matches, current_balance_date, current_balance)
                    if resolved:
                        conn.commit()
                        reconciliation_summary[account]["result"] = "Reconciled with exclusions/deletions after searching all transactions."
                        continue                        

            # Otherwise ask user what they want to do
            else:
                user_input = input(
                    f"No easy combination of transactions have been identified yet which, if excluded, would result in a successful reconcile for account {account}.\n"
                    f"If you would like, I can search for additional combinations of all transactions. There are {num_transactions} unreconciled transactions.\n"
                    f"Options:\n"
                    f" - (l, limit) Search limited sets of combinations.\n"
                    f" - (a)  Search ALL {est_all[1]} combinations: {est_all[0] / 60:.2f} minutes.\n"
                    f" - (s)  skip searching.\n\n"
                    f"Your selection? : ").lower()
                
                # Go for broke! Start with searching for all matches!
                if user_input == 'a':
                    potential_matches = find_matching_transactions(all_unreconciled_transactions_filtered, discrepancy)
                    if potential_matches:
                        resolved = process_potential_matches(cursor, account, potential_matches, current_balance_date, current_balance)                                   
                        if resolved:
                            conn.commit()
                            reconciliation_summary[account]["result"] = "Reconciled with exclusions/deletions after extensively searching all transactions."
                            continue

                elif user_input in ['l', 'limit']:
                    # We're going to take an iterative approach here
                    last_limit = 1
                    new_limit = 1
                    while not resolved:
                        if last_limit >= num_transactions:
                            print(f"We've already gone through all possible transaction combos. Sorry!")
                            break

                        # Establish next suggested limits
                        inc_small = last_limit + 1
                        inc_medium = min(last_limit + 2, num_transactions)
                        inc_high = min(max(last_limit + ceil((num_transactions - last_limit) / 20), last_limit + 4), num_transactions)
                        
                        # Estimate them
                        est_small  = estimate_processing_time(num_transactions, r_min=last_limit, r_max=inc_small)
                        est_medium = estimate_processing_time(num_transactions, r_min=last_limit, r_max=inc_medium)
                        est_high   = estimate_processing_time(num_transactions, r_min=last_limit, r_max=inc_high)
                        est_all    = estimate_processing_time(num_transactions, r_min=last_limit)

                        # Prompt user
                        user_input = input(
                            f"Where would you like to set the limit of number of transactions in a combo?\n"
                            f" - ({inc_small}) Search {est_small[1]} combinations of up to {inc_small} transactions: {est_small[0] / 60:.2f} minutes.\n"
                            f" - ({inc_medium}) Search {est_medium[1]} combinations of up to {inc_medium} transactions: {est_medium[0] / 60:.2f} minutes.\n"
                            f" - ({inc_high}) Search {est_high[1]} combinations of up to {inc_high} transactions: {est_high[0] / 60:.2f} minutes.\n"
                            f" - (a) Search ALL {est_all[1]} combinations of all transactions: {est_all[0] / 60:.2f} minutes.\n"
                            f" - (s) If you've had enough, skip further searching.\n\n"
                            f"Your selection? : ").lower()
                        
                        # Read input and act
                        if user_input in ['a', 'all']:
                            new_limit = num_transactions
                        elif user_input.isdigit():
                            user_input = int(user_input)

                            # Make sure the number is higher than what we've already searched. 
                            if user_input <= last_limit:
                                print(f"We've searched for transactions up to {last_limit} already. Pick a higher number, or enter 's' to skip.")
                                continue
                            new_limit = min(user_input, num_transactions)
                            
                            # Search
                            potential_matches = find_matching_transactions(all_unreconciled_transactions_filtered, discrepancy, r_min=last_limit, r_max=new_limit)
                            if potential_matches:
                                resolved = process_potential_matches(cursor, account, potential_matches, current_balance_date, current_balance)
                                if resolved:
                                    conn.commit()
                                    reconciliation_summary[account]["result"] = "Reconciled with exclusions/deletions after searching limited combinations of transactions."
                                    continue
                                else:
                                    print(f"Unfortunately, potential matches were rejected. Shall we keep trying?")
                            else: 
                                print(f"No matches found in this sample. Shall we keep trying?")

                        else:
                            break

                        # Finalize this while loop iteration    
                        last_limit = new_limit

                    else:
                        reconciliation_summary[account]["result"] = "Reconciled with exclusions/deletions after searching limited combinations of transactions."
                        continue

                    reconciliation_summary[account]["result"] = "NOT reconciled after iteratively searching combinations of transactions."
                    export_reconciliation_details(account, last_reconciled_balance, last_reconciled_date, all_unreconciled_df, current_balance, discrepancy)

                else:
                    print(f"Skipping reconciliation for account {account}.\n\n")
                    reconciliation_summary[account]["result"] = "NOT reconciled."
                    export_reconciliation_details(account, last_reconciled_balance, last_reconciled_date, all_unreconciled_df, current_balance, discrepancy)
                    continue
        
        # Just a backstop
        if not resolved:
            print(f"Tried everything, but could not reconcile account {account}.\n\n")
            reconciliation_summary[account]["result"] = "NOT reconciled."            
            export_reconciliation_details(account, last_reconciled_balance, last_reconciled_date, all_unreconciled_df, current_balance, discrepancy)

    # Display the reconciliation summary
    print("\nReconciliation Summary:")
    print(f"{'Account':<20} {'Initial Balance':<15} {'Online Balance':<15} {'Discrepancy':<15} {'Result':<40}")
    print("="*100)
    for account, details in reconciliation_summary.items():
        print(f"{account[:19]:<20} {str(details['initial_balance'])[:14]:<15} {str(details['online_balance'])[:14]:<15} {str(details['transaction_discrepancy'])[:14]:<15} {details['result']:<40}")

    # END :-)
    conn.close()

def estimate_processing_time(num_transactions, r_min=1, r_max=None):
    global time_per_combination  # Ensure to use the global variable

    if r_max is None:
        r_max = num_transactions  # Set r_max to the number of transactions if not specified

    # Ensure we have time_per_combination calculated already.
    if time_per_combination is None:
        print("Calculating average time per combination...")
        time_per_combination = calculate_time_per_combination()

    combinations_count = sum(comb(num_transactions, r) for r in range(r_min, r_max + 1))
    estimated_time = combinations_count * time_per_combination

    # print(f"Estimated processing time: {estimated_time / 60:.2f} minutes for {combinations_count} combinations.")
    return estimated_time, combinations_count

def calculate_time_per_combination():
    sample_size = 50

    # Generate sample transactions with random amounts
    transactions = [(i, '2024-05-11', random.uniform(-100, 100)) for i in range(sample_size)]
    discrepancy = Decimal('98.02')  # Set a fixed discrepancy for the test
    r_min, r_max = 1, 3  # Define the range for combinations

    # Calculate number of combinations to process
    combinations_count = sum(comb(sample_size, r) for r in range(r_min, r_max + 1))

    start_time = timer()
    # Process the combinations using the serial approach
    matching_combinations = find_matching_transactions_serial(transactions, discrepancy, r_min, r_max)
    end_time = timer()

    # Calculate the average time per combination
    processing_time = end_time - start_time
    if combinations_count > 0:
        average_time_per_combination = processing_time / combinations_count
    else:
        average_time_per_combination = 0  # Avoid division by zero if no combinations

    print(f"Calculated average time per combination: {average_time_per_combination:.6f} seconds.")
    return average_time_per_combination

def find_matching_transactions(transactions, discrepancy, r_min=1, r_max=None):
    # Skip if there aren't actaully any transactions
    if len(transactions) < 1:
        return None
    
    # If r_max is not specified, use the maximum possible
    if r_max is None:
        r_max = len(transactions)

    # Determine if we should use serial or parallel processing
    num_combinations = sum(comb(len(transactions), r) for r in range(r_min, r_max + 1))
    non_parallel_threshold = 10000  # Threshold to decide between serial and parallel processing
    if num_combinations < non_parallel_threshold:
        return find_matching_transactions_serial(transactions, discrepancy, r_min, r_max)
    else:
        return find_matching_transactions_parallel(transactions, discrepancy, r_min, r_max)

def find_matching_transactions_serial(transactions, discrepancy, r_min, r_max):
    matching_combinations = []
    for r in range(r_min, r_max + 1):
        for combo in combinations(transactions, r):
            combo_sum = Decimal(sum(Decimal(trans[2]) for trans in combo)).quantize(cent)
            if combo_sum == -discrepancy:
                matching_combinations.append(combo)
    return matching_combinations

def find_matching_transactions_parallel(transactions, discrepancy, r_min, r_max):
    pool = multiprocessing.Pool()
    results = [pool.apply_async(process_combinations, args=(transactions, discrepancy, r)) for r in range(r_min, r_max + 1)]

    # Wait for all results to complete and collect them
    valid_combinations = [result.get() for result in results if result.get()]
    pool.close()
    pool.join()

    # Flatten the list of combinations received from each process
    valid_combinations = [item for sublist in valid_combinations for item in sublist]
    return valid_combinations

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

def process_potential_matches(cursor, account, potential_matches, current_balance_date, current_balance, display_only=False):
    """
    Processes potential matching transactions that may resolve a discrepancy.
    
    Args:
        cursor (sqlite3.Cursor): Database cursor for executing SQL commands.
        account (str): The account for which transactions are being reconciled.
        potential_matches (list of tuples): List of transaction combinations that match the discrepancy.
        current_balance_date (str): The date of the current balance check.
        current_balance (Decimal): Balance as of current_balance_date we are reconciling against.
        display_only (bool): If True, display the matches but do not ask for confirmation and proceed to reconcile with exclusion.
    
    Returns:
        bool: True if the matches were processed and the account reconciled, False otherwise.
    """
    print(f"\nPotential transactions matching the discrepancy for account {account}:")
    
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

    if display_only:
        selected_combo = potential_matches[0]
        selected_ids = [trans[0] for trans in selected_combo]

        exclude_ids = selected_ids
        cursor.execute('UPDATE transactions SET reconciled = 1, reconcile_date = ? WHERE account = ? AND reconciled = 0 AND id NOT IN ({seq})'.format(seq=','.join(['?']*len(exclude_ids))), 
                       (current_balance_date, account, *exclude_ids))
        cursor.execute('UPDATE account_balances SET last_reconciled_balance = ?, last_reconciled_date = ? WHERE account = ?', 
                       (Decimal(current_balance).quantize(cent), current_balance_date, account))
        print(f"Transactions automatically reconciled by excluding these likely pending transactions. Account: {account}.")
        return True
    else:
        # Get user input to select a combination or skip
        selected_combo_index = input("Select the combination number to reconcile, or s to skip/reject: ")
        try:
            selected_combo_index = int(selected_combo_index)
        except:
            selected_combo_index = -1    

        if selected_combo_index == -1:
            print("No transactions reconciled for this account.")
            return False

        elif 1 <= selected_combo_index <= len(potential_matches):  # Valid numerical values
            selected_combo = potential_matches[selected_combo_index - 1]
            selected_ids = [trans[0] for trans in selected_combo]

            exclude_ids = []
            delete_ids = []
            reconcile_ids = []
            for trans_id in selected_ids:
                action = input(f"Transaction ID {trans_id}: (e)xclude from reconciliation, (d)elete from the database, or mark previously (r)econciled? (e/d/r): ").lower()
                if action in ['d', 'del', 'delete']:
                    delete_ids.append(trans_id)
                elif action in ['r', 'rec', 'reconcile']:
                    reconcile_ids.append(trans_id)
                else:
                    exclude_ids.append(trans_id)

            # Delete transactions that were marked for deletion
            if delete_ids:
                cursor.execute('DELETE FROM transactions WHERE id IN ({seq})'.format(seq=','.join(['?']*len(delete_ids))), delete_ids)
                print(f"Transaction ids {delete_ids} deleted. Be sure to delete them in Monarch as well, or they will just come back again.")

            # Reconcile transactions that were marked for reconciliation
            if reconcile_ids:
                cursor.execute('UPDATE transactions SET reconciled = 1, reconcile_date = ? WHERE id IN ({seq})'.format(seq=','.join(['?']*len(reconcile_ids))), 
                               (current_balance_date, *reconcile_ids))
                print(f"Transaction ids {reconcile_ids} reconciled.")

            # Proceed with reconciliation, excluding selected transactions
            if exclude_ids:
                cursor.execute('UPDATE transactions SET reconciled = 1, reconcile_date = ? WHERE account = ? AND reconciled = 0 AND id NOT IN ({seq})'.format(seq=','.join(['?']*len(exclude_ids))), 
                               (current_balance_date, account, *exclude_ids))

            cursor.execute('UPDATE account_balances SET last_reconciled_balance = ?, last_reconciled_date = ? WHERE account = ?', 
                           (Decimal(current_balance).quantize(cent), current_balance_date, account))

            print(f"Transactions reconciled for account {account}.")
            return True
        else:
            print(f"Invalid selection. No transactions were reconciled for account {account}.")
            return False

def export_reconciliation_details(account, initial_balance, initial_date, transactions, online_balance, discrepancy):
    """
    Export or display details of non-reconciled accounts.
    
    Args:
        account (str): The account name.
        initial_balance (Decimal): The last reconciled balance.
        initial_date (str): The date of the last reconciled balance.
        transactions (pd.DataFrame): DataFrame of non-reconciled transactions.
        online_balance (Decimal): The current online balance.
        discrepancy (Decimal): The discrepancy amount.
    """
    # This is a display function - all monetary values need to have decimals quantized.
    initial_balance        = Decimal(initial_balance).quantize(cent)
    online_balance         = Decimal(online_balance).quantize(cent)
    discrepancy            = Decimal(discrepancy).quantize(cent)
    transactions['amount'] = [Decimal(amt).quantize(cent) for amt in transactions['amount']]
    
    # Display or export options
    option = input(f"Would you like to display or export details for account {account}? (display/export/skip): ").lower()
    if option not in ['d', 'display', 'e', 'export']:
        print("Invalid option selected. Skipping details export.")
        return

    # Calculate the running balance
    transactions['running_balance'] = transactions['amount'].cumsum() + initial_balance

    if option in ['d', 'display']:
        details = f"Account: {account}\n"
        details += f"Initial Balance (as of {initial_date}): {initial_balance}\n"
        details += f"Online Balance: {online_balance}\n"
        details += f"Discrepancy: {discrepancy}\n"
        details += "\nNon-reconciled Transactions:\n"
        details += f"{'ID':<5} {'Date':<15} {'Merchant':<25} {'Amount':<10} {'Running Balance':<15}\n"
        details += "-" * 65 + "\n"

        for idx, row in transactions.iterrows():
            details += f"{row['id']:<5} {row['transaction_date']:<15} {row['merchant']:<25} {row['amount']:<10} {row['running_balance']:<15}\n"
        
        print(details)
    elif option in ['e', 'export']:
        export_details = transactions.copy()
        export_details['account'] = account
        export_details['initial_balance'] = initial_balance
        export_details['initial_date'] = initial_date
        export_details['online_balance'] = online_balance
        export_details['discrepancy'] = discrepancy

        # Define the order of columns for the CSV
        columns_order = [
            'id', 'transaction_date', 'merchant', 'amount', 'running_balance',
            'account', 'initial_balance', 'initial_date', 'online_balance', 'discrepancy'
        ]

        # Export to CSV
        export_path = os.path.join(os.path.dirname(db_path), f"reconciliation_details_{account}_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv")
        export_details.to_csv(export_path, columns=columns_order, index=False)
        print(f"Details exported to {export_path}")


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
