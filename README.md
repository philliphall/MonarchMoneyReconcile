# Monarch Money Reconciliation Script

## Overview
This script automates the reconciliation of transactions from Monarch Money with the balances in each account. It performs the following tasks:
- Initializes a database to store transactions and balances.
- Imports transactions and balance data from CSV files.
- Sets initial balances for each account.
- Reconciles transactions against the balances and identifies discrepancies.

## Features
- **Backup Database**: Creates a backup of the database before making any changes.
- **Import Transactions**: Imports transaction data from a CSV file and handles discrepancies.
- **Set Initial Balances**: Sets the initial balances for each account using balance data or user input.
- **Reconcile Accounts**: Reconciles the transactions against the balances, identifies discrepancies, and prompts the user for action if needed.
- **Parallel Processing**: Uses parallel processing to efficiently find matching transactions.

## Requirements
- Python 3.7+
- Required Python libraries: `pandas`, `sqlite3`, `multiprocessing`, `decimal`, `shutil`, `glob`, `datetime`, `random`, `timeit`

## Configuration
Before running the script, ensure you have updated the configuration variables within the script body.

### Configuration Variables
Update the following variables in the script body section near the top conveniently labeled "### Configuration ###":

1. `db_path`: Path to the SQLite database file.
    ```python
    db_path = "C:\\Users\\Me\\Documents\\reconciliation.db" 
    ```
2. `max_backups`: Maximum number of database backups to keep.
    ```python
    max_backups = 20
    ```
3. `import_folder`: Folder where the CSV files are stored.
    ```python
    import_folder = "C:\\Users\\Me\\Downloads\\" 
    ```
4. `earliest_reconcile_date`: The earliest date from which to start the reconciliation process. Format yyyy-mm-dd
    ```python
    earliest_reconcile_date = "2023-01-01"
    ```
5. `combine_sofi_vaults`: Special case for SoFi bank accounts.
    ```python
    combine_sofi_vaults = True
    ```

## Instructions

### Step 1: Set Up
1. Clone or download the repository to your local machine.
2. Install the required Python libraries, if not already installed:
    ```
    pip install pandas
    ```

### Step 2: Prepare Your Data
1. Go to [Monarch Money](https://app.monarchmoney.com/accounts) and click "Download CSV" under the Summary widget. Save it as `balances.csv` in the folder specified in `import_folder`.
2. Go to [Monarch Money Settings](https://app.monarchmoney.com/settings/data) and click "Download transactions" and save it in the same folder.

### Step 3: Run the Script
1. Open a terminal or command prompt.
2. Navigate to the directory where the script is located.
3. Run the script:
    ```
    python MMReconcile.py
    ```
4. Follow the prompts to complete the reconciliation process.

## Key Functions

### `initialize_db`
Creates the necessary tables in the SQLite database.

### `import_transactions`
Imports transaction data from a CSV file, filtering out transactions before a specified earliest reconciliation date. It also checks for existing non-reconciled transactions before this date and prompts the user for action.

### `set_initial_balances`
Prompts the user to set the initial balances for each account, using the balance data from the CSV file or prompting the user if the data is not available.

### `load_daily_balances`
Loads the daily balances from a CSV file.

### `reconcile_accounts`
Reconciles the transactions against the balances and identifies any discrepancies. It uses parallel processing to find matching transactions efficiently.

