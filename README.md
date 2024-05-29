# MonarchMoneyReconcile

## Transactions Table Schema

Below is a detailed description of each column in the `transactions` table:

- **id**
  - **Type**: INTEGER
  - **Description**: A unique identifier for each transaction entry. It is set as the primary key and automatically increments for each new transaction.
  - **Data Source**: Automatically generated by the database when a new transaction is added.

- **transaction_date**
  - **Type**: TEXT
  - **Description**: The date the transaction occurred, stored as a string in the format "YYYY-MM-DD". This is directly imported from your CSV data.
  - **Data Source**: Imported from the Monarch Money transaction CSV export, Date heading.

- **merchant**
  - **Type**: TEXT
  - **Description**: The name of the merchant where the transaction took place.
  - **Data Source**: Imported from the Monarch Money transaction CSV export, Merchant heading.

- **category**
  - **Type**: TEXT
  - **Description**: The category of the transaction, such as "Groceries," "Utilities," "Dining," etc.
  - **Data Source**: Imported from the Monarch Money transaction CSV export, Category heading.

- **account**
  - **Type**: TEXT
  - **Description**: The account used for the transaction, often denoted by the last few digits of the account number or the account type.
  - **Data Source**: Imported from the Monarch Money transaction CSV export, Account heading..

- **original_statement**
  - **Type**: TEXT
  - **Description**: The original description of the transaction as it appears on the statement.
  - **Data Source**: Imported from the Monarch Money transaction CSV export, Original Statement heading.

- **amount**
  - **Type**: REAL
  - **Description**: The monetary amount of the transaction. This is a floating-point number that can represent both debits and credits.
  - **Data Source**: Imported from the Monarch Money transaction CSV export, Amount heading.

- **reconciled**
  - **Type**: BOOLEAN
  - **Description**: A boolean value that indicates whether the transaction has been reconciled with the account balances. It defaults to 0 (false) and is set to 1 (true) once reconciliation is confirmed.
  - **Data Source**: Updated by the script during the reconciliation process.

- **import_date**
  - **Type**: TEXT
  - **Description**: The date and time when the transaction was imported into the database, stored as a string in the format "YYYY-MM-DD HH:MM:SS".
  - **Data Source**: Automatically set by the script at the time of importing transactions from the CSV.

- **reconcile_date**
  - **Type**: TEXT
  - **Description**: The date and time when the transaction was marked as reconciled.
  - **Data Source**: Automatically set by the script when the transaction is marked as reconciled during the reconciliation process.
