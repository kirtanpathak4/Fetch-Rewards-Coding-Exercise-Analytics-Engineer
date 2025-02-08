import sqlite3

# Path to the SQLite database file
db_path = 'mydb.db'

# Connect to the database
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# 1. Check for null values in primary keys
def check_null_ids():
    tables = ['users', 'receipts', 'brands']
    for table in tables:
        cursor.execute(f"""
            SELECT 
                COUNT(*) AS total_rows,
                SUM(CASE WHEN _id IS NULL THEN 1 ELSE 0 END) AS null_ids
            FROM {table};
        """)
        total_rows, null_ids = cursor.fetchone()
        print(f"Table: {table} | Total Rows: {total_rows} | Null _id Values: {null_ids}")

# 2. Check for invalid userId references in receipts
def check_invalid_user_ids():
    cursor.execute("""
        SELECT COUNT(*) AS invalid_user_ids
        FROM receipts
        WHERE userId IS NOT NULL AND userId NOT IN (SELECT _id FROM users);
    """)
    invalid_user_ids = cursor.fetchone()[0]
    print(f"Invalid userId references in receipts: {invalid_user_ids}")

# 3. Check for non-numeric values in pointsEarned and totalSpent
def check_non_numeric_values():
    cursor.execute("""
        SELECT COUNT(*) 
        FROM receipts
        WHERE pointsEarned IS NOT NULL AND pointsEarned GLOB '*[^0-9]*';
    """)
    invalid_points_earned = cursor.fetchone()[0]

    cursor.execute("""
        SELECT COUNT(*) 
        FROM receipts
        WHERE totalSpent IS NOT NULL AND totalSpent GLOB '*[^0-9.]*';
    """)
    invalid_total_spent = cursor.fetchone()[0]

    print(f"Non-numeric values in pointsEarned: {invalid_points_earned}")
    print(f"Non-numeric values in totalSpent: {invalid_total_spent}")

# 4. Check for invalid date ranges (before 2000 or after 2030)
def check_invalid_dates():
    cursor.execute("""
        SELECT COUNT(*) 
        FROM receipts
        WHERE (purchaseDate < 946684800 OR purchaseDate > 1893456000)
        AND purchaseDate IS NOT NULL;
    """)
    invalid_dates = cursor.fetchone()[0]
    print(f"Invalid purchaseDate values: {invalid_dates}")

# 5. Check for boolean inconsistencies
def check_boolean_inconsistencies():
    cursor.execute("""
        SELECT DISTINCT active 
        FROM users
        WHERE active NOT IN (0, 1) AND active IS NOT NULL;
    """)
    invalid_active_values = cursor.fetchall()

    cursor.execute("""
        SELECT DISTINCT topBrand 
        FROM brands
        WHERE topBrand NOT IN (0, 1) AND topBrand IS NOT NULL;
    """)
    invalid_topBrand_values = cursor.fetchall()

    print(f"Invalid values in users.active: {invalid_active_values}")
    print(f"Invalid values in brands.topBrand: {invalid_topBrand_values}")

# 6. Check for duplicate _id values
def check_duplicate_ids():
    tables = ['users', 'receipts', 'brands']
    for table in tables:
        cursor.execute(f"""
            SELECT _id, COUNT(*) AS count
            FROM {table}
            GROUP BY _id
            HAVING count > 1;
        """)
        duplicates = cursor.fetchall()
        print(f"Duplicate _id values in {table}: {len(duplicates)}")

# 7. Check for empty strings in key text fields
def check_empty_strings():
    fields_to_check = {
        'users': ['role', 'signUpSource', 'state'],
        'receipts': ['bonusPointsEarnedReason', 'rewardsReceiptStatus'],
        'brands': ['name', 'category']
    }
    for table, fields in fields_to_check.items():
        for field in fields:
            cursor.execute(f"""
                SELECT COUNT(*) 
                FROM {table}
                WHERE {field} = '';
            """)
            empty_count = cursor.fetchone()[0]
            print(f"Empty strings in {table}.{field}: {empty_count}")

# 8. Identify outliers in numeric fields
def check_outliers():
    numeric_fields = {
        'receipts': ['bonusPointsEarned', 'purchasedItemCount', 'totalSpent']
    }
    for table, fields in numeric_fields.items():
        for field in fields:
            cursor.execute(f"""
                SELECT MAX(CAST({field} AS FLOAT)), MIN(CAST({field} AS FLOAT))
                FROM {table}
                WHERE {field} IS NOT NULL AND {field} != '';
            """)
            max_val, min_val = cursor.fetchone()
            print(f"Outliers in {table}.{field} - Max: {max_val}, Min: {min_val}")

# 9. Check for inconsistent casing
def check_inconsistent_casing():
    categorical_fields = {
        'users': ['role', 'state'],
        'brands': ['category']
    }
    for table, fields in categorical_fields.items():
        for field in fields:
            cursor.execute(f"""
                SELECT DISTINCT {field}
                FROM {table}
                WHERE {field} IS NOT NULL
                ORDER BY {field} COLLATE NOCASE;
            """)
            distinct_values = [row[0] for row in cursor.fetchall()]
            print(f"Inconsistent casing in {table}.{field}: {distinct_values}")

# 10. Check for unusual characters in text fields
def check_unusual_characters():
    text_fields = {
        'users': ['role', 'signUpSource', 'state'],
        'receipts': ['bonusPointsEarnedReason', 'rewardsReceiptStatus'],
        'brands': ['name', 'category']
    }
    for table, fields in text_fields.items():
        for field in fields:
            cursor.execute(f"""
                SELECT COUNT(*)
                FROM {table}
                WHERE {field} GLOB '*[!@#$%^&*()_+=<>?/\\|{{}}~`]*';
            """)
            unusual_count = cursor.fetchone()[0]
            print(f"Unusual characters in {table}.{field}: {unusual_count}")

# Run all checks
if __name__ == "__main__":
    print("Checking for null values in primary keys...")
    check_null_ids()
    
    print("\nChecking for invalid userId references...")
    check_invalid_user_ids()

    print("\nChecking for non-numeric values in pointsEarned and totalSpent...")
    check_non_numeric_values()

    print("\nChecking for invalid date ranges...")
    check_invalid_dates()

    print("\nChecking for boolean inconsistencies...")
    check_boolean_inconsistencies()

    print("\nChecking for duplicate _id values...")
    check_duplicate_ids()

    print("\nChecking for empty strings in key text fields...")
    check_empty_strings()

    print("\nChecking for outliers in numeric fields...")
    check_outliers()

    print("\nChecking for inconsistent casing in categorical fields...")
    check_inconsistent_casing()

    print("\nChecking for unusual characters in text fields...")
    check_unusual_characters()

# Close the database connection
conn.close()
