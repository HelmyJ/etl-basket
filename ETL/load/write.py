import psycopg2

def execute_query(connection, query):
    try:
        # Create a cursor object
        cursor = connection.cursor()

        # Execute the query
        cursor.execute(query)

        # Commit the transaction if necessary
        connection.commit()

        # Close the cursor
        cursor.close()
        print("Query executed successfully.")
    except psycopg2.Error as e:
        print(f"Error: Could not execute query.\n{e}")