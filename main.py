import argparse
import psycopg2
import os

from ETL.load.write import execute_query


def parse_args():
    """
    Parses the app arguments

    Returns:
        the args namespace
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("-host", "--host", type=str, default=None, nargs='?', help="Hostname")
    parser.add_argument("-p", "--port", type=str, default=None, nargs='?', help="Portnumber")
    parser.add_argument("-db", "--database", type=str, default=None, nargs='?', help="Databasename")
    parser.add_argument("-u", "--user", type=str, default=None, nargs='?', help="Username")
    #parser.add_argument("-pw", "--password", type=str, default=None, nargs='?', help="Password")

    return parser.parse_args()


def run(host: str, port: str, database: str, user: str): #password: str):
    try:
        # Connect to the PostgreSQL database
        connection = psycopg2.connect(
            host=host,
            port=port,
            dbname=database,
            user=user,
            password=os.getenv('DB_POSTGRES_PASSWORD')
        )
        print("Connection to the database established successfully.")
        return connection
    except psycopg2.Error as e:
        print(f"Error: Could not connect to the database. Check your parameters\n{e}")
        return None


def main():
    args = parse_args()
    connection = run(args.host, args.port, args.database, args.user)

    if connection:
        # Define your query to create a table
        create_table_query = """
            CREATE TABLE IF NOT EXISTS Test_basketball_stats (
                id SERIAL PRIMARY KEY,
                player_name VARCHAR(100),
                team_name VARCHAR(100),
                points INT,
                assists INT,
                rebounds INT,
                steals INT,
                blocks INT
            );
            """

        # Execute the query to create the table
        execute_query(connection, create_table_query)

        # Close the connection
        connection.close()


if __name__ == "__main__":
    main()
