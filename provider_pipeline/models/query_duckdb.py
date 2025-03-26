import duckdb
import pandas as pd

def query_duckdb():
    """Queries the provider_address_agg table and returns all rows as a Pandas DataFrame."""
    # Connect to DuckDB database
    conn = duckdb.connect(database="/tmp/dev.duckdb", read_only=True)

    # SQL query to fetch all records from provider_address_agg
    query = "SELECT * FROM provider_address_agg;"

    # Execute query and load results into Pandas DataFrame
    df = conn.execute(query).fetchdf()

    # Print DataFrame in columnar format
    print(df.to_string(index=False))

    # Return DataFrame for further use
    return df

# Run the function when executed as a script
if __name__ == "__main__":
    query_duckdb()
