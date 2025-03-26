import pandas as pd

def model(dbt, session):
    """Queries the provider_address_agg table and expands JSON into separate columns."""

    # Query provider_address_agg
    df = session.sql("SELECT provider_id, addresses FROM {{ ref('provider_address_agg') }}").df()


    # Normalize JSON data to extract address field
    addresses_expanded = []
    for index, row in df.iterrows():
        if row["addresses"]:  # If addresses exist
            for addr in row["addresses"]:  # Iterate through the array
                addresses_expanded.append([row["provider_id"], addr["address_id"], addr["street"], addr["rank"]])
        else:  # If no address, keep an empty row
            addresses_expanded.append([row["provider_id"], None, None, None])

    # Create a new DataFrame with proper column names
    df_expanded = pd.DataFrame(addresses_expanded, columns=["provider_id", "address_id", "street", "rank"])

    # Print output in a clean columnar format
    print(df_expanded.to_string(index=False))

    return df_expanded
