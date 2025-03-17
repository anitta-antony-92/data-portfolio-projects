'''

    Assert & row(0)[0] Example

       Input Data:
    Polars DataFrame (df):

    import polars as pl
    df = pl.DataFrame({
        "age": [25, 30, None, 40],
        "income": [50000, None, 70000, 80000],
        "height": [None, 175, 180, 165]
    })
    Numerical Columns:
    numerical_cols = ["age", "income", "height"]
    Execution:
    null_counts = df[numerical_cols].null_count():

    Output:
    shape: (1, 3)
    ┌─────┬────────┬────────┐
    │ age │ income │ height │
    ├─────┼────────┼────────┤
    │ 1   │ 1      │ 1      │
    └─────┴────────┴────────┘
    This indicates there is 1 null in each column.

    total_nulls = null_counts.sum().row(0)[0]:

    Output:
    3
    This is the total number of nulls across all columns.

    assert total_nulls == 0, ...:

    Since total_nulls = 3, the assertion fails, and an error is raised:

    AssertionError: Missing values still exist in numerical columns: shape: (1, 3)
    ┌─────┬────────┬────────┐
    │ age │ income │ height │
    ├─────┼────────┼────────┤
    │ 1   │ 1      │ 1      │
    └─────┴────────┴────────┘

'''

'''
    TargetEncoding Example
    Here’s how this code might be used in a Polars DataFrame transformation:
    

    import polars as pl
    import logging
    
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Example DataFrame
    df = pl.DataFrame({
        "AREA NAME": ["North", "South", "North", "East", "South", "East"],
        "Crm Cd": [1, 2, 3, 4, 5, 6]
    })
    
    # Log the start of categorical encoding
    logging.info("Encoding categorical variables using Target Encoding...")
    
    try:
        # Calculate the mean of 'Crm Cd' for each unique value in 'AREA NAME'
        area_name_encoded = df.group_by("AREA NAME").agg(
            pl.col("Crm Cd").mean().alias("area_name_encoded")
        )
    
        # Join the encoded values back to the original DataFrame
        df = df.join(area_name_encoded, on="AREA NAME", how="left")
    
        logging.info("Categorical variables successfully encoded.")
        print(df)
    
    except Exception as e:
        logging.error(f"Error during categorical encoding: {e}")
        
        
    Output:

    INFO:root:Encoding categorical variables using Target Encoding...
    INFO:root:Categorical variables successfully encoded.
    shape: (6, 3)
    ┌───────────┬────────┬───────────────────┐
    │ AREA NAME │ Crm Cd │ area_name_encoded │
    ├───────────┼────────┼───────────────────┤
    │ North     │ 1      │ 2.0               │
    │ South     │ 2      │ 3.5               │
    │ North     │ 3      │ 2.0               │
    │ East      │ 4      │ 5.0               │
    │ South     │ 5      │ 3.5               │
    │ East      │ 6      │ 5.0               │
    └───────────┴────────┴───────────────────┘
    Explanation of Output:
    The "AREA NAME" column contains categorical values: "North", "South", and "East".
    The "Crm Cd" column contains numerical values (the target variable).
    The group_by and agg operations calculate the mean of "Crm Cd" for each unique "AREA NAME":
    "North": Mean of [1, 3] is 2.0.
    "South": Mean of [2, 5] is 3.5.
    "East": Mean of [4, 6] is 5.0.
    The resulting "area_name_encoded" column contains the target-encoded values.
'''