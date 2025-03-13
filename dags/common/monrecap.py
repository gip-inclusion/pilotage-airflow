import pandas as pd


def convert_date_columns(df, date_columns):
    for col in date_columns:
        # Sometimes the monrecap team might add a column starting with "date" that does not contain a date
        # here we handle this case
        try:
            df[col] = pd.to_datetime(df[col])
            print(f"Successfully converted {col}")
        except Exception as e:
            print(f"Error converting {col}: {e}")

    return df
