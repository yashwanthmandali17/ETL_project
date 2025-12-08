import os
import pandas as pd
from extract_titanic import extract_data
# Function to transform Titanic data
def transform_data(raw_path):
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    staged_dir = os.path.join(base_dir, "data", "staged")
    os.makedirs(staged_dir, exist_ok=True)
    df = pd.read_csv(raw_path)
    # Numeric columns in Titanic dataset
    numeric_cols = ["age", "fare"]
    # Fill missing values with median for numeric columns
    for col in numeric_cols:
        df[col] = df[col].fillna(df[col].median())
    # Fill missing categorical columns with mode
    categorical_cols = ["embarked", "embark_town", "deck", "sex", "who", "class"]
    for col in categorical_cols:
        if col in df.columns:
            df[col] = df[col].fillna(df[col].mode()[0])
    #  Feature Engineering
    df["family_size"] = df["sibsp"] + df["parch"] + 1
    df["is_child"] = (df["age"] < 18).astype(int)
    df["fare_per_person"] = df["fare"] / df["family_size"]
    df.drop(columns=[], inplace=True, errors="ignore")
    staged_path = os.path.join(staged_dir, "titanic_transformed.csv")
    df.to_csv(staged_path, index=False)
    print(f"Data transformed and saved at: {staged_path}")
    return staged_path
if __name__ == "__main__":
    raw_path = extract_data()
    transform_data(raw_path)