import sqlite3
import pandas as pd
import matplotlib.pyplot as plt

conn = sqlite3.connect("policy.db")
cursor = conn.cursor()
cursor.execute("DROP TABLE IF EXISTS policy_types")
sql = "CREATE TABLE policy_types (country TEXT PRIMARY KEY, policy_type TEXT NOT NULL)"
cursor.execute(sql)
data = [
    # ETS
    ("Austria", "ETS"),
    ("Belgium", "ETS"),
    ("Bulgaria", "ETS"),
    ("Czech Republic", "ETS"),
    ("Germany", "ETS"),
    ("Greece", "ETS"),
    ("Italy", "ETS"),
    ("Kazakhstan", "ETS"),
    ("South Korea", "ETS"),
    ("New Zealand", "ETS"),

    # Carbon Tax
    ("Japan", "Carbon_Tax"),
    ("Estonia", "Carbon_Tax"),
    ("Finland", "Carbon_Tax"),
    ("Iceland", "Carbon_Tax"),
    ("Ireland", "Carbon_Tax"),
    ("Latvia", "Carbon_Tax"),
    ("Luxembourg", "Carbon_Tax"),
    ("Portugal", "Carbon_Tax"),
    ("Singapore", "Carbon_Tax"),
    ("South Africa", "Carbon_Tax"),
    ("Chile", "Carbon_Tax"),
    ("Colombia", "Carbon_Tax"),
    ("Costa Rica", "Carbon_Tax"),
    ("Uruguay", "Carbon_Tax"),
    ("Taiwan", "Carbon_Tax"),

    # Mixed
    ("France", "Mixed"),
    ("Netherlands", "Mixed"),
    ("Norway", "Mixed"),
    ("Poland", "Mixed"),
    ("Slovenia", "Mixed"),
    ("Spain", "Mixed"),
    ("Sweden", "Mixed"),
    ("Switzerland", "Mixed"),
    ("United Kingdom", "Mixed"),

    # No Carbon Pricing
    ("Argentina", "No_Carbon_Pricing"),
    ("Brazil", "No_Carbon_Pricing"),
    ("India", "No_Carbon_Pricing"),
    ("Indonesia", "No_Carbon_Pricing"),
    ("Mexico", "No_Carbon_Pricing"),
    ("Nigeria", "No_Carbon_Pricing"),
    ("Pakistan", "No_Carbon_Pricing"),
    ("Philippines", "No_Carbon_Pricing"),
    ("Russia", "No_Carbon_Pricing"),
    ("Saudi Arabia", "No_Carbon_Pricing"),
    ("Turkey", "No_Carbon_Pricing"),
    ("United States", "No_Carbon_Pricing"),
    ("Vietnam", "No_Carbon_Pricing"),
    ("Egypt", "No_Carbon_Pricing"),
]
cursor.executemany("INSERT INTO policy_types (country, policy_type) VALUES (?, ?)", data)
conn.commit()
df = pd.read_csv("Project_data.csv", skiprows=2)
df_co2 = df[[ "Country Name", "2014", "2024"]].copy()
df_co2.columns = ["country", "co2_2014", "co2_2024"]
print(df_co2.columns)
df_co2[["co2_2014", "co2_2024"]] = (df_co2[["co2_2014", "co2_2024"]].apply(pd.to_numeric, errors="coerce"))
df_co2 = df_co2.dropna()
cursor.execute("DROP TABLE IF EXISTS co2_data")
cursor.execute("CREATE TABLE co2_data (country TEXT PRIMARY KEY, co2_2014 REAL, co2_2024 REAL)")
cursor.executemany("INSERT INTO co2_data (country, co2_2014, co2_2024) VALUES (?, ?, ?)", df_co2.values.tolist())
conn.commit()
query = "SELECT c.country, p.policy_type, c.co2_2014, c.co2_2024, (c.co2_2024 - c.co2_2014) AS delta_co2 FROM co2_data c JOIN policy_types p ON c.country = p.country "
df_merged = pd.read_sql_query(query, conn)
df_merged["pct_reduction"] = df_merged["delta_co2"] / df_merged["co2_2014"] * 100
result = (
    df_merged
    .groupby("policy_type")
    .apply(lambda g: pd.Series({
        "avg_pct_reduction": (g["pct_reduction"] * g["co2_2014"]).sum() / g["co2_2014"].sum(),
        "median_pct_reduction": g["pct_reduction"].median(),
        "countries": g["country"].nunique()
    })).reset_index()
)
result.to_csv("policy_comparison_2014_2024.csv", index=False)
plot_data = result.sort_values("avg_pct_reduction")
plt.figure(figsize=(10, 6))
plt.bar(plot_data["policy_type"],plot_data["avg_pct_reduction"])
for policy in plot_data["policy_type"]:
    y = df_merged.loc[
        df_merged["policy_type"] == policy,
        "pct_reduction"
    ]
    x = [policy] * len(y)
plt.axhline(0, linestyle="--", linewidth=1)
plt.title("Changes in CO₂ Emissions by Climate Policy Type (2014–2024)")
plt.ylabel("Change in CO₂ emissions, %")
plt.xlabel("Type of climate policy")
plt.xticks(rotation=30, ha="right")
plt.tight_layout()
plt.show()