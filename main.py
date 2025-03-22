#!/usr/bin/env ipython
import numpy as np
import pandas as pd
import sys

if len(sys.argv) != 2:
    print("Usage: python3 nordnet-statement.py <nordnet-statement-file>")
    sys.exit(1)

fname = sys.argv[1]

df_full = pd.read_csv(fname, sep="\t", decimal=",", encoding="utf-16")

print("Head of orginal file:")
display(df_full.head())


# Define the account mapping logic
def map_accounts(row):
    t = row["Transaksjonstype"]
    if t == "INNSKUDD":
        return "1920", "1925"
    elif t in ("KJØPT", "KJØP, BYTTE AV FOND"):
        return "1811", "1920"
    elif t == "SALG, BYTTE AV FOND":
        return "1920", "1811"
    elif t == "PLATTFORMAVGIFT":
        return "7770", "1920"
    elif t == "TILBAKEBET. FOND AVG":
        return "1920", "7770"
    elif t in ("OVERBELÅNINGSRENTE", "DEBETRENTE"):
        return "8150", "1920"
    elif t == "PLATTFORMAVG KORR":
        return "1920", "7770"
    else:
        return "", ""


# Apply account mapping
df_full["Debetkto"], df_full["Kreditkto"] = zip(*df_full.apply(map_accounts, axis=1))

# Create new accounting format DataFrame
accounting_df = pd.DataFrame()
accounting_df["Dato"] = df_full["Bokføringsdag"]

# Handle 'Inn' and 'Ut'
belop = df_full["Beløp"].fillna(0)
accounting_df["Inn"] = belop.where(belop > 0, 0)
accounting_df["Ut"] = (-belop).where(belop < 0, 0)

accounting_df["Motpart"] = np.where(
    df_full["Transaksjonstype"].str.contains("PLATTFORMAVG"),
    "Nordnet AS",
    df_full.get("Verdipapir", ""),
)
accounting_df["Beskrivelse"] = df_full.get("Transaksjonstekst", "")
accounting_df["Kategori"] = ""
accounting_df["Debetkto"] = df_full["Debetkto"]
accounting_df["Kreditkto"] = df_full["Kreditkto"]
accounting_df["Bilag"] = "Ja"
accounting_df["Detaljer"] = "-"
accounting_df["Referanse"] = df_full.get("Verifikationsnummer", "")
accounting_df["Notater"] = ""
accounting_df["Korteier"] = ""

# Merge together rows with same date and accounts
mergeable = ["PLATTFORMAVG KORR", "TILBAKEBET. FOND AVG"]

mask = df_full["Transaksjonstype"].isin(mergeable)
df_merge = accounting_df[mask]
df_rest = accounting_df[~mask]

df_agg = df_merge.groupby(["Dato", "Debetkto", "Kreditkto"], as_index=False).agg(
    {
        "Inn": "sum",
        "Ut": "sum",
        "Motpart": lambda x: ", ".join(x.dropna().unique()),
        "Beskrivelse": lambda x: ", ".join(x.dropna().unique()),
        "Kategori": "first",
        "Bilag": "first",
        "Detaljer": "first",
        "Referanse": lambda x: ", ".join(x.dropna().astype(str).unique()),
        "Notater": "first",
        "Korteier": "first",
    }
)

accounting_df = pd.concat([df_rest, df_agg], ignore_index=True)
accounting_df = accounting_df.sort_values(
    by=["Dato", "Debetkto", "Kreditkto"]
).reset_index(drop=True)
accounting_df.insert(0, "#", range(1, len(accounting_df) + 1))

print("Head of accounting df:")
display(accounting_df.set_index("#").head())

store_at = "nordnet-account-statement.csv"
print("Transformed file stored at:", store_at)
accounting_df.to_csv(store_at, sep=",", index=False, quoting=0, decimal=",")
