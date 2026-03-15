import os
import pandas as pd
from azure.storage.blob import BlobServiceClient

CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING", "")
CONTAINER = "rlogs"
FILE = r"C:/Users/asher/Desktop/neostats/data/Sample_Data_Ingestion.csv"

df = pd.read_csv(FILE)

print("Shape of the dataframe:", df.shape)
print("null:\n", df.isnull().sum())
print("duplicate:", df.duplicated().sum())

df.dropna(inplace=True)
df.drop_duplicates(inplace=True)

df = df[(df["CPU_Utilization (%)"] >= 0) & (df["CPU_Utilization (%)"] <= 100)]
df = df[(df["Memory_Usage (%)"] >= 0) & (df["Memory_Usage (%)"] <= 100)]
df = df[(df["Disk_IO (%)"] >= 0) & (df["Disk_IO (%)"] <= 100)]

df.to_csv("data/clean_raw.csv", index=False)

print("Data saved to clean_raw.csv in data folder")

client = BlobServiceClient.from_connection_string(CONNECTION_STRING)
container_client = client.get_container_client(CONTAINER)

with open("data/clean_raw.csv", "rb") as f:
    container_client.upload_blob(name="clean_raw.csv", data=f, overwrite=True)

print("Uploaded adls rlogs container")
