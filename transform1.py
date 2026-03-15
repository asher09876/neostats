import pandas as pd
from azure.storage.blob import BlobServiceClient
from cryptography.fernet import Fernet

CONNECTION_STRING = ""
CONTAINER = "plogs"

df = pd.read_csv("data/clean_raw.csv")

key = Fernet.generate_key()
cipher = Fernet(key)

with open("data/encryption.key", "wb") as f:
    f.write(key)

df["IP_Address"] = df["IP_Address"].apply(lambda x: cipher.encrypt(str(x).encode()).decode())
df["Hostname"] = df["Hostname"].apply(lambda x: cipher.encrypt(str(x).encode()).decode())
df["Admin_Name"] = df["Admin_Name"].apply(lambda x: cipher.encrypt(str(x).encode()).decode())
df["Admin_Email"] = df["Admin_Email"].apply(lambda x: cipher.encrypt(str(x).encode()).decode())
df["Admin_Phone"] = df["Admin_Phone"].apply(lambda x: cipher.encrypt(str(x).encode()).decode())

print("PII encrypted")

df["CPU_Health"] = pd.cut(df["CPU_Utilization (%)"], bins=[0, 50, 80, 100], labels=["GOOD", "MODERATE", "CRITICAL"])
df["Memory_Health"] = pd.cut(df["Memory_Usage (%)"], bins=[0, 60, 85, 100], labels=["GOOD", "MODERATE", "CRITICAL"])
df["Disk_Health"] = pd.cut(df["Disk_IO (%)"], bins=[0, 60, 85, 100], labels=["GOOD", "MODERATE", "CRITICAL"])

df["Availability_Pct"] = ((df["Uptime (Hours)"] / (df["Uptime (Hours)"] + df["Downtime (Hours)"])) * 100).round(2)
df["Total_Network_MBs"] = df["Network_Traffic_In (MB/s)"] + df["Network_Traffic_Out (MB/s)"]
df["Load_Score"] = ((df["CPU_Utilization (%)"] + df["Memory_Usage (%)"] + df["Disk_IO (%)"]) / 3).round(2)

df["Log_Timestamp"] = pd.to_datetime(df["Log_Timestamp"], format="%d-%m-%Y %H:%M")
df["Date"] = df["Log_Timestamp"].dt.date
df["Hour"] = df["Log_Timestamp"].dt.hour

raw_hostnames = pd.read_csv("data/clean_raw.csv", usecols=["Server_ID", "Hostname"])


def get_server_role(hostname):
    h = str(hostname).lower()
    if "web" in h:
        return "Web Server"
    elif "database" in h or "db" in h:
        return "Database"
    elif "cache" in h:
        return "Cache"
    elif "api" in h:
        return "API"
    elif "app" in h:
        return "Application"
    elif "backup" in h:
        return "Backup"
    elif "analytics" in h:
        return "Analytics"
    else:
        return "Other"


def get_instance_size(hostname):
    h = str(hostname).lower()
    if "database" in h or "analytics" in h:
        return "Large"
    elif "app" in h or "api" in h:
        return "Medium"
    else:
        return "Small"


def get_region(location):
    location = str(location).lower()
    if "london" in location or "berlin" in location:
        return "Europe"
    elif "new york" in location:
        return "Americas"
    elif "tokyo" in location or "sydney" in location:
        return "Asia Pacific"
    else:
        return "Other"


# Apply using the plain Hostname from raw_hostnames, keyed on Server_ID
df["Server_Role"] = df["Server_ID"].map(
    raw_hostnames.set_index("Server_ID")["Hostname"].apply(get_server_role)
)
df["Instance_Size"] = df["Server_ID"].map(
    raw_hostnames.set_index("Server_ID")["Hostname"].apply(get_instance_size)
)
df["Region_Group"] = df["Server_Location"].apply(get_region)

df.to_csv("data/plogs.csv", index=False)
print("plogs.csv saved")


summary = df.groupby(["Server_ID", "Date", "Server_Location", "OS_Type", "Instance_Size", "Server_Role", "Region_Group"]).agg(
    Avg_CPU=("CPU_Utilization (%)", "mean"),
    Max_CPU=("CPU_Utilization (%)", "max"),
    Avg_Memory=("Memory_Usage (%)", "mean"),
    Max_Memory=("Memory_Usage (%)", "max"),
    Avg_Disk=("Disk_IO (%)", "mean"),
    Avg_Availability=("Availability_Pct", "mean"),
    Avg_Uptime=("Uptime (Hours)", "mean"),
    Avg_Load_Score=("Load_Score", "mean"),
    Bad_Events=("CPU_Health", lambda x: (x == "BAD").sum()),
    Total_Records=("Server_ID", "count")
).reset_index()

summary.to_csv("data/daily_summary.csv", index=False)
print("daily_summary.csv saved")

client = BlobServiceClient.from_connection_string(CONNECTION_STRING)
container_client = client.get_container_client(CONTAINER)

with open("data/plogs.csv", "rb") as f:
    container_client.upload_blob(name="plogs.csv", data=f, overwrite=True)

with open("data/daily_summary.csv", "rb") as f:
    container_client.upload_blob(name="daily_summary.csv", data=f, overwrite=True)

