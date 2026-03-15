import pandas as pd
import pyodbc

SERVER = "asher3456.database.windows.net"
DATABASE = "neostats"
USERNAME = "asher"
PASSWORD = "" #please mail me for the password

conn = pyodbc.connect(
    f"DRIVER={{ODBC Driver 18 for SQL Server}};"
    f"SERVER={SERVER};DATABASE={DATABASE};"
    f"UID={USERNAME};PWD={PASSWORD};"
    f"Encrypt=yes;TrustServerCertificate=no;"
)

cursor = conn.cursor()

cursor.execute("""
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='server_logs' AND xtype='U')
CREATE TABLE server_logs (
    Server_ID NVARCHAR(100),
    OS_Type NVARCHAR(100),
    Server_Location NVARCHAR(100),
    Hostname NVARCHAR(MAX),
    IP_Address NVARCHAR(MAX),
    Admin_Name NVARCHAR(MAX),
    Admin_Email NVARCHAR(MAX),
    Admin_Phone NVARCHAR(MAX),
    CPU_Utilization FLOAT,
    Memory_Usage FLOAT,
    Disk_IO FLOAT,
    Network_In FLOAT,
    Network_Out FLOAT,
    Uptime_Hours FLOAT,
    Downtime_Hours FLOAT,
    CPU_Health NVARCHAR(10),
    Memory_Health NVARCHAR(10),
    Disk_Health NVARCHAR(10),
    Availability_Pct FLOAT,
    Total_Network_MBs FLOAT,
    Load_Score FLOAT,
    Server_Role NVARCHAR(50),
    Instance_Size NVARCHAR(20),
    Region_Group NVARCHAR(50),
    Log_Timestamp DATETIME2,
    Date DATE,
    Hour INT
)
""")

cursor.execute("""
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='daily_summary' AND xtype='U')
CREATE TABLE daily_summary (
    Server_ID NVARCHAR(100),
    Date DATE,
    Server_Location NVARCHAR(100),
    OS_Type NVARCHAR(100),
    Instance_Size NVARCHAR(20),
    Server_Role NVARCHAR(50),
    Region_Group NVARCHAR(50),
    Avg_CPU FLOAT,
    Max_CPU FLOAT,
    Avg_Memory FLOAT,
    Max_Memory FLOAT,
    Avg_Disk FLOAT,
    Avg_Availability FLOAT,
    Avg_Uptime FLOAT,
    Avg_Load_Score FLOAT,
    Bad_Events INT,
    Total_Records INT
)
""")

conn.commit()
print("Tables created")

df = pd.read_csv("data/plogs.csv")

df["Date"] = pd.to_datetime(df["Date"]).dt.date
df["Log_Timestamp"] = pd.to_datetime(df["Log_Timestamp"])

for _, row in df.iterrows():
    cursor.execute("""
        INSERT INTO server_logs VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """,
        row["Server_ID"], row["OS_Type"], row["Server_Location"],
        row["Hostname"], row["IP_Address"], row["Admin_Name"],
        row["Admin_Email"], row["Admin_Phone"],
        row["CPU_Utilization (%)"], row["Memory_Usage (%)"], row["Disk_IO (%)"],
        row["Network_Traffic_In (MB/s)"], row["Network_Traffic_Out (MB/s)"],
        row["Uptime (Hours)"], row["Downtime (Hours)"],
        str(row["CPU_Health"]), str(row["Memory_Health"]), str(row["Disk_Health"]),
        row["Availability_Pct"], row["Total_Network_MBs"], row["Load_Score"],
        row["Server_Role"], row["Instance_Size"], row["Region_Group"],
        row["Log_Timestamp"], row["Date"], int(row["Hour"])
    )

conn.commit()
print(f"Inserted {len(df)} rows into server_logs")

ds = pd.read_csv("data/daily_summary.csv")

ds["Date"] = pd.to_datetime(ds["Date"]).dt.date

for _, row in ds.iterrows():
    cursor.execute("""
        INSERT INTO daily_summary VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """,
        row["Server_ID"], row["Date"], row["Server_Location"], row["OS_Type"],
        row["Instance_Size"], row["Server_Role"], row["Region_Group"],
        row["Avg_CPU"], row["Max_CPU"], row["Avg_Memory"], row["Max_Memory"],
        row["Avg_Disk"], row["Avg_Availability"], row["Avg_Uptime"], row["Avg_Load_Score"],
        int(row["Bad_Events"]), int(row["Total_Records"])
    )

conn.commit()
print(f"Inserted {len(ds)} rows into daily_summary")

cursor.close()
conn.close()
