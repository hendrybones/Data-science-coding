import pandas as pd
from glob import glob


# --------------------------------------------------------------
# Read single CSV file
# --------------------------------------------------------------
Single_file_acc=pd.read_csv(
    "../../data/raw/MetaMotion/A-bench-heavy2-rpe8_MetaWear_2019-01-11T16.10.08.270_C42732BE255C_Accelerometer_12.500Hz_1.4.4.csv")
Single_file_acc

Single_file_gyr=pd.read_csv("../../data/raw/MetaMotion/A-bench-heavy2-rpe8_MetaWear_2019-01-11T16.10.08.270_C42732BE255C_Gyroscope_25.000Hz_1.4.4.csv")
# --------------------------------------------------------------
# List all data in data/raw/MetaMotion
# --------------------------------------------------------------
File= glob("../../data/raw/MetaMotion/*.csv")
len(File)
#checking the file name
File[0]



# --------------------------------------------------------------
# Extract features from filename
# --------------------------------------------------------------
data_path="../../data/raw/MetaMotion/"
f= File[1]


f.split("-")[0]

participants = f.split("/")[-1].split("\\")[-1].split("-")[0]
# participants = f.split("-")[0].replace(data_path, " ")
Label =f.split("-")[1]
Category=f.split("-")[2].rstrip("123").rstrip("_MetaWear_2019")

df= pd.read_csv(f)

df["participants"]=participants
df["Label"]=Label
df["Category"]=Category
df


# --------------------------------------------------------------
# Read all files
# --------------------------------------------------------------
acc_df=pd.DataFrame()
gyr_df=pd.DataFrame()

# We set a counter for the sets
acc_sets=1
gyr_sets=1

for f in File:
    participants = f.split("/")[-1].split("\\")[-1].split("-")[0]
    Label =f.split("-")[1]
    Category=f.split("-")[2].rstrip("123").rstrip("_MetaWear_2019")
    
    df =pd.read_csv(f)
    df["participants"]=participants
    df["Label"]=Label
    df["Category"]=Category
    
    if "Accelerometer" in f:
        df["set"]=acc_sets
        acc_sets +=1
        acc_df = pd.concat([acc_df, df])

    if "Gyroscope" in f:
        df["set"]=gyr_sets
        gyr_sets +=1
        gyr_df = pd.concat([gyr_df, df])


# --------------------------------------------------------------
# Working with datetimes
# --------------------------------------------------------------

acc_df.info()
gyr_df.info()

pd.to_datetime(df["epoch (ms)"],unit="ms")
df["time (01:00)"].dt.month
pd.to_datetime(df["time (01:00)"].dt.month)


acc_df.index=pd.to_datetime(acc_df["epoch (ms)"],unit="ms")
gyr_df.index=pd.to_datetime(gyr_df["epoch (ms)"],unit="ms")

del acc_df["epoch (ms)"]
del acc_df["time (01:00)"]
del acc_df["elapsed (s)"]

del gyr_df["epoch (ms)"]
del gyr_df["time (01:00)"]
del gyr_df["elapsed (s)"]

acc_df.drop(columns=['epoch (ms)'], errors='ignore')
acc_df.drop(columns=['time (01:00)'], errors='ignore')
acc_df.drop(columns=['elapsed (s)'], errors='ignore')

gyr_df.drop(columns=['epoch (ms)'], errors='ignore')
gyr_df.drop(columns=['time (01:00)'], errors='ignore')
gyr_df.drop(columns=['elapsed (s)'], errors='ignore')

# --------------------------------------------------------------
# Turn into function
# --------------------------------------------------------------
File= glob("../../data/raw/MetaMotion/*.csv")
def read_data_from_file(File):
    # Initialize accelerometer and gyroscope DataFrames
    acc_df = pd.DataFrame()  # Corrected from cc_df to acc_df
    gyr_df = pd.DataFrame()

    # Set counters for the sets
    acc_sets = 0
    gyr_sets = 0

    for f in File:
        # Extracting participant, label, and category information from the filename
        participants = f.split("/")[-1].split("\\")[-1].split("-")[0]
        Label = f.split("-")[1]
        Category = f.split("-")[2].rstrip("123").rstrip("_MetaWear_2019")

        # Read the CSV file into a DataFrame
        df = pd.read_csv(f)
        df["participants"] = participants
        df["Label"] = Label
        df["Category"] = Category

        # For Accelerometer data
        if "Accelerometer" in f:
            df["set"] = acc_sets
            acc_sets += 1
            acc_df = pd.concat([acc_df, df], ignore_index=True)  # Concatenate with existing acc_df

        # For Gyroscope data
        if "Gyroscope" in f:
            df["set"] = gyr_sets
            gyr_sets += 1
            gyr_df = pd.concat([gyr_df, df], ignore_index=True)  # Concatenate with existing gyr_df

    # Convert epoch (ms) to datetime index for both DataFrames
    acc_df.index = pd.to_datetime(acc_df["epoch (ms)"], unit="ms")
    gyr_df.index = pd.to_datetime(gyr_df["epoch (ms)"], unit="ms")

    # Remove unnecessary columns
    del acc_df["epoch (ms)"]
    del acc_df["time (01:00)"]
    del acc_df["elapsed (s)"]

    del gyr_df["epoch (ms)"]
    del gyr_df["time (01:00)"]
    del gyr_df["elapsed (s)"]
    # Return the processed accelerometer and gyroscope DataFrames
    return acc_df, gyr_df

data_merged=read_data_from_file(File)
# --------------------------------------------------------------
# Merging datasets
# --------------------------------------------------------------

acc_df.reset_index(inplace=True)
gyr_df.reset_index(inplace=True)
data_merged = pd.concat([acc_df.iloc[:, :3], gyr_df], axis=1)
data_merged.dropna()

data_merged.columns=[
    "acc_x",
    "acc_y",
    "acc_z",
    "gyr_x",
    "gyr_y",
    "gyr_z",
    "Label",
    "category",
    "participant",
    "set",
]
# --------------------------------------------------------------
# Resample data (frequency conversion)
# --------------------------------------------------------------

# Accelerometer:    12.500HZ
# Gyroscope:        25.000Hz

sampling={
    'acc_x':"mean", 
    'acc_y':"mean", 
    'acc_z':"mean", 
    'gyr_x':"mean", 
    'gyr_y':"mean", 
    'gyr_z':"mean", 
    'Label':"last",
    'category':"last",
    'participant':"last", 
    'set':"last"
    }

data_merged.columns

data_merged[:100].resample(rule="200ms").apply(sampling)


# --------------------------------------------------------------
# Export dataset
# --------------------------------------------------------------
