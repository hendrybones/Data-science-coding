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


# --------------------------------------------------------------
# Turn into function
# --------------------------------------------------------------


# --------------------------------------------------------------
# Merging datasets
# --------------------------------------------------------------


# --------------------------------------------------------------
# Resample data (frequency conversion)
# --------------------------------------------------------------

# Accelerometer:    12.500HZ
# Gyroscope:        25.000Hz


# --------------------------------------------------------------
# Export dataset
# --------------------------------------------------------------
