import os
import pandas as pd
import shutil

def list_subfolders(root_folder):
    subfolders = []
    for foldername, subfolder_list, _ in os.walk(root_folder):
        for subfolder in subfolder_list:
            subfolders.append(os.path.join(foldername, subfolder))
    return subfolders

def read_and_save_csv_files(source_folder, destination_folder):
    subfolders = list_subfolders(source_folder)

    for subfolder in subfolders:
        print(subfolder)
        relative_subfolder = os.path.relpath(subfolder, source_folder)
        destination_subfolder = os.path.join(destination_folder, relative_subfolder)
        os.makedirs(destination_subfolder, exist_ok=True)

        for filename in os.listdir(subfolder):
            if filename.endswith('.csv') or filename.endswith('.json'):
                csv_path = os.path.join(subfolder, filename)
                print("load "+csv_path)
                if filename.endswith('.csv'):
                    df = pd.read_csv(csv_path)
                if filename.endswith('.json'):
                    df = pd.read_json(csv_path, lines=True)
                print(df.head())

                destination_file_path = os.path.join(destination_subfolder, filename[:-4])
                print("save "+destination_file_path)
                df_to_parquet(df, destination_file_path)

def df_to_parquet(df, target_dir, chunk_size=1000000, **parquet_wargs):
    for i in range(0, len(df), chunk_size):
        slc = df.iloc[i : i + chunk_size]
        chunk = int(i/chunk_size)
        fname = target_dir+f"part_{chunk:04d}.parquet"
        print("write "+fname)
        slc.to_parquet(fname, engine="pyarrow", **parquet_wargs)

if __name__ == "__main__":
    source_folder = "/Users/quentin.ambard/Downloads/landing_zone"
    destination_folder = "/Users/quentin.ambard/Downloads/landing_zone_parquet"

    read_and_save_csv_files(source_folder, destination_folder)
