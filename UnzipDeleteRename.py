import os
import numpy as np
import pandas as pd
import utilibs
import sys
import zipfile

import zipfile,fnmatch,os
#This is for unzipping .zip files in a folder

def unzipfiles(rootPath):
    pattern = '*.zip'
    for root, dirs, files in os.walk(rootPath):
        for filename in fnmatch.filter(files, pattern):
            print(os.path.join(root, filename))
            zipfile.ZipFile(os.path.join(root, filename)).extractall(os.path.join(root, os.path.splitext(filename)[0]))

def deletefilesunder800(rPath):
    import os, os.path
#This is for deleting files under 800 kb
    for root, _, files in os.walk(rPath):
        for f in files:
            fullpath = os.path.join(root, f)
            try:
                if os.path.getsize(fullpath) < 800 * 1024:   #set file size in kb
                    print(fullpath)
                    os.remove(fullpath)
            except WindowsError:
                print ("Error" + fullpath)
                
def deletemat(dirpath):
    for parent, dirnames, filenames in os.walk(dirpath):
        for fn in filenames:
            if fn.lower().endswith('.mat'):
                os.remove(os.path.join(parent, fn))

def deletecsv(directorypath):                
    for parent, dirnames, filenames in os.walk(directorypath):
        for fn in filenames:
            if fn.lower().endswith('.csv'):
                os.remove(os.path.join(parent, fn))

def csv_files(source_dir):
    for filename in os.listdir(source_dir):
        if filename.endswith('.csv'):
            yield filename

def zipfiles(source_dir,dest_dir):
    os.chdir(dest_dir)
    for csv_filename in csv_files(source_dir):
        file_root = os.path.splitext(csv_filename)[0]
        zip_file_name = file_root + '.zip'
        zip_file_path = os.path.join(dest_dir, zip_file_name)
        with zipfile.ZipFile(zip_file_path, 'w', zipfile.ZIP_DEFLATED) as zf:
            zf.write(csv_filename)
            
def converttoparquet(flist, target_folder):
    for i, fpath in enumerate(flist):
        #fname = fpath.split('\\')[-1]
        df = pd.read_csv(fpath)
        fname = fpath.split('\\')[-1].split('.')[0] + '.parquet'
        print(f"{i:03} ... Working on file ... {fname}")
        df.to_parquet(f"{target_folder}{fname}", compression="gzip")
                
def renamefiles(path):   
          
#This is to rename files

    count = 1

    ori_filename = []
    new_filename = []
    folder = []
    head, tail = os.path.split(path)
    for root, dirs, files in os.walk(path):
        for file in files:
            new_filecode = "flight_" + str(1000000 + count) +".parquet"
        
            ori_filename.append(os.path.basename(file))
            new_filename.append(new_filecode)
            folder.append(os.path.basename(root))
        
            fullpath = os.path.join(root,file)
            os.rename(fullpath, os.path.join(root, new_filecode))
        
            count += 1

#Store data to csv
    df = pd.DataFrame(list(zip(ori_filename, new_filename, folder)), columns = ['raw_file','file_id','tail_number'])
    df.to_csv(r'D:\Proyekan\FILES\Metadata.csv',index = False, header = True)
