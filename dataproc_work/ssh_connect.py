import os

def move_files():
    os.system('scp -i ~/.ssh/google-cloud-cs123 -r ../dataproc_work kirizawa@104.154.20.36:~/')
    os.system('scp -i ~/.ssh/google-cloud-cs123 extract_csv.py kirizawa@104.154.20.36:~/')
    # os.system('scp -i ~/.ssh/google-cloud-cs123 ../converting_to_csvs/file_dates.csv kirizawa@104.154.20.36:~/')

def ssh():
    os.system('ssh -i ~/.ssh/google-cloud-cs123 kirizawa@104.154.20.36')

def setup_vm():
    packages =  '''
                sudo apt-get update
                sudo apt-get install python3-pip -y
                sudo pip3 install numpy
                sudo pip3 install pandas
                sudo pip3 install geopandas
                sudo pip3 install networkx
                sudo pip3 install shapely
                sudo pip3 install mrjob
                '''
    os.system(packages)

def run_code():
    os.system('time python3 dataproc_work/mr_trips.py csvs/first_hundred.csv > output.txt')

def save_output():
    os.system('scp -i ~/.ssh/google-cloud-cs123 adam_a_oppenheimer@104.154.20.36:~/output.txt ./')
