import os

def get_info():
    #username = 'kirizawa'
    username = 'adam_a_oppenheimer'
    ip = '35.224.130.72'
    login = username + '@' + ip
    return login

def move_files():
    login = get_info()

    os.system('scp -i ~/.ssh/google-cloud-cs123 -r ../dataproc_work ' + login + ':~/')
    # os.system('scp -i ~/.ssh/google-cloud-cs123 ../converting_to_csvs/file_dates.csv ' + login + ':~/')

def move_extract_csv():
    login = get_info()
    os.system('scp -i ~/.ssh/google-cloud-cs123 extract_csv.py ' + login + ':~/')

def move_mr_trips():
    login = get_info()
    os.system('scp -i ~/.ssh/google-cloud-cs123 mr_trips.py ' + login + ':~/')

def move_run_project():
    login = get_info()
    os.system('scp -i ~/.ssh/google-cloud-cs123 run_project.py ' + login + ':~/')

def ssh():
    login = get_info()
    os.system('ssh -i ~/.ssh/google-cloud-cs123 ' + login)

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
                sudo pip3 install google-cloud-logging
                sudo pip3 install google-cloud-storage
                '''
    os.system(packages)

def run_code():
    os.system('time python3 dataproc_work/mr_trips.py csvs/first_hundred.csv > output.txt')

def save_output():
    login = get_info()
    os.system('scp -i ~/.ssh/google-cloud-cs123 ' + login + ':~/output.txt ./')
