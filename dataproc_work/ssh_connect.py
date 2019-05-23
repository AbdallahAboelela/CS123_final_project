import os

def move_files():
    os.system('scp -i ~/.ssh/google-cloud-cs123 mr_trips.py adam_a_oppenheimer@35.202.2.231:~/')
    os.system('scp -i ~/.ssh/google-cloud-cs123 boundaries.py adam_a_oppenheimer@35.202.2.231:~/')
    os.system('scp -i ~/.ssh/google-cloud-cs123 csvs/first_hundred.csv adam_a_oppenheimer@35.202.2.231:~/')
    os.system('scp -i ~/.ssh/google-cloud-cs123 csvs/first_thousand.csv adam_a_oppenheimer@35.202.2.231:~/')
    os.system('scp -i ~/.ssh/google-cloud-cs123 ssh_connect.py adam_a_oppenheimer@35.202.2.231:~/')

def ssh():
    os.system('ssh -i ~/.ssh/google-cloud-cs123 adam_a_oppenheimer@35.202.2.231')

def setup_vm():
    os.system('sudo apt-get install python3-pip')
    os.system('sudo pip3 install mrjob')
    os.system('sudo pip3 install osmnx')
    os.system('sudo apt install python3-rtree')
    os.system('sudo pip3 install networkx')

def run_code():
    os.system('time python3 mr_trips.py first_hundred.csv > output.txt')
