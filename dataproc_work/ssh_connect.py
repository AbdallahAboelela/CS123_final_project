import os

def move_files():
    os.system('scp -i ~/.ssh/google-cloud-cs123 -r dataproc_work keiirizawa@104.154.20.36:~/')

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

def save_output():
    os.system('scp -i ~/.ssh/google-cloud-cs123 adam_a_oppenheimer@35.202.2.231:~/output.txt ./')
