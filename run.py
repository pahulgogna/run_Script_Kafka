import subprocess
import threading
import time
import sys


# change these paths according to your setup
ZOO_KEEPER_START_FILE_COMMAND = 'E:/kafka/bin/windows/zookeeper-server-start.bat E:/kafka/config/zookeeper.properties'
KAFKA_SERVER_START_FILE_COMMAND = 'E:/kafka/bin/windows/kafka-server-start.bat E:/kafka/config/server.properties'

def start_process(cmd, shell = True, start_new_thread = True):
    try:
        subprocess.run(cmd , shell=shell, start_new_session=start_new_thread)
        
    except:
        raise Exception

def main():
    print('running...')
    p1 = threading.Thread(target=start_process, args= (ZOO_KEEPER_START_FILE_COMMAND,))
    p1.start()

    time.sleep(20)

    p2 = threading.Thread(target=start_process, args= (KAFKA_SERVER_START_FILE_COMMAND,))
    p2.start()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        pass
    
    