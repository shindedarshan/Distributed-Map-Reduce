import sys
import os
import rpyc
import shutil
from rpyc.utils.server import ThreadedServer
from KVServer_IO import IO_Operations
import configparser

class KVServerService(rpyc.Service):
    def __init__(self):
        self.config = configparser.ConfigParser()
        self.config.read('config.ini', encoding = 'utf8')
        self.__base_directory = self.config['KVServer']['base_path']


    def exposed_get(self, id, key, role, index, mode):
        root_path = self.__base_directory + str(id) + '/' + role + '/'
        if index >= 0:
            worker_path = root_path + role + '_' + str(index) + '/'
        else:
            worker_path = root_path
        worker_mode_path = worker_path + mode + '/'
        file_path = worker_mode_path + str(key) + '.txt'
        try:
            if os.path.exists(file_path):
                return IO_Operations.read_file(file_path)
            else:
                raise Exception('Key: ' + str(file_path) + ' not exists in KVServer for role ' + role)
        except Exception as e:
            print(e)
            raise Exception('Some error occurred during fetching data from KVServer while get in mode ' + mode)


    def exposed_put(self, id, key, value, role, index, mode):
        base_path = self.__base_directory + str(id) + '/'
        if not os.path.exists(base_path):
            os.makedirs(base_path)

        root_path = base_path + role + '/'
        if not os.path.exists(root_path):
            os.makedirs(root_path)
        
        worker_path = root_path + role + '_' + str(index) + '/'
        if not os.path.exists(worker_path):
            os.makedirs(worker_path)
            
        worker_mode_path = worker_path + mode + '/'
        if not os.path.exists(worker_mode_path):
            os.makedirs(worker_mode_path)
            
        file_name = str(key) + '.txt'
        return IO_Operations.update_file(worker_mode_path, file_name, value)


    def exposed_clear(self, id):
        base_path = self.__base_directory + str(id) + '/'
        delete_dir = os.listdir(base_path)
        self.collect_final_results(base_path, base_path + self.config['KVServer']['final_result_dir'] + '/')
        for directory in delete_dir:
            shutil.rmtree(base_path + directory)


    def collect_final_results(self, base_path, dest_path):
        os.makedirs(dest_path)
        reducer_path = base_path + self.config['Master']['reduce_role_name'] + '/'
        for worker in os.listdir(reducer_path):
            final_path = reducer_path + worker + '/' + self.config['Master']['output_dir'] + '/'
            files = os.listdir(final_path)
            for f in files:
                shutil.copy(final_path + f, dest_path)

    def exposed_getKeys(self, id, role, index, mode):
        root_path = self.__base_directory + str(id) + '/' + role + '/'
        worker_path = root_path + role + '_' + str(index) + '/'
        worker_mode_path = worker_path + mode + '/'
        
        try:
            if os.path.exists(worker_mode_path):
                return os.listdir(worker_mode_path)
            else:
                print('worker mode path', worker_mode_path, 'not found...')
                raise Exception('Worker mode path ', worker_mode_path, ' not found...')
        except Exception:
            raise Exception('Some error occurred during fetching data from KVServer while get keys in mode ' + mode + ' for role ' + role)
    

    def exposed_combine(self, id, key, src_role, src_mode, dest_role, dest_mode = 'output'):
        dest_path = self.__base_directory + str(id) + '/' + dest_role + '/'
        if not os.path.exists(dest_path):
            os.makedirs(dest_path)
        
        dest_mode_path = dest_path + dest_mode + '/'
        if not os.path.exists(dest_mode_path):
            os.makedirs(dest_mode_path)
        
        src_role_path = self.__base_directory + str(id) + '/' + src_role + '/'

        index = 0
        key = key.split('.')[0]
        for src_worker in os.listdir(src_role_path):
            src_mode_path = src_role_path + src_worker + '/' + src_mode + '/'
            file_path = src_mode_path + key + '.txt'
            try:
                if os.path.exists(file_path):
                    value = self.exposed_get(id, key, src_role, index, src_mode)
                    IO_Operations.update_file(dest_mode_path, key + '.txt', value)
            except Exception:
                raise Exception('Some error occurred during fetching data from KVServer')
            index += 1
        print('Done with combiner task for key: ' + key)


if __name__ == '__main__':
    port = int(self.config['KVServer']['KVServer_port'])
    rpyc.core.protocol.DEFAULT_CONFIG['sync_request_timeout'] = None
    server = ThreadedServer(KVServerService, port = port, protocol_config = rpyc.core.protocol.DEFAULT_CONFIG)
    try:
        server.start()
    except Exception:
        server.stop()
