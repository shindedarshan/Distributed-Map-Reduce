import gcp
import rpyc
import os, time
from rpyc.utils.server import ThreadedServer
import math
import configparser
from multiprocessing import Process, Queue

class MasterService(rpyc.Service):
    def __init__(self):
        self.config = configparser.ConfigParser()
        self.config.read('config.ini', encoding = 'utf8')
        
        self.__KVServer_port = int(self.config['KVServer']['KVServer_port'])
                                       
        self.__gcp_api = gcp.GCP_APIs()
        self.__project = self.config['GCP']['project_id']
        self.__zone = self.config['GCP']['zone']
        
        self.__worker_ext_ips = []

    def on_connect(self, conn):
        self.__worker_connections = []
        pass

    def on_disconnect(self, conn):
        pass

    def exposed_init_cluster(self, n_worker):
        try:
            _, self.__KVServer_address = self.__gcp_api.getIPAddresses(self.__project, self.__zone, self.config['KVServer']['kvserver_name'])
            print('KVServer instance already exists', end = ' ')
        except:
            print('Starting KVServer...', end = ' ')
            self.__gcp_api.create_instance(self.__project, self.__zone, self.config['KVServer']['kvserver_name'], self.config['Master']['master_name'])
            _, self.__KVServer_address = self.__gcp_api.getIPAddresses(self.__project, self.__zone, self.config['KVServer']['kvserver_name'])

        while True:
            try:
                rpyc.core.protocol.DEFAULT_CONFIG['sync_request_timeout'] = None
                self.__KVServer = rpyc.connect(self.__KVServer_address, self.__KVServer_port,
                                               config = rpyc.core.protocol.DEFAULT_CONFIG).root
                print('Connection status... CONNECTED')
                break
            except:
                continue

        self.__num_worker = n_worker
        self.__spawn_worker()
        return int(time.time())
    
    def __start_node(self, index, queue):
        Done = False
        while not Done:
            try:
                self.__gcp_api.create_instance(self.__project, self.__zone, self.config['Worker']['worker_name_prefix'] + str(index), self.config['Master']['master_name'])
                _, ext_ip = self.__gcp_api.getIPAddresses(self.__project, self.__zone, self.config['Worker']['worker_name_prefix'] + str(index))
                Done = True
                print(self.config['Worker']['worker_name_prefix'] + str(index), 'instance launched...')
                queue.put(ext_ip)
            except Exception as e:
                raise e
       
            
    def __spawn_worker(self):
        for i in range(self.__num_worker):
            queue = Queue()
            process = Process(target = self.__start_node, args = (i,queue))
            process.start()
            process.join()
            self.__worker_ext_ips.append(queue.get())

        # Wait until all vms launched
        while len(self.__worker_ext_ips) != self.__num_worker:
            continue

        # capture and keep record of connections with all workers
        for ip in self.__worker_ext_ips:
            port = int(self.config['Worker']['worker_port'])
            worker_started = False
            
            print('Trying to connect instance on ip', ip, end = '... ')
            while not worker_started:
                # Wait till starting rpyc server on worker node...
                try:
                    rpyc.core.protocol.DEFAULT_CONFIG['sync_request_timeout'] = None
                    conn = rpyc.connect(str(ip), port=port, config = rpyc.core.protocol.DEFAULT_CONFIG)
                    worker_started = True
                    self.__worker_connections.append(conn.root)
                    print('CONNECTED')
                except Exception:
                    time.sleep(1)
        print('Cluster initialized...\n')


    def exposed_run_mapreduce(self, id, data, map_func, reduce_func, output_location):
        print('Starting map-reduce task...')
        mappers = []
        
        # Generate mapper inputs by splitting data
        self.__mapper_input(id, data)

        for i, mapper in enumerate(self.__worker_connections):
            mappers.append(rpyc.async_(mapper.execute)(id, map_func, 
                           self.config['Master']['map_role_name'], i, self.__KVServer_address, 
                           self.__KVServer_port))
            mappers[i].set_expiry(None)

        # wait till all workers completes its assigned task
        for mapper in mappers:
            while not mapper.ready:
                continue
        print('Mappers have completed their assigned task...')
        
        self.__combine_map_outputs(id)
        
        reducers = []
        reducer_response = []
        
        # Generate reducer inputs by splitting keys of intermidiate data
        self.__reducer_input(id)

        for i, reducer in enumerate(self.__worker_connections):
            reducers.append(rpyc.async_(reducer.execute)(id, reduce_func, 
                            self.config['Master']['reduce_role_name'], i, self.__KVServer_address, 
                            self.__KVServer_port))
            reducers[i].set_expiry(None)

        # wait till all workers completes its assigned task
        for reducer in reducers:
            while not reducer.ready:
                continue
        
        for i in range(len(reducers)):
            keys = self.__KVServer.getKeys(id, self.config['Master']['reduce_role_name'], i, self.config['Master']['output_dir'])
            for key in keys:
                reducer_response.append(('.'.join(key.split('.')[:-1]), self.__KVServer.get(id, key.split('.')[0], self.config['Master']['reduce_role_name'], i, self.config['Master']['output_dir'])))
        print('Reducers has completed their assigned task...')

        # Generate final output to store at output location and to send in reply to client
        reducer_response = sorted(reducer_response, key = lambda x: -int(x[1]))
        try:
            with open(output_location, 'w') as fp:
                fp.write('\n'.join('{} {}'.format(x[0],x[1]) for x in reducer_response))
        except Exception:
            print('Error occured while writing output to output_location.')
        print('Map-reduce task has been completed...')
        return reducer_response


    def __mapper_input(self, id, path):
        print('Generating mapper input...')
        # When data is a single file
        if os.path.isfile(path):
            data = open(path, 'r').read()
            words = data.split()
            n = int(math.ceil(len(words) / self.__num_worker))
            for i in range(len(self.__worker_connections)):
                if i != self.__num_worker - 1:
                    mapper_input = ' '.join(words[i*n:(i+1)*n])
                else:
                    mapper_input = ' '.join(words[i*n:])
                self.__KVServer.put(id, self.config['Master']['input_dir'], mapper_input, self.config['Master']['map_role_name'], i, self.config['Master']['input_dir'])
        # When data is a directory
        elif os.path.isdir(path):
            files = os.listdir(path)
            n = math.ceil(len(files) / self.__num_worker)
            for i in range(len(self.__worker_connections)):
                if i != self.__num_worker - 1:
                    mapper_input = files[i*n:(i+1)*n]
                else:
                    mapper_input = files[i*n:]
                self.__KVServer.put(id, self.config['Master']['input_dir'], mapper_input, self.config['Master']['map_role_name'], i, self.config['Master']['input_dir'])
    

    def __combine_map_outputs(self, id):
        print('Performing combine operation...')
        keys = self.__get_keys_intermidiate_data(id, self.config['Master']['map_role_name'], self.config['Master']['output_dir'])
        for key in keys:
            self.__KVServer.combine(id, key, self.config['Master']['map_role_name'], self.config['Master']['output_dir'], self.config['Master']['combine_role_name'])


    def __reducer_input(self, id):
        print('Generating reducer input...')
        keys = list(set(self.__get_keys_intermidiate_data(id, self.config['Master']['map_role_name'], self.config['Master']['output_dir'])))
        n_keys = len(keys)
        n = int(n_keys/self.__num_worker)

        for i in range(self.__num_worker):
            if i != self.__num_worker - 1:
                reducer_input = [key for key in keys[i*n:(i+1)*n]]
            else:
                reducer_input = [key for key in keys[i*n:]]
            reduce_input = ', '.join(reducer_input)
            self.__KVServer.put(id, self.config['Master']['input_dir'], reduce_input, self.config['Master']['reduce_role_name'], i, self.config['Master']['input_dir'])


    def __get_keys_intermidiate_data(self, id, role, mode):
        keys = []
        for i in range(self.__num_worker):
            keys += self.__KVServer.getKeys(id, role, i, mode)
        return keys


    def exposed_destroy_cluster(self, id):
        self.__KVServer.clear(id)
        
        # Terminate and delete all worker instances
        for index in range(self.__num_worker):
            self.__gcp_api.delete_instance(self.__project, self.__zone, self.config['Worker']['worker_name_prefix'] + str(index))
        self.__gcp_api.delete_snapshot(self.__project, self.__zone, self.config['Master']['master_snap'])

        # Update KVServer snapshot (by deleting old and create new)
        self.__gcp_api.delete_snapshot(self.__project, self.__zone, self.config['KVServer']['kvserver_snap'])
        self.__gcp_api.create_snapshot(self.__project, self.__zone, self.config['KVServer']['kvserver_name'], self.config['KVServer']['kvserver_snap'])
        
        # Terminate KVServer
        #self.__gcp_api.delete_instance(self.__project, self.__zone, 'kvserver')
        print('Cluster destroyed', id)



if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read('config.ini', encoding = 'utf8')
    port = int(config['Master']['master_port'])
    rpyc.core.protocol.DEFAULT_CONFIG['sync_request_timeout'] = None
    t = ThreadedServer(MasterService, port = port, protocol_config = rpyc.core.protocol.DEFAULT_CONFIG)
    try:
        t.start()
    except Exception:
        t.stop()
