import rpyc
from rpyc.utils.server import ThreadedServer
import marshal, types
import time
import configparser

class WorkerService(rpyc.Service):
    def __init__(self):
        self.config = configparser.ConfigParser()
        self.config.read('config.ini', encoding = 'utf8')
        
    def on_connect(self, conn):
        pass

    def on_disconnect(self, conn):
        pass

    def exposed_execute(self, id, func = None, role = None, index = None, 
                        kv_server_address = None, kv_server_port = None):
        rpyc.core.protocol.DEFAULT_CONFIG['sync_request_timeout'] = None
        kv_server = rpyc.connect(kv_server_address, kv_server_port, 
                                 config = rpyc.core.protocol.DEFAULT_CONFIG).root
        
        #code = marshal.loads(func)
        #worker_func = types.FunctionType(code, globals(), 'worker function')
        
        if role == self.config['Master']['map_role_name']:
            data = kv_server.get(id, self.config['Master']['input_dir'], role, index, self.config['Master']['input_dir'])
        elif role == self.config['Master']['reduce_role_name']:
            data = []
            keys = kv_server.get(id, self.config['Master']['input_dir'], role, index, self.config['Master']['input_dir'])
            keys = keys.split(', ')
            for key in keys:
                data.append((key, kv_server.get(id, key.split('.')[0], self.config['Master']['combine_role_name'], -1, self.config['Master']['output_dir'])))

        if func == self.config['Worker']['wc_operation']:
            if role == self.config['Master']['map_role_name']:
                res = word_count_map(data)
            elif role == self.config['Master']['reduce_role_name']:
                res = word_count_reduce(data)
        elif func == 'inv_index':
            if role == 'mapper':
                res = inverse_index_map(data)
            elif role == 'reducer':
                res = inverse_index_reduce(data)
        
        for int_res in res:
            kv_server.put(id, int_res[0].split('.')[0], int_res[1], role, index, 'output') 
        

def word_count_map(data):
    try:
        punctuations = '!",.:;?`\/' + str('\'')
        mapper_out = [(word.lower().translate(str.maketrans('','',punctuations)), 1) for word in data.split()]
        return mapper_out
    except Exception as e:
        print(e)

def word_count_reduce(data):
    reducer_out = []
    for word in data:
        occ = word[1]
        occ = len(occ.split(', '))
        reducer_out.append((word[0], occ))
    return reducer_out

def inverse_index_map(directory):
    try:
        punctuation = '!",.:;?`\\/'
        for file in directory:
            f = open(file, 'r')
            data = f.read()
            f.close()
            mapper_out = [(word.lower().translate(None, punctuation), 1) for word in data.split()]
        return mapper_out
    except Exception as e:
        print(e)

def inverse_index_reduce(keys):
    reducer_out = []
    for key in keys:
        word, file = key[0], key[1]
        reducer_out.append((word, file))
    return reducer_out

def main(port = 3389):
    time.sleep(10)
    rpyc.core.protocol.DEFAULT_CONFIG['sync_request_timeout'] = None
    t = ThreadedServer(WorkerService, port = port, 
                       protocol_config = rpyc.core.protocol.DEFAULT_CONFIG)
    try:
        print('worker started on port: ', port)
        t.start()
    except Exception:
        t.stop()

if __name__ == '__main__':
    main()
