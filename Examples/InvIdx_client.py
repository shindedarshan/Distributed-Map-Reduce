import rpyc
import marshal

def mapper_function(directory):
    rpyc.core.protocol.DEFAULT_CONFIG['sync_request_timeout'] = None
    kv_server = rpyc.connect('localhost', 9889, config=rpyc.core.protocol.DEFAULT_CONFIG).root
    try:
        punctuation = '!",.:;?`\\/'
        for file in directory:
            f = open(file, 'r')
            data = f.read()
            f.close()
            mapper_out = [kv_server.put(word.lower().translate(None, punctuation), 1)
                          for word in data.split()]
        return mapper_out
    except Exception as e:
        print(e)


def reducer_function(keys):
    rpyc.core.protocol.DEFAULT_CONFIG['sync_request_timeout'] = None
    kv_server = rpyc.connect('localhost', 9889, config=rpyc.core.protocol.DEFAULT_CONFIG).root
    reducer_out = []
    for key in keys:
        reducer_out.append((keys, kv_server.get(key)))
    return reducer_out


output_location = 'InvIdx_output.txt'
data = 'InvIdx'

map_func = marshal.dumps(mapper_function.__code__)
reduce_func = marshal.dumps(reducer_function.__code__)

rpyc.core.protocol.DEFAULT_CONFIG['sync_request_timeout'] = None
master = rpyc.connect('35.237.254.183', 3389, config = rpyc.core.protocol.DEFAULT_CONFIG)
cluster_id = master.root.init_cluster(2)
word_count = master.root.run_mapreduce(cluster_id, data, 'inv_index', 'inv_index', output_location)
master.root.destroy_cluster(cluster_id)