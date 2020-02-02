import os, time
import googleapiclient.discovery
from google.oauth2 import service_account
import configparser

class GCP_APIs:
    
    def __init__(self):
        self.config = configparser.ConfigParser()
        self.config.read('config.ini', encoding = 'utf8')
        
        self.scopes = ['https://www.googleapis.com/auth/cloud-platform']
        self.sa_file = self.config['GCP']['cred_file_path']
        credentials = service_account.Credentials.from_service_account_file(self.sa_file, scopes=self.scopes)
        self.service = googleapiclient.discovery.build('compute', 'v1', credentials=credentials)
        
    def create_instance(self, project, zone, name, server_instance_name):
        snap_name = server_instance_name + '-snap'
        if name == self.config['KVServer']['kvserver_name']:
            snap_name = self.config['KVServer']['kvserver_snap']

        try:
            snap = self.service.snapshots().get(project = project, snapshot = snap_name).execute()
        except:
            snap = self.create_snapshot(project, zone, server_instance_name, snap_name)
        source_snap = snap['selfLink']
        
        machine_type = "zones/" + zone + "/machineTypes/" + self.config['GCP']['machine_type']
        if name == self.config['KVServer']['kvserver_name']:
            startup_script = open(os.path.join(os.path.dirname(__file__), self.config['KVServer']['kvserver_startup_script_path']), 'r').read()
        else:
            startup_script = open(os.path.join(os.path.dirname(__file__), self.config['Worker']['worker_startup_script_path']), 'r').read()
        
        config = {
            'name': name,
            'machineType': machine_type,
            'disks': [
                {
                    'boot': True,
                    'autoDelete': True,
                    'initializeParams': {
                        'sourceSnapshot': source_snap,
                    }
                }
            ],
            'networkInterfaces': [{
                'network': 'global/networks/default',
                'accessConfigs': [
                    {'type': 'ONE_TO_ONE_NAT', 'name': 'External NAT'}
                ]
            }],
            'serviceAccounts': [{
                'email': 'default',
                'scopes': [
                    'https://www.googleapis.com/auth/devstorage.read_write',
                    'https://www.googleapis.com/auth/logging.write'
                ]
            }],
            'metadata': {
                'items': [{
                    # Startup script is automatically executed by the instance upon startup.
                    'key': 'startup-script',
                    'value': startup_script
                }]
            }
        }
        
        while True:
            try:
                instance = self.service.instances().insert(project = project, zone = zone, body = config).execute()
                self.wait_for_operation(project, zone, instance['name'], 'start_vm')
                break
            except:
                continue
    

    def delete_instance(self, project, zone, name):
        credentials = service_account.Credentials.from_service_account_file(self.sa_file, scopes = self.scopes) 
        compute = googleapiclient.discovery.build('compute', 'v1', credentials = credentials)
        compute.instances().delete(project = project, zone = zone, instance = name).execute()
    
    def create_snapshot(self, project, zone, instance_name, snap_name):
        snapshot_body = {'name':snap_name}
        credentials = service_account.Credentials.from_service_account_file(self.sa_file, scopes = self.scopes)
        compute = googleapiclient.discovery.build('compute', 'v1', credentials = credentials)
        compute.disks().createSnapshot(project = project, zone = zone, disk = instance_name, body = snapshot_body).execute()
        while True:
            try:
                snap = compute.snapshots().get(project = project, snapshot = snap_name).execute()
                return snap
            except:
                continue
    

    def delete_snapshot(self, project, zone, disk_name):
        credentials = service_account.Credentials.from_service_account_file(self.sa_file, scopes = self.scopes)
        compute = googleapiclient.discovery.build('compute', 'v1', credentials = credentials)
        compute.snapshots().delete(project = project, snapshot = disk_name).execute()
    

    def wait_for_operation(self, project, zone, operation, action):
        print('Waiting for ' + action + ' operation to finish... ', end = '')
        while True:
            result = self.service.zoneOperations().get(
                project=project,
                zone=zone,
                operation=operation).execute()
    
            if result['status'] == 'DONE':
                print("DONE", end = '\n')
                if 'error' in result:
                    raise Exception(result['error'])
                return result
    
            time.sleep(1)
    

    def getIPAddresses(self, project_id, zone, name):
        instance = self.service.instances().get(project=project_id, zone=zone, instance=name).execute()
        external_ip = instance['networkInterfaces'][0]['accessConfigs'][0]['natIP']
        internal_ip = instance['networkInterfaces'][0]['networkIP']
        return internal_ip, external_ip
