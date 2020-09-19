import requests
import json
import time

def get_available_sizes(selected_region):
    print("")
    response = requests.get('https://api.digitalocean.com/v2/sizes', headers=headers)
    temp = response.json()
    available_sizes = []
    for size in temp.get('sizes'):
        if selected_region in size["regions"]:
            available_sizes.append(size)
    if len(available_sizes) != 0:
        for idx,s in enumerate(available_sizes):
            print("{}. {} ---> Ram:{}, vCpus{}, disk:{}".format(idx, s["slug"], s["memory"], s["vcpus"], s["disk"]))

    selected_size = {}
    while 1:
        index = int(input("Choose your option: "))
        if index < len(available_sizes):
            selected_size = available_sizes[index]
            break
        else:
            print("Not a valid option")
    return selected_size
# USER_DATA = """#cloud-config\n\nruncmd:\n  - export PYTHONPATH=/home/devops/envs/pyspark-notebook-env/bin/python\n  - export PYSPARK_DRIVER_PYTHON="jupyter"\n  - export PYSPARK_DRIVER_PYTHON_OPTS="notebook --ip 0.0.0.0 --allow-root"\n  - export PYSPARK_PYTHON=/home/devops/envs/pyspark-notebook-env/bin/python\n  - export PATH=/home/devops/envs/pyspark-notebook-env/bin:$PATH\n  - export SPARK_MASTER_HOST=`ip route get 8.8.8.8 | awk '{print $NF; exit}'`\n  - export SPARK_LOCAL_IP=`ip route get 8.8.8.8 | awk '{print $NF; exit}'`\n  - /home/devops/spark-2.3.1-bin-hadoop2.7/sbin/start-master.sh\n  - /home/devops/spark-2.3.1-bin-hadoop2.7/bin/pyspark --master spark://$SPARK_MASTER_HOST:7077 --jars /home/devops/transformer/spark-cassandra-connector_2.11-2.3.1.jar,/home/devops/transformer/jsr166e-1.1.0.jar,/home/devops/transformer/mongo-scala-driver_2.11-2.4.2-alldep.jar,/home/devops/transformer/aws-java-sdk-1.7.4.jar,/home/devops/transformer/hadoop-aws-2.7.6.jar --executor-memory 5G --total-executor-cores 8"""
USER_DATA = """#cloud-config\n\nruncmd:\n  - export PYTHONPATH=/home/devops/envs/pyspark-notebook-env/bin/python\n  - export PYSPARK_DRIVER_PYTHON="jupyter"\n  - export PYSPARK_DRIVER_PYTHON_OPTS="notebook --ip 0.0.0.0 --allow-root"\n  - export PYSPARK_PYTHON=/home/devops/envs/pyspark-notebook-env/bin/python\n  - export PATH=/home/devops/envs/pyspark-notebook-env/bin:$PATH\n  - export SPARK_MASTER_HOST=`ip route get 8.8.8.8 | awk '{print $NF; exit}'`\n  - export SPARK_LOCAL_IP=`ip route get 8.8.8.8 | awk '{print $NF; exit}'`\n  - /home/devops/spark-2.3.1-bin-hadoop2.7/sbin/start-master.sh\n  - cd /home/devops/notebooks\n  - /home/devops/spark-2.3.1-bin-hadoop2.7/bin/pyspark --master spark://$SPARK_MASTER_HOST:7077 --jars /home/devops/transformer/spark-cassandra-connector_2.11-2.3.1.jar,/home/devops/transformer/jsr166e-1.1.0.jar,/home/devops/transformer/mongo-scala-driver_2.11-2.4.2-alldep.jar,/home/devops/transformer/aws-java-sdk-1.7.4.jar,/home/devops/transformer/hadoop-aws-2.7.6.jar --executor-memory 5G --total-executor-cores 8"""

def create_nodes(names, selected_region, selected_size, cluster_name, mode):
    
    node_data = {
        'region': selected_region,
        'size': selected_size['slug'],
        'image': 43808292,
        'ssh_keys': [23733290],
        'backups': False,
        'ipv6': True,
        'user_data': USER_DATA,
        'private_networking':None,
        'volumes': None,
        'tags':[cluster_name]
    }
    if mode == "single":
        node_data['name'] = names
    else:
        node_data['names'] = names
        node_data['image'] =  45007531
        node_data['user_data'] = USER_DATA_WORKER
    res = requests.post('https://api.digitalocean.com/v2/droplets', headers=headers, data=json.dumps(node_data))
    if res.status_code == 202:
        return res.json()
    else:
        print("Creating node failed. Exiting .....")
        print(res.json())
        exit



headers = {
    'Content-Type': 'application/json'
}

# Known Variables

regions = ['ams2', 'ams3', 'blr1', 'fra1', 'lon1', 'nyc1', 'nyc2', 'nyc3', 'sfo1', 'sfo2', 'sgp1', 'tor1']


bearer = input('Provide your token: ')
headers['Authorization'] = "Bearer {}".format(bearer)

cluster_name = input("Name the cluster: ")
while 1:
    selected_region= input("Select your region('ams2', 'ams3', 'blr1', 'fra1', 'lon1', 'nyc1', 'nyc2', 'nyc3', 'sfo1', 'sfo2', 'sgp1', 'tor1'): ")
    if selected_region not in regions:
        print("Selected region not valid")
    else:
        break
print("")
print("Getting available node types for master node ....")
selected_size_master = get_available_sizes(selected_region)
print("")
print("Creating master node. This might take a while ....")
res_details_master = create_nodes('{}-master'.format(cluster_name), selected_region, selected_size_master, cluster_name, "single")
print("Master node successfully created. id: {}.Fetching details ....".format(res_details_master['droplet']['id']))
print("Waiting for DO to assign ip address ....[approx 30s]")
time.sleep(30)
res_details_wait = requests.get("https://api.digitalocean.com/v2/droplets/{}".format(res_details_master['droplet']['id']), headers=headers)
master_details = {
    'ip': res_details_wait.json()['droplet']['networks']['v4'][0]['ip_address'],
    'id': res_details_master['droplet']['id']
}
print("Master IP: ", res_details_wait.json()['droplet']['networks']['v4'][0]['ip_address'])
print("")
print("****************Master node creation successful************")
print("")


print("Configuring firewalls for your cluster. Hold on ....")
print("""\nRoses are red\nViolets are blue\nDigital Ocean APIs are slow\n\nThis will take about 60s...""")

time.sleep(60)
fw_name = 'fw-{}'.format(cluster_name)

firewall_data = {
    "name": fw_name,
    "inbound_rules":[
        {
            "protocol":"tcp",
            "ports":"all",
            "sources":{
                "tags":[cluster_name]
            }
        },
        {
            "protocol":"tcp",
            "ports":"22",
            "sources":{
                "addresses":["0.0.0.0/0","::/0"]
            }
        },
        {
            "protocol":"tcp",
            "ports":"8080",
            "sources":{
                "addresses":["0.0.0.0/0","::/0"]
            }
        },
        {
            "protocol":"tcp",
            "ports":"8888",
            "sources":{
                "addresses":["0.0.0.0/0","::/0"]
            }
        },
        {
            "protocol":"tcp",
            "ports":"4040-4050",
            "sources":{
                "addresses":["0.0.0.0/0","::/0"]
            }
        }
    ],
     "outbound_rules":[
        {
            "protocol": "tcp",
            "ports": "all",
            "destinations": {
                "addresses": [
                "0.0.0.0/0",
                "::/0"
            ]
            }
        },
        {
            "protocol": "udp",
            "ports": "all",
            "destinations": {
                "addresses": [
                "0.0.0.0/0",
                "::/0"
            ]
            }
        }

    ],
    "tags":[cluster_name]
}

# print("this is the firewall body", json.dumps(firewall_data))

response_firewall = requests.post('https://api.digitalocean.com/v2/firewalls', headers=headers, data=json.dumps(firewall_data))

# print(response_firewall.json())

USER_DATA_WORKER = """#cloud-config\n\nruncmd:\n  - export PATH=/home/devops/envs/pyspark-notebook-env/bin:$PATH\n  - export SPARK_LOCAL_IP=`hostname -I | awk '{print $1;}'`\n  - /home/devops/spark-2.3.1-bin-hadoop2.7/sbin/start-slave.sh  spark://%s:7077""" % res_details_wait.json()['droplet']['networks']['v4'][0]['ip_address']
# selected_region = 'blr1'
# USER_DATA_WORKER = """#cloud-config\n\nruncmd:\n  - export PATH=/home/devops/envs/pyspark-notebook-env/bin:$PATH\n  - export SPARK_LOCAL_IP=`hostname -I | awk '{print $1;}'`\n  - /home/devops/spark-2.3.1-bin-hadoop2.7/sbin/start-slave.sh  spark://159.65.159.161:7077"""

print("******** Setup workers ***********")
print("")
selected_size_worker = get_available_sizes(selected_region)
no_workers_nodes = int(input("Number of worker nodes: "))

node_name_array = []
for x in range(no_workers_nodes):
  node_name_array.append('{}-worker-{}'.format(cluster_name, x+1))
print("")
print("Creating worker nodes. This might take a while ....")
res_details_workers = create_nodes(node_name_array, selected_region, selected_size_worker, cluster_name, "multi")


print("Worker nodes successfully created..Fetching details ....")
print("Waiting for DO to assign ip address ....[approx 30s]")
time.sleep(30)
worker_array = []
for idx in range(len(node_name_array)):
    res_details_wait_worker = requests.get("https://api.digitalocean.com/v2/droplets/{}".format(res_details_workers['droplets'][idx]['id']), headers=headers)
    worker_details = {
    'ip': res_details_wait_worker.json()['droplet']['networks']['v4'][0]['ip_address'],
    'id': res_details_workers['droplets'][idx]['id'],
    'name': res_details_workers['droplets'][idx]['name']
    }
    worker_array.append(worker_details)
print("")
print("****************Worker node creation successful************")
print("These are your nodes:")
print(worker_array)

print("""******************SETUP COMPLETE. PLEASE GIVE 2 MINUTES FOR WORKERS TO JOIN THE CLUSTER AND DO HEALTHCHECKS************************\n""")
print("Preping your notebook... About 120s left...")
time.sleep(120)
print("""\n\nNotebook url: http://{}:8888\nPassword: EVG7f.F]PE6E\nSpark Monitor: http://{}:8080""".format(master_details['ip'], master_details['ip']))

print("Adios. Exiting in 10 sec ....")
time.sleep(10)
exit






