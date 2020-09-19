## Spark auto cluster notebook
- Script to create a spark notebook cluster with user defined node requirements

## INSTALL && SETUP
- create a python 3 virtual env and install req from requirements.txt 
- RUN ```python init_cluster.py```

## SETUP WALK THROUGH
- Provide your DO token 
```Provide your token: <Your TOKEN>```

- Name your cluster -  provide a name without spaces

```Name the cluster: test50```
- Selecting region for vms. use blr1 as it's the closest

``` Select your region('ams2', 'ams3', 'blr1', 'fra1', 'lon1', 'nyc1', 'nyc2', 'nyc3', 'sfo1', 'sfo2', 'sgp1', 'tor1'): blr1 ```

- Choosing a node type for your master. You should choose option 11 or higher as smaller vms does not meet requirements for stable setup. Recommended option is 11

```Choose your option: 11```

- The script automatically creates the master node with correct configs. DO APIs are slow. so wait for instructions on screen

- Once master node is created. provide your size for worker node. Again recommended size is 11

```Choose your option: 11```

- Choosing number of workers you need

```Number of worker nodes: 2```

- Now, wait till the script completes. (Around 2 minutes). The script will print out your notebook ip, password and spark monitor ip

```
Notebook url: http://159.89.174.164:8888
Password: EVG7f.F]PE6E
Spark Monitor: http://159.89.174.164:8080

```

- visit notebook url and log in with the password. A simple template noteboook 'template.ipynb' will be available to get you started