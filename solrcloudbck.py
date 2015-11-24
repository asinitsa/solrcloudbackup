#!/usr/bin/env python

from kazoo.client import KazooClient
from urllib2 import *
import os
import logging
import json
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('zookkeper_server_port', help='zookeeper server and port: 10.0.0.1:2181')
parser.add_argument('backup_dir', help='/path/to/backup')

args = parser.parse_args()

zk_server_port = args.zookkeper_server_port
bck_dir = args.backup_dir

#sys.exit(0)

logging.basicConfig()

zk = KazooClient(hosts=zk_server_port, read_only=True)
zk.start()

data, stat = zk.get('/clusterstate.json')
json_obj = json.loads(data.decode('utf-8'))

zk.stop()

api_preffix = '/solr/admin/cores?action=STATUS&wt=json&core='

for collection in json_obj:
    for shard in json_obj[collection]['shards']:
        shard_obj = json_obj[collection]['shards'][shard]
        for core in shard_obj['replicas']:
            core_obj = shard_obj['replicas'][core]
            if core_obj['state'] == 'active' and 'leader' in core_obj and core_obj['leader']:  # get leader core
                node_name = core_obj['node_name'].split(":")[0]
                node_port = core_obj['node_name'].split(":")[1].split("_")[0]
                core_name = core_obj['core']
                shard_range = shard_obj['range']

                url_string = 'http://' + node_name + ':' + node_port + api_preffix + core_name
                core_status_obj = json.load(urlopen(url_string))

                core_dir = core_status_obj['status'][core_name]['instanceDir']

                bck_path = bck_dir + '/' + collection + '/' + shard + '/' + shard_range + '/'

                rsync_cmd = 'mkdir -p ' + bck_path + ' && rsync -avr --exclude "tlog/" ' + node_name + ':' + core_dir + ' ' + bck_path

                print collection, shard, shard_range, core_name, node_name, core_dir, rsync_cmd

                p = os.popen(rsync_cmd, "r")
                while 1:
                    line = p.readline()
                    if not line:
                        break
                    print line
