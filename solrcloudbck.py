#!/usr/bin/env python

from kazoo.client import KazooClient
from urllib2 import *
import logging
import json

logging.basicConfig()

zk = KazooClient(hosts='10.200.0.143:2181', read_only=True)
zk.start()

data, stat = zk.get("/clusterstate.json")
json_obj = json.loads(data.decode("utf-8"))

for collection in json_obj:
    for shard in json_obj[collection]['shards']:
        shard_obj = json_obj[collection]['shards'][shard]
        for core in shard_obj['replicas']:
            core_obj = shard_obj['replicas'][core]
            if core_obj['state'] == 'active' and 'leader' in core_obj and core_obj['leader']:
                node_name = core_obj['node_name'].split(":")[0]
                core_name = core_obj['core']

                url_string = 'http://' + node_name + ':9080/solr/admin/cores?action=STATUS&wt=json&core=' + core_name
                core_status_obj = json.load(urlopen(url_string))

                dir_name = core_status_obj['status'][core_name]['instanceDir']

                print collection, shard, shard_obj['range'], core_name, node_name, dir_name
