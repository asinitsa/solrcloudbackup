#!/usr/bin/env python

from kazoo.client import KazooClient
import subprocess
import sys
import logging
import json

logging.basicConfig()

zk = KazooClient(hosts='10.200.0.143:2181', read_only=True)
zk.start()

data, stat = zk.get("/clusterstate.json")
json_obj = json.loads(data.decode("utf-8"))

HOST = "www.example.org"
COMMAND = "ps -ef | grep solr"

for collection in json_obj:
    for shard in json_obj[collection]['shards']:
        shard_obj = json_obj[collection]['shards'][shard]
        for core in shard_obj['replicas']:
            core_obj = shard_obj['replicas'][core]
            if core_obj['state'] == 'active' and 'leader' in core_obj and core_obj['leader']:

                print collection, shard, shard_obj['range'], core_obj['core'], core_obj['node_name'].split(":")[0]

                HOST = core_obj['node_name'].split(":")[0]
                COMMAND = "ps -ef | grep solr"
                ssh = subprocess.Popen(["ssh", "%s" % HOST, COMMAND],
                                       shell=False,
                                       stdout=subprocess.PIPE,
                                       stderr=subprocess.PIPE)
                result = ssh.stdout.readlines()
                if not result:
                    error = ssh.stderr.readlines()
                    print >>sys.stderr, "ERROR: %s" % error
                else:
                    result_list = result[2].split()
                    for k in result_list:
                        k_l = k.split("=")
                        if len(k_l) > 1 and k_l[0] == "-Dsolr.solr.home":
                            solr_home = k_l[1]



