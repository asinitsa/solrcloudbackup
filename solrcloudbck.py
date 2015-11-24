#!/usr/bin/env python

from kazoo.client import KazooClient
from urllib2 import *
import os
import logging
import json
import argparse

class LeaderCore(object):

    def __init__(self, collection, shard, shard_range, core_name, node_name, node_port, core_dir):
        self.collection = collection
        self.shard = shard
        self.shard_range = shard_range
        self.core_name = core_name
        self.node_name = node_name
        self.node_port = node_port
        self.core_dir = core_dir


class SolrCloudBackup:

    solr_api_preffix = '/solr/admin/cores?action=STATUS&wt=json&core='
    zk_server_port = '127.0.0.1:2181'
    bck_dir = '/tmp'
    leader_core_list = []
    clusterstate_json = {}

    def __init__(self, zk_server_port, bck_dir):
        self.zk_server_port = zk_server_port
        self.bck_dir = bck_dir

    def get_clusterstate_json(self):
        logging.basicConfig()
        zk = KazooClient(hosts=self.zk_server_port, read_only=True)
        zk.start()

        data, stat = zk.get('/clusterstate.json')
        self.clusterstate_json = json.loads(data.decode('utf-8'))

        zk.stop()
        zk.close()

    def build_cores_definitions(self):
        for collection in self.clusterstate_json:
            for shard in self.clusterstate_json[collection]['shards']:
                shard_obj = self.clusterstate_json[collection]['shards'][shard]
                for core in shard_obj['replicas']:
                    core_obj = shard_obj['replicas'][core]
                    if core_obj['state'] == 'active' and 'leader' in core_obj and core_obj['leader']:  # get leader core
                        node_name = core_obj['node_name'].split(":")[0]
                        node_port = core_obj['node_name'].split(":")[1].split("_")[0]
                        core_name = core_obj['core']
                        shard_range = shard_obj['range']

                        url_string = 'http://' + node_name + ':' + node_port + self.solr_api_preffix + core_name

                        core_status_obj = json.load(urlopen(url_string))
                        core_dir = core_status_obj['status'][core_name]['instanceDir']

                        leader_core_obj = LeaderCore(
                            collection=collection,
                            shard=shard,
                            shard_range=shard_range,
                            core_name=core_name,
                            node_name=node_name,
                            node_port=node_port,
                            core_dir=core_dir
                        )

                        self.leader_core_list.append(leader_core_obj)

    def backup_solr_cores(self):
        for core in self.leader_core_list:
            bck_path = self.bck_dir + '/' + core.collection + '/' + core.shard + '/' + core.shard_range + '/'
            rsync_cmd = 'rsync -avr --exclude "tlog/" ' + core.node_name + ':' + core.core_dir + ' ' + bck_path

            p = os.popen(rsync_cmd, "r")
            while 1:
                line = p.readline()
                if not line:
                    break
                print line

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('zk_server_port', help='zookeeper server and port, default is 127.0.0.1:2181')
    parser.add_argument('backup_dir', help='directory to store backup, default is /tmp')

    args = parser.parse_args()

    sb = SolrCloudBackup(zk_server_port=args.zk_server_port, bck_dir=args.backup_dir)

    sb.get_clusterstate_json()

    sb.build_cores_definitions()

    sb.backup_solr_cores()
