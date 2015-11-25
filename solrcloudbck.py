#!/usr/bin/env python

from kazoo.client import KazooClient
from urllib2 import *
import os
import logging
import json
import argparse
import time

class LeaderCore(object):

    def __init__(self, collection, shard, shard_range, core_name, node_name, node_port, core_dir):
        self.collection = collection
        self.shard = shard
        self.shard_range = shard_range
        self.core_name = core_name
        self.node_name = node_name
        self.node_port = node_port
        self.core_dir = core_dir

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return (other.collection == self.collection) and (other.shard_range == self.shard_range)
        else:
            return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def __str__(self):
        s1 = str(self.collection) + '\n' + str(self.shard) + '\n' + str(self.shard_range) + '\n'
        s2 = str(self.core_name) + '\n' + str(self.node_name) + '\n' + str(self.node_port) + '\n' + str(self.core_dir)
        s = s1 + s2 + '\n'
        return s


class CoreListBuilder:

    solr_api_preffix = '/solr/admin/cores?action=STATUS&wt=json&core='
    zk_server_port = '127.0.0.1:2181'
    backup_dir = '/tmp'
    leader_core_list_server = []
    leader_core_list_filesystem = []
    clusterstate_json = {}

    def __init__(self, zk_server_port, backup_dir):
        self.zk_server_port = zk_server_port
        self.backup_dir = backup_dir

    def get_clusterstate_json(self):
        logging.basicConfig()
        zk = KazooClient(hosts=self.zk_server_port, read_only=True)
        zk.start()

        data, stat = zk.get('/clusterstate.json')
        self.clusterstate_json = json.loads(data.decode('utf-8'))

        zk.stop()
        zk.close()

    def get_cores_definitions_server(self):

        self.get_clusterstate_json()

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

                        self.leader_core_list_server.append(leader_core_obj)

        return self.leader_core_list_server

    def get_cores_definitions_filesystem(self):

        latest_backup_dir = self.backup_dir + '/' + str(max(map(int, os.listdir(self.backup_dir))))

        for collection in os.listdir(latest_backup_dir):
            for shard_range in os.listdir(latest_backup_dir + '/' + collection):

                leader_core_obj = LeaderCore(
                    collection=collection,
                    shard='shard_fs',
                    shard_range=shard_range,
                    core_name='core_name_fs',
                    node_name='node_name_fs',
                    node_port='node_port_fs',
                    core_dir='core_dir_fs'
                )

                self.leader_core_list_filesystem.append(leader_core_obj)

        return self.leader_core_list_filesystem

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('zk_server_port', help='zookeeper server and port, default is 127.0.0.1:2181')
    parser.add_argument('backup_dir', help='directory to store backup, default is /tmp')

    args = parser.parse_args()

    clb = CoreListBuilder(zk_server_port=args.zk_server_port, backup_dir=args.backup_dir)
    s_cores = clb.get_cores_definitions_server()
    f_cores = clb.get_cores_definitions_filesystem()

    for s_core in s_cores:
        for f_core in f_cores:
            if s_core == f_core:
                print 'Equal', s_core.shard_range,  f_core.shard_range

    bck_dir = args.backup_dir

    ts = str(int(time.time()))
    exclude_list = '--exclude "tlog/" --exclude "data/replication.properties" --exclude "core.properties" --exclude "data/index/write.lock" '

    for core in s_cores:
        bck_path = bck_dir + '/' + ts + '/' + core.collection + '/' + core.shard_range + '/'

        mkdir_cmd = 'mkdir -p ' + bck_path

        p = os.popen(mkdir_cmd, "r")
        while 1:
            line = p.readline()
            if not line:
                break
            print line

        rsync_cmd = 'rsync -arvv --delete-before ' + exclude_list + core.node_name + ':' + core.core_dir + ' ' + bck_path
        #rsync_cmd = "exit 0"

        for i in range(1, 5):  # dirty loop
            print '-***-'
            print core.node_name, core.core_dir, 'iteration', i
            print '-***-'
            p = os.popen(rsync_cmd, "r")
            while 1:
                line = p.readline()
                if not line:
                    break
                print line
