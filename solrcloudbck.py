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


class SolrCloudBackup:

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

        for collection_fs in os.listdir(latest_backup_dir):
            for shard_range_fs in os.listdir(latest_backup_dir + '/' + collection_fs):

                core_dir_fs = latest_backup_dir + '/' + collection_fs + '/' + shard_range_fs + '/'

                leader_core_obj = LeaderCore(
                    collection=collection_fs,
                    shard='empty_field',
                    shard_range=shard_range_fs,
                    core_name='empty_field',
                    node_name='empty_field',
                    node_port='empty_field',
                    core_dir=core_dir_fs
                )

                self.leader_core_list_filesystem.append(leader_core_obj)

        return self.leader_core_list_filesystem

    def compare_cores_structures(self, f_cores_list, s_cores_list):

        eq_cores = 0
        structures_is_equal = False

        f_len = len(f_cores_list)
        s_len = len(s_cores_list)

        for f_core in f_cores_list:
            if f_core in s_cores_list:
                eq_cores += 1

        if f_len != 0 and s_len != 0 and f_len == s_len and eq_cores == s_len:
            structures_is_equal = True
        else:
            print 'ERROR: Different cores number in backup and on server OR different sharding schema.'

        return structures_is_equal

    def backup_from_server_to_filesystem(self):

        s_cores_list = self.get_cores_definitions_server()

        ts = str(int(time.time()))
        exclude_list = '--exclude "tlog/" --exclude "data/replication.properties" --exclude "core.properties" --exclude "data/index/write.lock" '
        options_list = '-arvv --delete-before '

        for core in s_cores_list:
            bck_path = self.backup_dir + '/' + ts + '/' + core.collection + '/' + core.shard_range + '/'

            mkdir_cmd = 'mkdir -p ' + bck_path

            p = os.popen(mkdir_cmd, "r")
            while 1:
                line = p.readline()
                if not line:
                    break
                print line

            rsync_cmd = 'rsync ' + options_list + exclude_list + core.node_name + ':' + core.core_dir + ' ' + bck_path

            for i in range(1, 5):  # dirty loop, attempt to be consistent
                print '-***-'
                print core.node_name, core.core_dir, 'iteration', i
                print '-***-'
                p = os.popen(rsync_cmd, "r")
                while 1:
                    line = p.readline()
                    if not line:
                        break
                    print line

        return True

    def restore_from_filesystem_to_server(self):

        options_list = '-arvv --delete '

        f_cores_list = self.get_cores_definitions_filesystem()
        s_cores_list = self.get_cores_definitions_server()

        restore_possible = self.compare_cores_structures(f_cores_list=f_cores_list, s_cores_list=s_cores_list)

        if restore_possible:
            print 'Doing restore...'

            for s_core in s_cores_list:
                for f_core in f_cores_list:
                    if s_core == f_core:
                        print 'Restoring  ' + f_core.collection, f_core.shard_range, f_core.core_dir, s_core.collection, s_core.shard_range, s_core.core_dir
                        rsync_cmd = 'rsync ' + options_list + '' + f_core.core_dir + 'data/' + '  ' + s_core.node_name + ':' + s_core.core_dir + 'data/'
                        for i in range(1, 5):  # dirty loop, attempt to be consistent
                            print '-***-'
                            print f_core.collection, f_core.shard_range, 'iteration', i
                            print '-***-'
                            p = os.popen(rsync_cmd, "r")
                            while 1:
                                line = p.readline()
                                if not line:
                                    break
                                print line
        else:
            print 'Impossible to restore backup!'
            return False


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('zk_server_port', help='zookeeper server and port, default is 127.0.0.1:2181')
    parser.add_argument('backup_dir', help='directory to store backup, default is /tmp')
    parser.add_argument('action', help='"backup" OR "restore"')

    args = parser.parse_args()

    clb = SolrCloudBackup(zk_server_port=args.zk_server_port, backup_dir=args.backup_dir)

    if args.action == 'backup':
        r = clb.backup_from_server_to_filesystem()

    if args.action == 'restore':
        r = clb.restore_from_filesystem_to_server()
