import json
import sys
import zlib
from datetime import timedelta

from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.options import (ClusterOptions, ClusterTimeoutOptions,
                               QueryOptions)

import mc_bin_client
import memcacheConstants

# Update this to your cluster:
bucket_name = "default"
username = "Administrator"
password = "asdasd"
kv_node_host = "localhost"
# Production KV port is 11210 or 11207 (SSL)
kv_node_port = 12000
kv_node_ssl = False
# User Input ends here.

NUM_VBUCKETS = 1024

kv_nodes = []
vb_map = {}

def disconnect():
    for client in kv_nodes:
        client.close()

def connect_client(host, port):
    print('connect_client', host, port)
    client = mc_bin_client.MemcachedClient(host, port, use_ssl=kv_node_ssl)
    client.req_features = {memcacheConstants.FEATURE_SELECT_BUCKET,
                           memcacheConstants.FEATURE_JSON}
    client.hello('restore-null-prefix-keys')
    client.sasl_auth_plain(username, password)
    client.bucket_select(bucket_name)
    return client

def connect_cluster():
    client = connect_client(kv_node_host, kv_node_port)
    cluster_config = client.get_cluster_config()
    client.close()
    # print(json.dumps(cluster_config, indent=2))
    for server in cluster_config['vBucketServerMap']['serverList']:
        host = server.split(':')
        port = int(host[1])
        host = host[0]
        if host == '$HOST':
            host = kv_node_host
        if kv_node_ssl:
            port = kv_node_port
        client = connect_client(host, port)
        kv_nodes.append(client)
    for vbid, servers in enumerate(cluster_config['vBucketServerMap']['vBucketMap']):
        vb_map[vbid] = kv_nodes[servers[0]]

def get_vbid(doc_id):
    if isinstance(doc_id, str):
        doc_id = doc_id.encode()
    return (((zlib.crc32(doc_id)) >> 16) & 0x7fff) % NUM_VBUCKETS

def get_doc(id):
    docs = []
    for vbid in [get_vbid(id[1:]), get_vbid(id)]:
        client: mc_bin_client.MemcachedClient = vb_map[vbid]
        try:
            client.vbucketId = vbid
            flags, cas, doc = client.get(id)
            docs.append((doc, cas, flags, vbid))
        except mc_bin_client.ErrorKeyEnoent:
            pass
    return docs

def add_doc(id, value, flags, vbid=None):
    if vbid is None:
        vbid = get_vbid(id)
    client: mc_bin_client.MemcachedClient = vb_map[vbid]
    client.vbucketId = vbid
    client.add_with_dtype(id, 0, flags, value, 1)

def delete_doc(id, cas, vbid=None):
    if vbid is None:
        vbid = get_vbid(id)
    client: mc_bin_client.MemcachedClient = vb_map[vbid]
    client.vbucketId = vbid
    client.delete(id, cas)

def get_doc_ids():
    doc_ids = []
    kv_node = f'{kv_node_host}:{kv_node_port}'
    auth = PasswordAuthenticator(username, password)
    cluster = Cluster(('couchbases://' if kv_node_ssl else 'couchbase://') + kv_node, ClusterOptions(auth))
    cluster.wait_until_ready(timedelta(seconds=5))
    query_result = cluster.query(
        'select meta().id from `' + bucket_name + '` where meta().id like "\\u0000%"')
    for row in query_result:
        id = row['id']
        if len(id) < 2:
            print('Expecting doc id to have length at least 2 bytes')
            continue
        if id[0] != '\0':
            print("Prefix not null!!!", json.dumps(id))
            continue
        doc_ids.append(id)
    cluster.close()
    return doc_ids

def main():
    global bucket_name, username, password, kv_node_host, kv_node_port, kv_node_ssl
    for arg in sys.argv[1:]:
        if not arg.startswith('-C'):
            continue
        parts = arg[2:].split(':')
        bucket_name = parts[0]
        username = parts[1]
        password = parts[2]
        kv_node_host = parts[3]
        kv_node_port = int(parts[4])
        kv_node_ssl = len(parts) == 6 and parts[5] == 'ssl'
    doc_ids = get_doc_ids()
    print(f'Found {len(doc_ids)} null-prefixed doc ids\n')
    connect_cluster()
    print()
    for arg in sys.argv[1:]:
        if not arg.startswith('-A'):
            continue
        id = '\0' + arg[2:]
        add_doc(id, '{}', 0, get_vbid(id[1:]))
        print('Added test doc', json.dumps(id))
        disconnect()
        return
    do_restore = '--restore' in sys.argv[1:]
    do_delete = '--delete' in sys.argv[1:]
    not_found_count = 0
    already_exist_count = 0
    added_count = 0
    deleted_count = 0
    for id in doc_ids:
        escaped_id = json.dumps(id)
        docs = get_doc(id)
        if len(docs) == 0:
            print('Not found', escaped_id)
            not_found_count += 1
            continue
        for (doc, cas, flags, vbid) in docs:
            print('Got', escaped_id, 'cas:', cas, 'flags:', flags, 'vb:', vbid)
            if do_restore:
                try:
                    add_doc(id[1:], doc, flags)
                    print('Added', json.dumps(id[1:]))
                    added_count += 1
                except mc_bin_client.ErrorKeyEexists:
                    print('Already exists', json.dumps(id[1:]))
                    already_exist_count += 1
            if do_delete:
                delete_doc(id, cas, vbid)
                print('Deleted', escaped_id, 'vb:', vbid)
                deleted_count += 1
    print('\n------------------------------------------')
    print('Not found', not_found_count)
    print('Already exist', already_exist_count)
    print('Added', added_count)
    print('Deleted', deleted_count)
    disconnect()

if __name__ == '__main__':
    main()
