import array
import json
from argparse import ArgumentParser, ArgumentTypeError
from datetime import timedelta
from zlib import crc32

from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.options import ClusterOptions, TLSVerifyMode

import mc_bin_client
import memcacheConstants

bucket_name = "default"
username = "Administrator"
password = "password"
kv_node_host = "localhost"
kv_node_port = 11210
kv_node_ssl = False
collection_id = 0
search_all_vbs = False

kv_nodes = []
vb_map = {}

def disconnect():
    for client in kv_nodes:
        client.close()

def connect_client(host, port):
    print('connect_client', host, port)
    client = mc_bin_client.MemcachedClient(host, port, use_ssl=kv_node_ssl)
    client.req_features = {memcacheConstants.FEATURE_SELECT_BUCKET,
                           memcacheConstants.FEATURE_JSON,
                           memcacheConstants.FEATURE_XATTR,
                           memcacheConstants.FEATURE_COLLECTIONS}
    client.hello('manage-cid-prefix-keys')
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
    assert len(vb_map) in [1024, 128, 64]

def get_vbid(doc_id):
    if isinstance(doc_id, str):
        doc_id = doc_id.encode()
    return ((crc32(doc_id) >> 16) & 0x7fff) % len(vb_map)

def encode_key(doc_id, cid=0):
    prefix = array.array('B', [])
    while True:
        byte = cid & 0x7f
        cid >>= 7
        if cid > 0:
            prefix.append(byte | 0x80)
        else:
            prefix.append(byte)
            break
    if isinstance(doc_id, str):
        doc_id = doc_id.encode()
    return prefix.tobytes() + doc_id

def get_doc(id: str):
    docs = []
    prefix = encode_key('', collection_id).decode(errors='ignore')
    vbs = range(len(vb_map)) if search_all_vbs else [get_vbid(id), get_vbid(id.removeprefix(prefix))]
    for vbid in vbs:
        client: mc_bin_client.MemcachedClient = vb_map[vbid]
        try:
            client.vbucketId = vbid
            flags, cas, doc = client.get(encode_key(id))
            docs.append((doc, cas, flags, vbid))
        except mc_bin_client.ErrorKeyEnoent:
            pass
    return docs

def add_doc(id, cid, value, flags, vbid=None):
    if vbid is None:
        vbid = get_vbid(id)
    client: mc_bin_client.MemcachedClient = vb_map[vbid]
    client.vbucketId = vbid
    client.add_with_dtype(encode_key(id, cid), 0, flags, value, 1)

def delete_doc(id, cas, vbid=None):
    if vbid is None:
        vbid = get_vbid(id)
    client: mc_bin_client.MemcachedClient = vb_map[vbid]
    client.vbucketId = vbid
    client.delete(encode_key(id), cas)

def get_xattrs(id, vbid):
    client: mc_bin_client.MemcachedClient = vb_map[vbid]
    client.vbucketId = vbid
    key = encode_key(id)
    xkeys = client.subdoc_get(key, '$XTOC', 4)
    xattrs = {}
    for xkey in xkeys:
        xattrs[xkey] = client.subdoc_get(key, xkey, 4)
    return xattrs

def get_doc_ids():
    kv_node = f'{kv_node_host}:{kv_node_port}'
    options = ClusterOptions(PasswordAuthenticator(username, password), tls_verify=TLSVerifyMode.NONE)
    options.apply_profile('wan_development')
    cluster = Cluster(('couchbases://' if kv_node_ssl else 'couchbase://') + kv_node, options)
    cluster.wait_until_ready(timedelta(seconds=5))
    prefix = json.dumps(encode_key('%', collection_id).decode(errors='ignore'))
    query_result = cluster.query(f'select meta().id from `{bucket_name}` where meta().id like {prefix}')
    doc_ids = [row['id'] for row in query_result]
    cluster.close()
    return doc_ids

def check_port(s):
    v = int(s)
    if v <= 0 or v >= 0x10000:
        raise ArgumentTypeError(f'{v} is not a valid port number')
    return v

def parse_args():
    parser = ArgumentParser(allow_abbrev=False)
    parser.add_argument('-b', '--bucket', default=bucket_name)
    parser.add_argument('-u', '--username', default=username)
    parser.add_argument('-p', '--password', default=password)
    parser.add_argument('--port', default=kv_node_port, type=check_port, help='KV node port (11210 or 11207 for TLS)')
    parser.add_argument('--host', default=kv_node_host, help='KV node hostname')
    parser.add_argument('--tls', default=kv_node_ssl, action='store_true')
    parser.add_argument('--cid', default=collection_id, type=int)
    parser.add_argument('--search-all-vbs', dest='search_all_vbs', action='store_true', help='Search all vbuckets')
    parser.add_argument('--print-xattrs', dest='print_xattrs', action='store_true')
    parser.add_argument('--delete', action='store_true', help='Delete docs with cid key prefix')
    parser.add_argument('--restore', action='store_true', help='Add docs removing the cid key prefix')
    parser.add_argument('--add-test-doc', metavar='DOC_ID', dest='add_test_doc', help='Add a test doc with cid key prefix')
    return parser.parse_args()

def main():
    global bucket_name, username, password, kv_node_host, kv_node_port, kv_node_ssl, collection_id, search_all_vbs
    options = parse_args()
    bucket_name = options.bucket
    username = options.username
    password = options.password
    kv_node_host = options.host
    kv_node_port = options.port
    kv_node_ssl = options.tls
    collection_id = options.cid
    search_all_vbs = options.search_all_vbs
    assert collection_id >= 0 and collection_id < 32
    doc_ids = get_doc_ids()
    print(f'Indexed {len(doc_ids)} cid-prefixed doc ids\n')
    connect_cluster()
    print()
    if options.add_test_doc is not None:
        id = options.add_test_doc
        key = encode_key(id, collection_id)
        escaped_key = json.dumps(key.decode(errors='ignore'))
        try:
            vbid = get_vbid(id)
            add_doc(key, 0, '{}', 0, vbid)
            print('Added test doc', escaped_key, 'vb:', vbid)
            vbid = get_vbid(key)
            add_doc(key, 0, '{}', 0, vbid)
            print('Added test doc', escaped_key, 'vb:', vbid)
            vbid = get_vbid(b'\0' + key)
            add_doc(key, 0, '{}', 0, vbid)
            print('Added test doc', escaped_key, 'vb:', vbid)
        except mc_bin_client.ErrorKeyEexists:
            print('Already exists', escaped_key)
        disconnect()
        return
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
        docs.sort(reverse=True, key=lambda x: x[1]) # sort by cas
        prefix = encode_key('', collection_id).decode(errors='ignore')
        restored_one = False
        for (doc, cas, flags, vbid) in docs:
            print('Got', escaped_id, 'cas:', cas, 'flags:', flags, 'vb:', vbid)
            if options.print_xattrs:
                print('XATTRS:', json.dumps(get_xattrs(id, vbid), indent=2))
            if options.restore and not restored_one:
                try:
                    new_id = id.removeprefix(prefix)
                    add_doc(new_id, collection_id, doc, flags)
                    print('Added', json.dumps(new_id), 'cid:', collection_id)
                    added_count += 1
                    restored_one = True
                except mc_bin_client.ErrorKeyEexists:
                    print('Already exists', json.dumps(new_id), 'cid:', collection_id)
                    already_exist_count += 1
            if options.delete:
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
