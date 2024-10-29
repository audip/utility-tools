# -*- coding: utf-8 -*-
"""Elasticsearch Index Resizer Script
This module generates dynamic shard size for elasticsearch indices.

Example:
    An example when connecting over https and using debug mode:

        $ python application.py --host elasticsearch-url --port 443 --scheme https --debug True

Attributes:
    host (str): hostname for elasticsearch cluster (localhost/elasticsearch-url).
    scheme (str): scheme for elasticsearch cluster (http/https).
    port (int): port for elasticsearch cluster (9200/443).
    debug (bool): debug flag (True/False).
"""

import collections
import re
import click
import logging
import math
import time
import json
import certifi
from datetime import date
from elasticsearch import Elasticsearch


REQUEST_TIMEOUT = 300

logger = logging.getLogger(__name__)
logging.basicConfig(level="INFO")

def connect_elasticsearch(host, scheme, port, ssl=False):
    """Create an elasticsearch client.

    Args:
        host (str): elasticsearch hostname
        scheme (str): elasticsearch scheme
        port (int): elasticsearch port
        ssl (bool): option to enable SSL

    Returns:
        Elasticsearch: returns an elasticsearch client. Object for success, None otherwise.
    """
    if ssl == True:
        es_client = Elasticsearch(
                [host],
                scheme=scheme,
                port=port,
                use_ssl=ssl,
                ca_certs=certifi.where()
            )
    else:
        es_client = Elasticsearch(
                [host],
                scheme=scheme,
                port=port
            )
    return es_client if es_client else None

def get_all_indices(es_client):
    """Fetche all indices from elasticsearch.

    Args:
        es_client (Elasticsearch): client for elasticsearch

    Returns:
        list: returns list of dict where each dict is {index_name: size}.
    """
    indices_with_sizes_list = list()
    indices_with_sizes_list = es_client.cat.indices(
                                bytes='gb',
                                format='json',
                                h='index,docs.count,store.size',
                                s='store.size:desc',
                                # v=True,
                                request_timeout=REQUEST_TIMEOUT)
    return indices_with_sizes_list

def get_index_name(index_blob):
    """Extract index names by stripping datetime pattern.

    Args:
        index_blob (str): index name (logstash-2018.02.30)

    Returns:
        str: returns an index name (logstash). str for success, None otherwise.
    """
    try:
        date_time_pattern = re.search(r'-(\d+.\d+.\d+)', index_blob)
        # when no 2018.07.21 exists in the pattern, assume other team's indices
        if not date_time_pattern:
            return None
        parsed_index = index_blob[:date_time_pattern.start()]
        return str(parsed_index)
    except AttributeError as e:
        print(index_blob, date_time_pattern)

def get_avg_index_size(indices_with_sizes_list=[]):
    """Calculate average index size for each index.

    Args:
        indices_with_sizes_list (list): list of dict {index_name: [size1,size2,size3]}

    Returns:
        dict: returns dict of index_name as key, avg_size as value.
    """
    indices_with_avg_sizes_dict,indices_with_all_sizes_dict = collections.defaultdict(), dict()

    # get all sizes for an index as key: index, value: [size1, size2]
    for index_dict in indices_with_sizes_list:
        parsed_index = get_index_name(index_dict['index'])

        if parsed_index is None:
            continue

        if parsed_index in indices_with_all_sizes_dict.keys():
            indices_with_all_sizes_dict[parsed_index].append(str(index_dict['store.size']))
        else:
            indices_with_all_sizes_dict[parsed_index] = [str(index_dict['store.size'])]

    # compute avg size for an index
    for index, sizes in indices_with_all_sizes_dict.items():
        sizes = [int(size) for size in sizes]
        indices_with_avg_sizes_dict[index] = int(sum(sizes)/len(sizes))
    return indices_with_avg_sizes_dict

def get_num_data_nodes(es_client):
    """Fetch number of data nodes in elasticsearch cluster.

    Args:
        es_client (Elasticsearch): client for elasticsearch

    Returns:
        int: returns count of data nodes.
    """
    nodes_resp = es_client.nodes.info(node_id=['data:true'],request_timeout=REQUEST_TIMEOUT)
    return nodes_resp['_nodes']['total']

def get_unix_timestamp():
    """Calculate unix timestamp for today's date.

    Returns:
        int: returns unix timestamp in seconds for today's date (1536883200 for '2018-09-14').
    """
    return int(time.mktime(date.today().timetuple()))

def put_index_templates(es_client, indices_with_num_shards_dict={}):
    """Apply index templates on elasticsearch cluster.

    Args:
        es_client (Elasticsearch): client for elasticsearch
        indices_with_num_shards_dict (dict): dict with index_name as key, num_of_shards as value

    Returns:
        None: returns None for success or failure.
    """
    version = get_unix_timestamp()
    for index, num_of_shards in indices_with_num_shards_dict.items():
        body = {
            "order": 10,
            "index_patterns" : [str(index+"-*")],
            "settings": {
            "index": {
              "routing": {
                "allocation": {
                  "total_shards_per_node": "2"
                }
              },
              "refresh_interval": "60s",
              "number_of_shards": int(num_of_shards),
              "translog": {
                "retention": {
                  "age": "24h"
                },
                "sync_interval": "5s",
                "durability": "async"
              },
              "auto_expand_replicas": "false",
              "unassigned": {
                "node_left": {
                  "delayed_timeout": "5m"
                }
              },
              "number_of_replicas": "1"
            }
            },
            "version": int(version),
            "mappings": {},
            "aliases": {}
        }
        logger.debug(str(body))
        es_client.indices.put_template(
                            name=index,
                            body=body)
    return

def get_num_shards_for_indices(num_of_data_nodes, indices_with_avg_sizes_dict):
    """Calculate number of shards for each index.

    Args:
        num_of_data_nodes (int): number of data nodes
        indices_with_avg_sizes_dict (dict): dict with index_name as key, avg_size as value

    Returns:
        dict: returns dict of index_name as key, num_of_shards as value.
    """
    min_num_shards, max_num_shards = 1, (num_of_data_nodes - 1)
    indices_with_num_shards_dict = dict()

    for index, avg_size in indices_with_avg_sizes_dict.items():
        if avg_size <= 10:
            num_of_shards = 1
            continue
        elif avg_size <= 100:
            num_of_shards = min(int(math.ceil(avg_size/10)),max_num_shards)
        elif avg_size <= 300:
            num_of_shards = min(int(math.ceil(avg_size/20)),max_num_shards)
        else:
            num_of_shards = max_num_shards
        indices_with_num_shards_dict[index] = num_of_shards

    return indices_with_num_shards_dict

@click.command()
@click.option('--host', default="localhost", help='Elasticsearch host (default=localhost)')
@click.option('--port', default="9200", help='Elasticsearch port (default=9200)')
@click.option('--scheme', default="http", help='Connection scheme (defautlt=http)')
@click.option('--debug', default="False", help='Enable debug mode (defautlt=False)')
def main(host, port, scheme, debug):
    """Main runner method."""
    try:
        if bool(debug)==True:
            logger.setLevel(logging.DEBUG)

        use_ssl = True if scheme == 'https' else False

        # create an elasticsearch client
        logger.info("step 1: connecting to elasticsearch cluster")

        es_client = connect_elasticsearch(
                    host=host,
                    scheme=scheme,
                    port=port,
                    ssl=use_ssl)

        logger.info("step 1 complete: connected to elasticsearch cluster")

        # fetch all indices in the cluster
        logger.info("step 2: fetching all indices from elasticsearch")
        indices_with_sizes_list = get_all_indices(es_client)
        logger.info(("step 2 complete: found {} indices in elasticsearch").format(len(indices_with_sizes_list)))

        # compute average indice size for each index
        logger.info("step 3: computing avg index size for each index")
        indices_with_avg_sizes_dict = get_avg_index_size(indices_with_sizes_list)
        logger.info(("step 3 complete: found {} unique indices in elasticsearch").format(len(indices_with_avg_sizes_dict)))

        # fetch number of data nodes
        logger.info("step 4: fetching count of elasticsearch data nodes")
        num_of_data_nodes = get_num_data_nodes(es_client)
        logger.info(("step 4 complete: found {} data nodes in the cluster").format(num_of_data_nodes))

        # compute number of shards for each index
        logger.info("step 5: computing number of shards for each index")
        indices_with_num_shards_dict = get_num_shards_for_indices(num_of_data_nodes, indices_with_avg_sizes_dict)
        logger.info(("step 5 complete: found {} indices with more than 1 shard").format(len(indices_with_num_shards_dict)))

        # publish index templates to elasticsearch
        logger.info("step 6: deploying index templates (overwrites existing templates)")
        put_index_templates(es_client, indices_with_num_shards_dict)

        logger.info(("step 6 complete: {} index templates deployed with today's version {} (unix timestamp of {})").format(len(indices_with_num_shards_dict),str(get_unix_timestamp()), str(date.today())))
    except Exception as e:
        print(e)

if __name__ == '__main__':
    main()
