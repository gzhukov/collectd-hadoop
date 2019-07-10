#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import sys
import time
import collectd
import urllib3

METRICS = {
    # ResourceManager
    #
    'Hadoop:service=ResourceManager,name=RMNMInfo': {
        'attributes': {},
        'embedded_attributes': {
            'LiveNodeManagers': 'HostName',
        }
    },
    'Hadoop:service=ResourceManager,name=QueueMetrics,q0=root,q1=default': {},
    'Hadoop:service=ResourceManager,name=QueueMetrics,q0=root': {},
    'Hadoop:service=ResourceManager,name=JvmMetrics': {},
    'Hadoop:service=ResourceManager,name=ClusterMetrics': {},
    # NameNode
    #
    'Hadoop:service=NameNode,name=JvmMetrics': {},
    'Hadoop:service=NameNode,name=NameNodeActivity': {},
    'Hadoop:service=NameNode,name=NameNodeStatus': {},
    'Hadoop:service=NameNode,name=NameNodeInfo': {
        'attributes': {},
        'embedded_attributes': {
            'LiveNodes': '*'
        }
    },
    'Hadoop:service=NameNode,name=StartupProgress': {},
    'Hadoop:service=NameNode,name=FSNamesystem': {},
    'Hadoop:service=NameNode,name=BlockStats': {},
    'Hadoop:service=NameNode,name=FSNamesystemState': {},
    'Hadoop:service=NameNode,name=RetryCache.NameNodeRetryCache': {},
    'Hadoop:service=NameNode,name=RpcDetailedActivityForPort8020': {},
    # SecondaryNameNode
    #
    'Hadoop:service=SecondaryNameNode,name=JvmMetrics': {},
    # HistoryServer
    #
    'Hadoop:service=JobHistoryServer,name=JvmMetrics': {},
    # NodeManager
    #
    'Hadoop:service=NodeManager,name=NodeManagerMetrics': {},
    'Hadoop:service=NodeManager,name=JvmMetrics': {},
    # DataNode
    #
    'Hadoop:service=DataNode,name=JvmMetrics': {},
    'Hadoop:service=DataNode,name=FSDatasetState': {},
    # 'Hadoop:service=DataNode,name=DataNodeInfo': {
    #     'embedded_attributes': {
    #         'VolumeInfo': '*'
    #     }
    # },
}

PORTS = []
HOST = '127.0.0.1'
URL = '/jmx?qry=Hadoop:*'


# PORT = 8088 YARN
# PORT = 50070 NameNode
# PORT = 50090 SecondaryNameNode
# PORT = 19888 HistoryServer
# PORT = 8042 NodeManager
# PORT = 50075 DataNode


def convert_jobject_name_to_metric_path(jobject_name):
    metric_path = list()
    for entry in jobject_name.split(','):
        metric_path.append(entry.split('=')[1])
    return '.'.join(metric_path)


def get_attributes(jobj, attributes, path):
    data = dict()
    # For whitelist of attrubutes
    if type(attributes) == set:
        for attribute in attributes:
            value = get_single_value(jobj, attribute)
            if value is not None:
                data.update({'.'.join([path, attribute]): value})
    # For pre-defined deep structure of attributes
    if type(attributes) == dict:
        for key, prefix in attributes.items():
            jdata = json.loads(jobj[key])
            data.update(get_multiple_attributes_with_prefix(jdata, key, prefix, path))
    if not attributes:
        for attribute in jobj:
            value = get_single_value(jobj, attribute)
            if value is not None:
                data.update({'.'.join([path, attribute]): value})
    return data


def get_single_value(jobj, attribute):
    # Returns all non-string digital values
    value = jobj[attribute]
    if type(value) == int or type(value) == float:
        return value
    else:
        return None


def get_multiple_attributes_with_prefix(jobjects, attribute, key, path):
    data = dict()
    for obj in jobjects:
        if key == '*':
            data.update(get_attributes(jobjects[obj], None, '.'.join([path, obj])))
            continue
        prefix = obj[key]
        for atr in obj:
            value = get_single_value(obj, atr)
            if value:
                data.update({'.'.join([path, attribute, prefix, atr]): value})
    return data


def run():
    for port in PORTS:
        try:
            read_metrics(port)
        except Exception:
            collectd.error("Can't get metrics from port %s" % port)


def read_metrics(port):
    metrics = dict()
    content = request('http://%s:%s%s' % (HOST, port, URL))
    jcontent = json.loads(content)['beans']
    for jobj in jcontent:
        if jobj['name'] in METRICS:
            data = dict()
            path = convert_jobject_name_to_metric_path(jobj['name'])
            # Use if we are going to move deeper to json structure
            if 'embedded_attributes' in METRICS[jobj['name']]:
                attributes = METRICS[jobj['name']]['embedded_attributes']
                data = get_attributes(jobj, attributes, path)
                metrics.update(data)
            # Use for `whitelisting` attributes
            if 'attributes' in METRICS[jobj['name']]:
                attributes = METRICS[jobj['name']]['attributes']
                data = get_attributes(jobj, attributes, path)
                metrics.update(data)
            # Getting all non-string attributes
            if not METRICS[jobj['name']]:
                data = get_attributes(jobj, None, path)
                metrics.update(data)

    # Sending metrics to collectd
    path = "hadoop"
    cd_values = collectd.Values()
    cd_values.time = get_unixtime()
    cd_values.plugin = path
    cd_values.type = 'gauge'

    for metric, value in metrics.items():
        collectd.debug("%s: %d" % (metric, value))
        cd_values.type_instance = metric
        cd_values.values = [value]
        cd_values.dispatch()


def request(url):
    collectd.debug('Request: %s' % url)
    http = urllib3.PoolManager()
    r = http.request('GET', url)
    content = r.data.decode('utf-8')
    return content


# Get Unix time
def get_unixtime():
    return int(time.time())


# Reader callback
def reader_callback():
    collectd.debug("hadoop: Start reading")
    run()
    collectd.debug("hadoop: Finish reading")


# Configure callback
def configure_callback(conf):
    collectd.debug("hadoop: Start configuring")
    for node in conf.children:
        if node.key == 'Ports':
            global PORTS
            PORTS = node.values
        if node.key == 'Host':
            global HOST
            HOST = node.values
        if node.key == 'Url':
            global URL
            URL = node.values
    collectd.debug("hadoop: Finish configuring")


collectd.register_config(configure_callback)
collectd.register_read(reader_callback)

