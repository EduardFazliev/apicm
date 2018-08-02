import atexit
import os
import logging
import sys
import unittest
import time

from subprocess import Popen, PIPE
from cm_api.api_client import ApiResource as api_cm
from cm_api.api_client import ApiException

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from apicm import NiagaraCMApi


logger = logging.getLogger()
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

CM_HOST = 'localhost'
CM_HOST_HOSTNAME='quickstart.cloudera'
CM_USER = 'cloudera'
CM_PASSWORD = 'cloudera'
TIMEOUT = 120

KAFKA_REPO_URL = 'http://archive.cloudera.com/kafka/parcels/2.0.2/'
KAFKA_PARCEL = 'KAFKA-2.0.2-1.2.0.2.p0.5'
KAFKA_VERSION = '2.0.2-1.2.0.2.p0.5'

CDH_VERSION = '5.7.0-1.cdh5.7.0.p0.45'

CLUSTER_NAME = 'Cloudera QuickStart'


def parcel_action(command, stage, service, version, timeout):
    base_url = 'clusters/{2}/parcels/products/{0}/versions/{1}'.format(service, version, CLUSTER_NAME)

    command_post = api.post(
        '{0}/commands/{1}'.format(base_url, command)
    )
    logger.info("Command {0} executed, \nrespond:{1}".format(command, command_post))

    parcel_command = False
    for i in range(0, timeout):
        try:
            state = api.get(base_url)
        except ApiException:
            logger.error('Can not get parcel state!')
        else:
            logger.info(
                         'Current state: {0}, progress: {1}, total progress: {2}'.format(
                             state['stage'],
                             state['state']['progress'],
                             state['state']['totalProgress'])
                         )
            if state['state']['progress'] == state['state']['totalProgress'] and state['stage'] != stage:
                parcel_command = True
                break
        time.sleep(20)
    if not parcel_command:
        logger.error('Command {0} failed or timeout is reached.'.format(command))
        sys.exit(1)
    else:
        logger.info('Command {0} finished successfully.'.format(command))


def parcel_init(api, cluster):
    cm_config = api.get('cm/config')
    for option in cm_config['items']:
        if option['name'] == 'REMOTE_PARCEL_REPO_URLS':
            repo_urls = option['value']
    logger.info('Got remote parcel repository urls: {0}'.format(repo_urls))

    if KAFKA_REPO_URL not in repo_urls:
        new_repo_urls = repo_urls + ', ' + KAFKA_REPO_URL

        respond = api.put(
            'cm/config', data='{{"items": [{{"name": "REMOTE_PARCEL_REPO_URLS", "value": "{0}"}}]}}'.format(new_repo_urls)
        )
        logger.info('Updating Cloudera Manager remote parcels repository URLs list, respond from CM: {0}'.format(respond))
    else:
        logger.info('Kafka repo URL is already in remote parcel repo URLs in this CM instance.')

    parcel_action('startDownload', 'DOWNLOADING', 'CDH', CDH_VERSION, TIMEOUT)
    parcel_action('startDistribution', 'DISTRIBUTING', 'CDH', CDH_VERSION, TIMEOUT)
    parcel_action('activate', 'ACTIVATING', 'CDH', CDH_VERSION, TIMEOUT)

    parcel_action('startDownload', 'DOWNLOADING', 'KAFKA', KAFKA_VERSION, TIMEOUT)
    parcel_action('startDistribution', 'DISTRIBUTING', 'KAFKA', KAFKA_VERSION, TIMEOUT)
    parcel_action('activate', 'ACTIVATING', 'KAFKA', KAFKA_VERSION, TIMEOUT)


def start_services(api, cluster, srv):
    for s in srv:
        service = cluster.get_service(s)
        out = service.start().wait()
        logger.info(out)


def create_kafka_service(api, cluster, name, type):
    service = cluster.create_service(name, type)
    service.create_role('broker1', 'KAFKA_BROKER', 'quickstart.cloudera')
    cluster.auto_configure()


class TestNiagaraCMApi(unittest.TestCase):
    KAFKA_HOSTS = [CM_HOST_HOSTNAME]
    KAFKA_PORT = '9092'
    ZK_PORT = '2181'
    KAFKA_CONFIG_GROUPS = ['kafka-KAFKA_BROKER-BASE', 'kafka-KAFKA_MIRROR_MAKER-BASE']
    ZK_CONFIG_GROUPS = ['zookeeper-SERVER-BASE']
    LOG_DIRS = ['', '/data1/kafka']

    def setUp(self):
        self.cm_api = NiagaraCMApi(cm_host=CM_HOST, user=CM_USER, password=CM_PASSWORD, version='12', cluster=CLUSTER_NAME)

    def test_get_hosts_by_role_kafka(self):
        result = self.cm_api.get_hosts_by_role('kafka', 'KAFKA_BROKER')
        self.assertEqual(result, self.KAFKA_HOSTS)

    def test_get_hosts_by_role_zookeeper(self):
        result = self.cm_api.get_hosts_by_role('zookeeper', 'SERVER')
        self.assertEqual(result, self.KAFKA_HOSTS)

    def test_get_service_ports_kafka(self):
        result = self.cm_api.get_service_ports('kafka', 'kafka-KAFKA_BROKER-BASE')
        self.assertEqual(result, self.KAFKA_PORT)

    def test_get_service_ports_zookeeper(self):
        result = self.cm_api.get_service_ports('zookeeper', 'zookeeper-SERVER-BASE')
        self.assertEqual(result, self.ZK_PORT)

    def test_get_all_role_config_groups_kafka(self):
        result = self.cm_api.get_all_role_config_groups('kafka')
        self.assertEqual(result, self.KAFKA_CONFIG_GROUPS)

    def test_get_all_role_config_groups_zk(self):
        result = self.cm_api.get_all_role_config_groups('zookeeper')
        self.assertEqual(result, self.ZK_CONFIG_GROUPS)

    def test_get_log_dirs_for_kafka_broker(self):
        self.cm_api.edit_log_dir_from_kafka_broker(nodename=CM_HOST_HOSTNAME, log_dir='/data1/kafka', action='add')
        result = self.cm_api.get_log_dirs_for_kafka_broker(nodename=CM_HOST_HOSTNAME)
        self.cm_api.edit_log_dir_from_kafka_broker(nodename=CM_HOST_HOSTNAME, log_dir='/data1/kafka', action='remove')
        self.assertEqual(self.LOG_DIRS, result)


def execute_cmd(cmd):
    p = Popen(cmd, stdout=PIPE, stderr=PIPE, shell=True)
    out, err = p.communicate()
    ret_code = p.returncode

    if ret_code != 0:
        logger.error(err)
        sys.exit(1)

    return out, err


def start_docker():
    docker_pull_cmd = 'docker pull cloudera/quickstart'
    out, err = execute_cmd(docker_pull_cmd)

    get_id_cmd = 'docker images | grep "cloudera/quickstart"'
    out, err = execute_cmd(get_id_cmd)

    while '  ' in out:
        out = out.replace('  ', ' ')
    docker_id = out.split(' ')[2]
    logger.info('Docker id {0}'.format(docker_id))

    start_cmd = (
        'docker run --hostname=quickstart.cloudera --privileged=true -t -i -d -p 7180:7180 {0} '
        '/usr/bin/docker-quickstart'.format(docker_id)
    )
    logger.info('Docker start command: {0}'.format(start_cmd))
    container_id, err = execute_cmd(start_cmd)
    container_id = container_id.replace('\n', '')
    logger.info('Docker run result: {0}'.format(out))

    docker_logs_cmd = 'docker logs {0}'.format(container_id)
    for i in xrange(1, TIMEOUT):
        out, err = execute_cmd(docker_logs_cmd)
        if 'Started Impala Server (impalad):' in out:
            logger.info('Cloudera QuickStart initialized.')
            break
        else:
            time.sleep(5)

    start_enterprise_cmd = 'docker exec -i {0} sudo /home/cloudera/cloudera-manager --enterprise'.format(container_id)
    logger.info('Docker exec command: {0}'.format(start_enterprise_cmd))
    out, err = execute_cmd(start_enterprise_cmd)

    return container_id


def stop_docker(container_id):
    docker_stop = 'docker stop {0}'.format(container_id)
    out, err = execute_cmd(docker_stop)
    logger.info('Docker stop: {0}'.format(out))

    rm_container = 'docker rm {0}'.format(container_id)
    out, err = execute_cmd(rm_container)
    logger.info('Docker rm: {0}'.format(out))

if __name__ == '__main__':
    container = start_docker()

    atexit.register(stop_docker, container)

    api = api_cm(server_host=CM_HOST, username=CM_USER, password=CM_PASSWORD, server_port=7180, version=12)
    cluster = api.get_cluster(CLUSTER_NAME)
    parcel_init(api, cluster)
    create_kafka_service(api, cluster, 'kafka', 'KAFKA')
    start_services(api, cluster, ['zookeeper', 'kafka'])
    api.put(
        'clusters/Cloudera QuickStart/services/kafka/roleConfigGroups/kafka-KAFKA_BROKER-BASE/config', data='{"items": [{"name": "port", "value": "9092"}]}'
    )

    api.put(
        'clusters/Cloudera QuickStart/services/zookeeper/roleConfigGroups/zookeeper-SERVER-BASE/config', data='{"items": [{"name": "clientPort", "value": "2181"}]}'
    )

    unittest.main()
