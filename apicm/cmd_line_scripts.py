import argparse
import os
import sys

from apicm import NiagaraCMApi


def get_cm_api():
    args = parse_args()

    if args.cm_host is None:
        cm_host = os.getenv('CM_HOST')
    else:
        cm_host = args.cm_host

    if args.user is None:
        cm_user = os.getenv('CM_USER')
    else:
        cm_user = args.user

    if args.password is None:
        cm_pass = os.getenv('CM_PASS')
    else:
        cm_pass = args.password

    cm_port = args.port
    cm_api_version = args.api_version
    if cm_host is None:
        print (
            'No Cloudera Manager host address provided. '
            'You should export CM_HOST environment variable.'
        )
        sys.exit(1)

    if cm_user is None:
        print (
            'No Cloudera Manager Username provided. '
            'You should export CM_USER environment variable.'
        )
        sys.exit(1)

    if cm_pass is None:
        print (
            'No Cloudera Manager User password provided. '
            'You should export CM_PASS environment variable.'
        )
        sys.exit(1)

    cm = NiagaraCMApi(cm_host, cm_user, cm_pass, port=cm_port, version=cm_api_version)
    return cm, args


def get_hdfs_namenode():
    cloudera_manager, args = get_cm_api()
    namenodes = cloudera_manager.get_hosts_by_role('hdfs', 'NAMENODE', haStatus='ACTIVE')
    for namenode in namenodes:
        print namenode


def get_kafka_brokers():
    cloudera_manager, args = get_cm_api()
    kafka_hosts = cloudera_manager.get_hosts_by_role('kafka', 'KAFKA_BROKER')
    for kafka_host in kafka_hosts:
        print kafka_host


def get_zk_nodes():
    cloudera_manager, args = get_cm_api()
    zookeeper_hosts = cloudera_manager.get_hosts_by_role('zookeeper', 'SERVER')
    for zookeeper_host in zookeeper_hosts:
        print zookeeper_host


def get_zk_ports():
    cloudera_manager, args = get_cm_api()
    zookeeper_port = cloudera_manager.get_service_ports('zookeeper', 'zookeeper-SERVER-BASE')
    print zookeeper_port


def get_kafka_ports():
    cloudera_manager, args = get_cm_api()
    kafka_port = cloudera_manager.get_service_ports('kafka', 'kafka-KAFKA_BROKER-BASE')
    print kafka_port


def get_kafka_roles():
    cloudera_manager, args = get_cm_api()
    kafka_roles = cloudera_manager.get_role_types('kafka')
    for role in kafka_roles:
        print role


def get_zk_roles():
    cloudera_manager, args = get_cm_api()
    zk_roles = cloudera_manager.get_role_types('zookeeper')
    for role in zk_roles:
        print role


def get_kafka_role_groups():
    cloudera_manager, args = get_cm_api()
    kafka_roles_groups = cloudera_manager.get_all_role_config_groups('kafka')
    for role in kafka_roles_groups:
        print role


def get_zk_role_groups():
    cloudera_manager, args = get_cm_api()
    zk_roles_groups = cloudera_manager.get_all_role_config_groups('zookeeper')
    for role in zk_roles_groups:
        print role


def add_kafka_log_dir():
    cloudera_manager, args = get_cm_api()
    ret_code, message = cloudera_manager.edit_log_dir_from_kafka_broker(args.hostname, args.log_dir, 'add')
    print message


def remove_kafka_log_dir():
    cloudera_manager, args = get_cm_api()
    ret_code, message = cloudera_manager.edit_log_dir_from_kafka_broker(args.hostname, args.log_dir, 'remove')
    print message


def start_kafka_broker():
    cloudera_manager, args = get_cm_api()
    message = cloudera_manager.kafka_broker_action(nodename=args.hostname, action='start')
    print message


def stop_kafka_broker():
    cloudera_manager, args = get_cm_api()
    message = cloudera_manager.kafka_broker_action(nodename=args.hostname, action='stop')
    print message


def restart_kafka_broker():
    cloudera_manager, args = get_cm_api()
    message = cloudera_manager.kafka_broker_action(nodename=args.hostname, action='restart')
    print message


def get_kafka_broker_status():
    cloudera_manager, args = get_cm_api()
    message = cloudera_manager.get_broker_status(nodename=args.hostname)
    print message


def get_log_dirs_list():
    cloudera_manager, args = get_cm_api()
    message = cloudera_manager.get_log_dirs_for_kafka_broker(nodename=args.hostname)
    print message


def get_yarn_resource_manager():
    cloudera_manager, args = get_cm_api()
    message = cloudera_manager.get_hosts_by_role(service_name=args.yarn_service_name, role='RESOURCEMANAGER', haStatus='ACTIVE')
    for host in message:
        print host


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--cm-host', type=str, help='Host with cloudera manager.', default=None)
    parser.add_argument('-u', '--user', type=str, help="Cloudera manager user.", default=None)
    parser.add_argument('-p', '--password', type=str, help="Cloudera manager read-only user password.", default=None)
    parser.add_argument('--port', type=str, help="Cloudera manager port. Default: 7180.", default='7180')
    parser.add_argument('--api-version', type=str, help="Cloudera manager API version. Default: 17.", default='17')
    parser.add_argument('--log-dir', type=str, help='Log directory to remove or add', default=None)
    parser.add_argument('--hostname', type=str, help='Hostname of server.', default=None)
    parser.add_argument('--yarn-service-name', type=str, help='Name of the yarn service.', default='yarn')
    args = parser.parse_args()
    return args


def help():
    print """
    Before using apicm commands ensure, that you provided environment variables or passed required
    values as arguments.
    
    Args:
    ${SAMPLE COMMAND} <cloduera manager hostname or IP address> <username> <password> <port> <API version>
    
    Environment variables:
    CM_HOST: Cloudera Manager server hostname.
    CM_PORT: Optional. Port of Cloudera Manager, if it differs from default port (default 7180).
    CM_USER: Username of Cloudera Manager user. User should have at least read only permissions.
    CH_PASS: Cloudera manager user's password.
    CM_API_VERSION: Optional. Version of API to use. (default 17).  
    
    Available commands:
    get-kafka-brokers: Get hostnames of all kafka brokers.
    get-zk-nodes: Get hostnames of all zookeeper servers.
    get-kafka-port: Get common kafka port, which set in kafka service configuration.
    get-zk-port: Get common zookeeper port, which set in zookeeper service configuration.
    get-kafka-roles: Get all available roles in kafka service.
    get-zk-roles: Get all available roles in zookeeper service.
    get-kafka-role-groups: Get all role config groups in kafka service.
    get-zk-role-groups: Get all rol config groups in kafka service.
    apicm-help: Print this help info.
    """
