import mock
import os
import unittest

from apicm import cmd_line_scripts


CM_HOST = 'cm_host'
CM_PORT = '1900'
CM_USER = 'admin'
CM_PASS = 'admin'
CM_API_VERSION = '12'


class TestCMDLineScripts(unittest.TestCase):
    def setUp(self):
        os.environ['CM_HOST'] = CM_HOST
        os.environ['CM_USER'] = CM_USER
        os.environ['CM_PASS'] = CM_PASS
        os.environ['CM_PORT'] = CM_PORT
        os.environ['CM_API_VERSION'] = CM_API_VERSION

    @mock.patch('apicm.NiagaraCMApi')
    @mock.patch('apicm.NiagaraCMApi.get_hosts_by_role')
    def test_get_kafka_brokers(self, get_host, ncmapi):
        cmd_line_scripts.get_kafka_brokers()
        get_host.assert_called_with('kafka', 'KAFKA_BROKER')

    @mock.patch('apicm.NiagaraCMApi')
    @mock.patch('apicm.NiagaraCMApi.get_hosts_by_role')
    def test_get_zk_nodes(self, get_host, ncmapi):
        cmd_line_scripts.get_zk_nodes()
        get_host.assert_called_with('zookeeper', 'SERVER')

    @mock.patch('apicm.NiagaraCMApi')
    @mock.patch('apicm.NiagaraCMApi.get_service_ports')
    def test_get_kafka_ports(self, get_port, ncmapi):
        cmd_line_scripts.get_kafka_ports()
        get_port.assert_called_with('kafka', 'kafka-KAFKA_BROKER-BASE')

    @mock.patch('apicm.NiagaraCMApi')
    @mock.patch('apicm.NiagaraCMApi.get_service_ports')
    def test_get_zk_ports(self, get_host, ncmapi):
        cmd_line_scripts.get_zk_ports()
        get_host.assert_called_with('zookeeper', 'zookeeper-SERVER-BASE')

    @mock.patch('apicm.NiagaraCMApi')
    @mock.patch('apicm.NiagaraCMApi.get_role_types')
    def test_get_kafka_roles(self, get_r, ncmapi):
        cmd_line_scripts.get_kafka_roles()
        get_r.assert_called_with('kafka')

    @mock.patch('apicm.NiagaraCMApi')
    @mock.patch('apicm.NiagaraCMApi.get_role_types')
    def test_get_zk_roles(self, get_r, ncmapi):
        cmd_line_scripts.get_zk_roles()
        get_r.assert_called_with('zookeeper')

    @mock.patch('apicm.NiagaraCMApi')
    @mock.patch('apicm.NiagaraCMApi.get_all_role_config_groups')
    def test_get_kafka_role_gropus(self, get_arcg, ncmapi):
        cmd_line_scripts.get_kafka_role_groups()
        get_arcg.assert_called_with('kafka')

    @mock.patch('apicm.NiagaraCMApi')
    @mock.patch('apicm.NiagaraCMApi.get_all_role_config_groups')
    def test_get_zk_role_groups(self, get_arcg, ncmapi):
        cmd_line_scripts.get_zk_role_groups()
        get_arcg.assert_called_with('zookeeper')


if __name__ == '__main__':
    unittest.main()
