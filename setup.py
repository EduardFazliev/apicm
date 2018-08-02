from setuptools import setup

setup(name='apicm',
      version='0.1',
      description='Cloudera Manager API implementation based on cm_api module.',
      url='https://github.com/integralads/niagara',
      author='Eduard Fazliev',
      author_email='napalmedd@gmail.com',
      license='MIT',
      packages=['apicm'],
      install_requires=[
          'cm_api'
      ],
      entry_points={
            'console_scripts':
                [
                    'get-kafka-brokers=apicm.cmd_line_scripts:get_kafka_brokers',
                    'get-zk-nodes=apicm.cmd_line_scripts:get_zk_nodes',
                    'get-zk-port=apicm.cmd_line_scripts:get_zk_ports',
                    'get-kafka-port=apicm.cmd_line_scripts:get_kafka_ports',
                    'get-kafka-roles=apicm.cmd_line_scripts:get_kafka_roles',
                    'get-zk-roles=apicm.cmd_line_scripts:get_zk_roles',
                    'get-kafka-role-groups=apicm.cmd_line_scripts:get_kafka_role_groups',
                    'get-zk-role-groups=apicm.cmd_line_scripts:get_zk_role_groups',
                    'get-hdfs-namenode=apicm.cmd_line_scripts:get_hdfs_namenode',
                    'apicm-help=apicm.cmd_line_scripts:help',
                    'add-kafka-log-dir=apicm.cmd_line_scripts:add_kafka_log_dir',
                    'remove-kafka-log-dir=apicm.cmd_line_scripts:remove_kafka_log_dir',
                    'stop-kafka-broker=apicm.cmd_line_scripts:stop_kafka_broker',
                    'start-kafka-broker=apicm.cmd_line_scripts:start_kafka_broker',
                    'restart-kafka-broker=apicm.cmd_line_scripts:restart_kafka_broker',
                    'get-kafka-broker-status=apicm.cmd_line_scripts:get_kafka_broker_status',
                    'get-kafka-broker-log-dirs=apicm.cmd_line_scripts:get_log_dirs_list',
                    'get-yarn-rm-host=apicm.cmd_line_scripts:get_yarn_resource_manager'
                ],
      },
      zip_safe=False)
