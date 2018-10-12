from cm_api.api_client import ApiResource, ApiException
# pathos module allows to use
# class methods in multiprocessing.


class NiagaraCMApi(object):
    def __init__(self, cm_host, user, password, cluster='cluster', port='7180', version=17):
        self.cm_host = cm_host
        self.user = user
        self.password = password
        self.cluster = cluster
        self.port = port
        self.version = version

        self.api = ApiResource(server_host=self.cm_host, server_port=self.port,
                               username=self.user, password=self.password,
                               version=self.version)

    def get_hosts_by_role(self, service_name, role, haStatus=None):
        """
        Method gets all hosts that runs specific service and role.

        Args:
            service_name(str): Name of service that runs under cloudera manager.
            role(str): Role name (f.e. KAFKA_BROKER)

        Returns:
            Sorted list of hostnames, that runs specific service and type.

        """
        cluster = self.api.get_cluster(self.cluster)
        service = cluster.get_service(service_name)
        service_nodes = service.get_roles_by_type(role)
        result = []
        for server in service_nodes:
            if haStatus == 'ACTIVE' and server.haStatus != 'ACTIVE':
                continue
            host_reference = server.hostRef.hostId
            result.append(self.api.get_host(host_reference).hostname)
        return result

    def get_kafka_broker_id_by_hostname(self, nodename, role='KAFKA_BROKER', service_name='kafka'):
        cluster = self.api.get_cluster(self.cluster)
        service = cluster.get_service(service_name)
        service_nodes = service.get_roles_by_type(role)
        for node in service_nodes:
            hostname = self.api.get_host(node.hostRef.hostId).hostname
            if hostname == nodename:
                broker_id = node.get_config()['broker.id']
                return broker_id

    def get_all_brokers_id(self, role='KAFKA_BROKER', service_name='kafka'):
        cluster = self.api.get_cluster(self.cluster)
        service = cluster.get_service(service_name)
        service_nodes = service.get_roles_by_type(role)
        broker_ids = list()
        for node in service_nodes:
            broker_id = node.get_config()['broker.id']
            broker_ids.append(broker_id)
        return broker_ids

    def get_service_ports(self, service_name, role_config_group):
        """
        Method gets ports of specific type of service.

        Args:
            service_name(str): Name of service that runs under cloudera manager.
            role_config_group(str): Role config group name (f.e. kafka-KAFKA_BROKER-BASE)

        Returns:
            ports(dict): Dictionary hostname:port.
        """

        cluster = self.api.get_cluster(self.cluster)
        service = cluster.get_service(service_name)
        config = service.get_role_config_group(role_config_group)
        if service_name == 'kafka':
            try:
                kafka_port = config.config['port'].value
            except AttributeError:
                kafka_port = config.config['port']
            return kafka_port
        elif service_name == 'zookeeper':
            try:
                zk_port = config.config['clientPort'].value
            except AttributeError:
                zk_port = config.config['clientPort']
            return zk_port
        else:
            raise ValueError("Unknown service {0}".format(service_name))

    def get_all_role_config_groups(self, service_name):
        """
        Method gets all service's role config groups names, that could be
        used in get_service_ports.

        Args:
            service_name(str): Name of service that runs under cloduera manager.

        Returns:
            result(dict): Dictionary with all available role config groups names.
        """

        cluster = self.api.get_cluster(self.cluster)
        service = cluster.get_service(service_name)
        all_role_groups = service.get_all_role_config_groups()
        result = []
        for role_group in all_role_groups:
            result.append(role_group.name)
        return result

    def get_log_dirs_for_kafka_broker(self, nodename, service_name='kafka', role='KAFKA_BROKER'):
        cluster = self.api.get_cluster(self.cluster)
        service = cluster.get_service(service_name)
        service_nodes = service.get_roles_by_type(role)
        for node in service_nodes:
            hostname = self.api.get_host(node.hostRef.hostId).hostname
            if hostname == nodename:
                config = node.get_config()['log.dirs'].split(',')
                return config

    def get_broker_status(self, nodename, service_name='kafka', role='KAFKA_BROKER'):
        cluster = self.api.get_cluster(self.cluster)
        service = cluster.get_service(service_name)
        service_nodes = service.get_roles_by_type(role)
        for node in service_nodes:
            hostname = self.api.get_host(node.hostRef.hostId).hostname
            if hostname == nodename:
                return node.roleState, node.maintenanceMode

    def kafka_broker_action(self, nodename, action, service_name='kafka', role='KAFKA_BROKER'):
        cluster = self.api.get_cluster(self.cluster)
        service = cluster.get_service(service_name)
        service_nodes = service.get_roles_by_type(role)
        for node in service_nodes:
            hostname = self.api.get_host(node.hostRef.hostId).hostname
            if hostname == nodename:
                _, maintenance = self.get_broker_status(nodename=nodename)
                if not maintenance:
                    if action == 'start':
                        cmd = service.start_roles(node.name)
                    elif action == 'stop':
                        cmd = service.stop_roles(node.name)
                    elif action == 'restart':
                        cmd = service.restart_roles(node.name)
                    else:
                        return 'Unknown action {0}'.format(action)

                    cmd[0].wait()
                    state, _ = self.get_broker_status(nodename=nodename)
                    return state
                else:
                    return maintenance

    def edit_log_dir_from_kafka_broker(self, nodename, log_dir, action, service_name='kafka', role='KAFKA_BROKER'):
        cluster = self.api.get_cluster(self.cluster)
        service = cluster.get_service(service_name)
        service_nodes = service.get_roles_by_type(role)
        for node in service_nodes:
            hostname = self.api.get_host(node.hostRef.hostId).hostname
            if hostname == nodename:
                config = node.get_config()
                try:
                    log_dirs = config['log.dirs']
                except KeyError:
                    error = "No log dirs exists."
                    log_dirs = ''
                if action == 'remove':
                    if log_dir in log_dirs:
                        new_log_dirs = log_dirs.replace(log_dir, '').replace(',,', ',').strip(',')
                    else:
                        return 0, 'Log dir {0} is not in a config.'.format(log_dir)
                elif action == 'add':
                    if log_dir not in log_dirs:
                        new_log_dirs = log_dirs + ',' + log_dir.replace(',,', ',').strip(',')
                    else:
                        return 0, 'Log dir {0} is already in a config.'.format(log_dir)
                else:
                    return 2, 'Error: unknown action {0}'.format(action)
                new_config = config
                new_config['log.dirs'] = new_log_dirs
                try:
                    node.update_config(new_config)
                except ApiException as e:
                    return 1, 'Error: {0}'.format(e)
                else:
                    return 0, 'Broker config updated.'

    def get_role_types(self, service_name):
        """
        Methos gets all service's role names, that could be
        used in get_host_by_role method.

        Args:
            service_name(str): Name of service that runs under cloduera manager.

        Returns:
            all_roles(dict): Dictionary with all available role names.
        :param service_name:
        :return:
        """

        cluster = self.api.get_cluster(self.cluster)
        service = cluster.get_service(service_name)
        all_roles = service.get_role_types()
        return all_roles


if __name__ == '__main__':
    c = NiagaraCMApi(cm_host='s-niagcm01.nj01.303net.pvt', user='efazliev', password='IAMKolossus4')
    c.get_kafka_broker_id_by_hostname('s-niagara04.nj01.303net.pvt')
    print (
       'This module should not be called directly. '
       'Please install it via pip use apicm-help to get list of available commands.'
    )
