class TableDefinition(object):

    def get_primary_key(self, table_name):
        pid = self.get_pid(table_name)
        return pid

    def get_pid(self, table_name):
        if table_name == 'monitor_rules':
            pid = 'ruleid'
        return pid
