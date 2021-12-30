from sqlalchemy import MetaData, Table, Column, String, Date, JSON, Boolean
from watchmen_boot.utils.singleton import singleton

from dqc.database.table.base_table_definition import TableDefinition


@singleton
class MysqlTableDefinition(TableDefinition):

    def __init__(self):
        self.metadata = MetaData()

        self.monitor_rules = Table("monitor_rules", self.metadata,
                                   Column('ruleid', String(60), primary_key=True),
                                   Column('code', String(45), nullable=False),
                                   Column('grade', String(45), nullable=True),
                                   Column('severity', String(45), nullable=True),
                                   Column('enabled', Boolean, nullable=True),
                                   Column('topicid', String(45), nullable=True),
                                   Column('factorid', String(45), nullable=True),
                                   Column('tenantid', String(60), nullable=False),
                                   Column('params', JSON, nullable=True),
                                   Column('createtime', String(50), nullable=True),
                                   Column('lastmodified', Date, nullable=True)
                                   )

        self.catalogs = Table("catalogs", self.metadata,
                                   Column('catalogid', String(60), primary_key=True),
                                   Column('name', String(45), nullable=False),
                                   Column('topicids', JSON, nullable=True),
                                   Column('techownerid', String(60), nullable=True),
                                   Column('bizownerid', String(60), nullable=True),
                                   Column('tags', JSON, nullable=True),
                                   Column('description', String(45), nullable=True),
                                   Column('tenantid', String(60), nullable=False),
                                   Column('createtime', String(60), nullable=True),
                                   Column('lastmodified', Date, nullable=True)
                                   )

    def get_table_by_name(self, table_name):
        return self.get_meta_table(table_name)

    def get_meta_table(self, table_name):

        if table_name == "monitor_rules":
            return self.monitor_rules
        elif table_name =="catalogs":
            return self.catalogs

