from sqlalchemy import MetaData, Table, Column, String, CLOB, Date, Boolean

metadata = MetaData()


def get_primary_key(table_name):
    pid = get_pid(table_name)
    return pid


def get_pid(table_name):
    if table_name == 'monitor_rules':
        pid = 'ruleid'
    return pid


monitor_rules = Table("monitor_rules", metadata,
                           Column('ruleid', String(60), primary_key=True),
                           Column('code', String(45), nullable=False),
                           Column('grade', String(45), nullable=True),
                           Column('severity', String(45), nullable=True),
                           Column('enabled', Boolean, nullable=True),
                           Column('topicid', String(45), nullable=True),
                           Column('factorid', String(45), nullable=True),
                           Column('tenantid', String(60), nullable=False),
                           Column('params', CLOB, nullable=True),
                           Column('createtime', String(50), nullable=True),
                           Column('lastmodified', Date, nullable=True)
                           )


def get_table_by_name(table_name):
    if table_name == "monitor_rules":
        table = monitor_rules

    return table
