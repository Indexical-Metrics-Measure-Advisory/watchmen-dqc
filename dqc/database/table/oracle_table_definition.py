from sqlalchemy import MetaData, Table, Column, String, CLOB, Date, Boolean

metadata = MetaData()


def get_primary_key(table_name):
    pid = get_pid(table_name)
    return pid


def get_pid(table_name):
    if table_name == 'monitor_rules':
        pid = 'ruleid'
    elif table_name == 'catalogs':
        pid = 'catalogid'
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

catalogs = Table("catalogs", metadata,
                 Column('catalogid', String(60), primary_key=True),
                 Column('name', String(45), nullable=False),
                 Column('topicids', CLOB, nullable=True),
                 Column('techownerid', String(60), nullable=True),
                 Column('bizownerid', String(60), nullable=True),
                 Column('tags', CLOB, nullable=True),
                 Column('description', String(200), nullable=True),
                 Column('tenantid', String(60), nullable=False),
                 Column('createtime', String(50), nullable=True),
                 Column('lastmodified', Date, nullable=True)
                 )


def get_table_by_name(table_name):
    if table_name == "monitor_rules":
        return monitor_rules
    elif table_name == "catalogs":
        return catalogs
