# -*- coding: utf-8 -*-
__author__ = "pav0n"

import re
import time
from .._globals import IDENTITY
from ..helpers.methods import varquote_aux
from .base import NoSQLAdapter
from .._load import PlainTextAuthProvider,web2py_uuid
from .._load import ConsistencyLevel
from .._load import SimpleStatement
from ..helpers.methods import uuid2int

TIMINGSSIZE = 100

class CassandraAdapter(NoSQLAdapter):
    drivers = ('cassandra',)

    commit_on_alter_table = True
    support_distributed_transaction = True
    types = {
        'boolean': 'boolean',
        'string': 'VARCHAR',
        'text': 'TEXT',
        'json': 'TEXT',
        'password': 'VARCHAR',
        'blob': 'BLOB',
        'upload': 'VARCHAR',
        'integer': 'INT',
        'bigint': 'BIGINT',
        'float': 'FLOAT',
        'double': 'DOUBLE',
        'decimal': 'DECIMAL',
        'date': 'TIMESTAMP',
        'time': 'TIMESTAMP',
        'datetime': 'TIMESTAMP',
        'id': 'INT PRIMARY KEY',
        'reference': 'BIGINT',
        'list:integer': 'TEXT',
        'list:string': 'TEXT',
        'list:reference': 'TEXT',
        'big-id': 'BIGINT',
        'big-reference': 'BIGINT'
    }

    QUOTE_TEMPLATE = "`%s`"


    REGEX_URI = re.compile('^(?P<user>[^:@]+)(\:(?P<password>[^@]*))?@(?P<host>[^\:/]+)(\:(?P<port>[0-9]+))?/(?P<db>[^?]+)(\?set_encoding=(?P<charset>\w+))?$')

    def create_sequence_and_triggers(self, query, table, **args):
        # following lines should only be executed if table._sequence_name does not exist
        # self.execute('CREATE SEQUENCE %s;' % table._sequence_name)
        # self.execute("ALTER TABLE %s ALTER COLUMN %s SET DEFAULT NEXTVAL('%s');" \
        #              % (table._tablename, table._fieldname, table._sequence_name))
        self.execute(query)

    def __init__(self,db,uri,pool_size=0,folder=None,db_codec ='UTF-8',
                 credential_decoder=IDENTITY, driver_args={},
                 adapter_args={}, do_connect=True, after_connection=None):
        self.db = db
        self.dbengine = "cassandra"
        self.uri = uri
        if do_connect: self.find_driver(adapter_args,uri)
        self.pool_size = pool_size
        self.folder = folder
        self.db_codec = db_codec
        self._after_connection = after_connection
        self.find_or_make_work_folder()
        import random
        self.random = random

        ruri = uri.split('://',1)[1]
        m = self.REGEX_URI.match(ruri)
        if not m:
            raise SyntaxError(
                "Invalid URI string in DAL: %s" % self.uri)
        user = credential_decoder(m.group('user'))
        if not user:
            raise SyntaxError('User required')
        password = credential_decoder(m.group('password'))
        if not password:
            password = ''
        host = m.group('host')
        if not host:
            raise SyntaxError('Host name required')
        db = m.group('db')
        if not db:
            raise SyntaxError('Database name required')
        port = int(m.group('port') or '9042')
        charset = m.group('charset') or 'utf8'
        auth_provider = PlainTextAuthProvider(username=credential_decoder(user), password=credential_decoder(password))
        driver_args.update(auth_provider=auth_provider,
                           protocol_version=2,
                           contact_points=[host],
                           port=port)

        def connector(driver_args=driver_args,keyspace=db):
            print driver_args
            cluster = self.driver(**driver_args)
            session = cluster.connect(keyspace)
            session.rollback = lambda : ''
            session.commit = lambda : ''
            session.cursor = lambda : session
            #query = SimpleStatement("INSERT INTO users (name, age) VALUES (%s, %s)",consistency_level=ConsistencyLevel.QUORUM)
            #session.execute('create table emp_test (empid int primary key, emp_first varchar, emp_last varchar, emp_dept varchar)')
            return  session
        self.connector = connector
        if do_connect: self.reconnect(f=connector)

    def after_connection(self):
        print 'test'
    def lastrowid(self,table):
        self.execute('select last_insert_id();')
        return int(self.cursor.fetchone()[0])
    def execute(self, command, *a, **b):
        print "command %s " %command
        return self.log_execute(command.decode('utf8'), *a, **b)

    def log_execute(self, *a, **b):
        if not self.connection: raise ValueError(a[0])
        if not self.connection: return None
        command = a[0]
        if hasattr(self,'filter_sql_command'):
            command = self.filter_sql_command(command)
        if self.db._debug:
            LOGGER.debug('SQL: %s' % command)
        self.db._lastsql = command
        t0 = time.time()
        ret = self.cursor.execute(command, *a[1:], **b)
        self.db._timings.append((command,time.time()-t0))
        del self.db._timings[:-TIMINGSSIZE]
        return ret

    def insert(self, table, fields, safe=None):
        """Safe determines whether a asynchronous request is done or a
        synchronous action is done
        For safety, we use by default synchronous requests"""
        print "imprimiendo id %s" %table.id
        print "imprimiendo id %s" %table._id
        try:
            id = uuid2int(web2py_uuid())
            values = dict((k.name,self.represent(v,k.type)) for k,v in fields)
            values['_id'] = id

            query = SimpleStatement(self._insert(table,values),consistency_level=ConsistencyLevel.QUORUM)
            session.execute(query)
        except Exception,e:
            print 'imprimiendo el Error %s'%e
        print 'imprimiendo el id : %s'%id
        return id




    def _insert(self, table, fields):
        table_rname = table.sqlsafe
        if fields:
            print 'estos son los fields'
            print fields
            keys = ','.join(f.name for f, v in fields)
            values = ','.join(self.expand(v, f.type) for f, v in fields)
            print '######################################################################'
            print 'insertando cosas'
            print '######################################################################'
            print 'INSERT INTO %s(%s) VALUES (%s);' % (table_rname, keys, values)
            return 'INSERT INTO %s(%s) VALUES (%s);' % (table_rname, keys, values)
        else:
            return self._insert_empty(table)
