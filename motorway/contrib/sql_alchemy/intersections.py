from sqlalchemy import MetaData, Table, create_engine, Integer, Column, select, bindparam
from sqlalchemy.exc import NoSuchTableError
from motorway.decorators import batch_process
from motorway.intersection import Intersection


class DatabaseInsertIntersection(Intersection):
    """
    Excepts messages with a single dictionary where the key is the field name in the database and the value is .. value
    """
    database_uri = None
    schema = 'public'
    table = None
    create_table = True
    add_columns = False
    batch_size = 100
    upsert = True
    table_columns = [
        {'name': 'id', 'type': Integer, 'primary_key': True}
    ]

    debug_sql_statements = False

    def __init__(self, *args, **kwargs):
        self.engine = create_engine(self.database_uri, echo=self.debug_sql_statements)
        self.metadata = MetaData(bind=self.engine)
        self.connection = self.engine.connect()
        try:
            self.table = Table(self.table, self.metadata, autoload=True, schema=self.schema)
        except NoSuchTableError:
            if not self.create_table:
                raise
            else:
                self.table = Table(self.table, self.metadata,
                    *[
                        Column(column['name'], column['type'], primary_key=column.get('primary_key', False))
                    for column in self.table_columns],
                schema = self.schema)
                self.table.create(self.engine)

        super(DatabaseInsertIntersection, self).__init__(*args, **kwargs)

    @staticmethod
    def _column_name(name):
        return 'b_%s' % name

    @staticmethod
    def bind_alias(dct):
        return {DatabaseInsertIntersection._column_name(key): value for key, value in dct.items()}

    @batch_process(limit=batch_size)  # todo: make this work with inheritance
    def process(self, messages):
        if not self.upsert:  # Just insert
            self.connection.execute(self.table.insert(), [
                message.content for message in messages
            ])
        else:  # "Upsert"
            primary_key = [col['name'] for col in self.table_columns if col.get('primary_key')]
            assert len(primary_key) == 1, "More than one primary key defined"
            primary_key = primary_key[0]  # Assume there is always only one
            pk_field = getattr(self.table.c, primary_key)
            messages_by_pk = {message.content.get(primary_key): message for message in messages}


            ##### GET PKS FOR UPDATE #####
            update_pks = [row[0] for row in self.connection.execute(
                select(
                    [pk_field],
                    pk_field.in_(
                        messages_by_pk.keys()
                    )
                )
            ).fetchall()]

            #### ACTUAL UPDATE OF NON-PK FIELDS IN ROWS ####
            if len(self.table_columns) > 1:  # No need to update if PK is the only column, in which case its already correct

                stmt = self.table.update().where(
                    pk_field == bindparam(self._column_name(primary_key))
                ).values(
                    {
                        col['name']: bindparam(self._column_name(col['name'])) for col in self.table_columns if not col.get('primary_key')
                    }
                )


                if update_pks:
                    self.connection.execute(stmt, [
                        self.bind_alias(message.content) for message_pk, message in messages_by_pk.items() if message_pk in update_pks
                    ])

            #### BULK INSERT ####

            # Compare number of returned existing rows with unique rows
            if not len(update_pks) == len(messages_by_pk.keys()):
                self.connection.execute(self.table.insert(), [
                    message.content for message_pk, message in messages_by_pk.items() if message_pk not in update_pks
                ])

        for message in messages:
            self.ack(message)

        yield