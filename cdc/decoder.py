import psycopg2
import psycopg2.extras

from abc import ABC, abstractmethod
from datetime import datetime, timedelta, timezone
from io import BytesIO
from loguru import logger
from typing import Optional

from data import (
    Column,
    Datum,
    EnumOp,
    RelationColumn,
    ReplicationMessage,
    Table,
    Transaction,
    TransactionEvent,
    TupleData,
)

PG_EPOCH = datetime(2000, 1, 1, tzinfo=timezone.utc)

MAP__PG_DTYPE__PY_DTYPE = {  # Other than this is considered 'str'
    'bigint': int,
    'integer': int,
    'smallint': int,
    'timestamp with time zone': str,
    'timestamp without time zone': str,
    'numeric': float,
    'double precision': float,
    'bool': bool,
}


def convert_ts_to_datetime(ts_ms: int) -> datetime: return PG_EPOCH + timedelta(microseconds=ts_ms)  # PostgreSQL epoch is 2000-01-01


def convert_bytes_to_int(input_bytes: bytes) -> int: return int.from_bytes(input_bytes, byteorder='big', signed=True)  # Big-endian, signed integer


def convert_bytes_to_utf8(input_bytes: bytes) -> str: return input_bytes.decode('utf-8')  # UTF-8


def json_serializer(obj):
    if isinstance(obj, (datetime,)):
        return obj.isoformat()
    raise TypeError(f'Type \'{type(obj)}\' not serializable')


class ReplicationMessagePayload(ABC):
    """
    Provides basic functionality to read logical replication message components
    """

    def __init__(self, payload: bytes) -> None:
        self.buffer = BytesIO(payload)  # Convert into serial-readable buffer
        self.byte1 = self.read_utf8()  # Read the first byte to determine the message type

        self.decode_buffer()  # Begin decoding

    @abstractmethod
    def decode_buffer(self) -> None: pass

    # Each of these functions represents different data types from a logical replication message components
    # https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html

    def read_utf8(self, n=1) -> str: return convert_bytes_to_utf8(self.buffer.read(n))

    def read_int8(self) -> int: return convert_bytes_to_int(self.buffer.read(1))  # Int8 = 1 byte

    def read_int16(self) -> int: return convert_bytes_to_int(self.buffer.read(2))  # Int16 = 2 bytes

    def read_int32(self) -> int: return convert_bytes_to_int(self.buffer.read(4))  # Int32 = 4 bytes

    def read_int64(self) -> int: return convert_bytes_to_int(self.buffer.read(8))  # Int64 = 8 bytes

    def read_timestamp(self) -> datetime: return convert_ts_to_datetime(self.read_int64())  # Timestamp is an Int64

    def read_string(self) -> str:
        output = bytearray()
        while (next_char := self.buffer.read(1)) != b'\x00':  # Read until NULL character
            output += next_char
        return convert_bytes_to_utf8(output)

    def read_tuple_data(self) -> str:  # https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html#PROTOCOL-LOGICALREP-MESSAGE-FORMATS-TUPLEDATA
        n_columns = self.read_int16()

        data = []
        for _ in range(n_columns):
            category = self.read_utf8()

            # Null, TOASTed
            if category in {'n', 'u'}:
                data.append(Datum(category=category))
            # Text
            elif category == 't':
                column_length = self.read_int32()
                column_value = self.read_utf8(column_length)
                data.append(Datum(category=category, length=column_length, value=column_value))

        return TupleData(n_columns=n_columns, data=data)


class Relation(ReplicationMessagePayload):  # https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html#PROTOCOL-LOGICALREP-MESSAGE-FORMATS-RELATION
    oid: int
    schema: str
    name: str
    replica_identity: int
    n_columns: int
    columns: list[RelationColumn]

    def decode_buffer(self) -> None:
        if self.byte1 != 'R':
            raise ValueError(f'Invalid message type for {Relation.__name__}: \'{self.byte1}\'')

        self.oid = self.read_int32()
        self.schema = self.read_string()  # Table schema
        self.name = self.read_string()  # Table name
        self.replica_identity = self.read_int8()
        self.n_columns = self.read_int16()
        self.columns = []

        for _ in range(self.n_columns):
            self.columns.append(RelationColumn(
                pk=self.read_int8(),
                name=self.read_string(),  # Column name
                dtype_oid=self.read_int32(),  # Column data type (OID)
                atttypmod=self.read_int32(),
            ))


class Begin(ReplicationMessagePayload):  # https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html#PROTOCOL-LOGICALREP-MESSAGE-FORMATS-BEGIN
    lsn: int
    commit_ts: int
    transaction_xid: int

    def decode_buffer(self) -> None:
        if self.byte1 != 'B':
            raise ValueError(f'Invalid message type for {Begin.__name__}: \'{self.byte1}\'')

        self.lsn = self.read_int64()
        self.commit_ts = self.read_timestamp()
        self.transaction_xid = self.read_int64()


class Insert(ReplicationMessagePayload):  # https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html#PROTOCOL-LOGICALREP-MESSAGE-FORMATS-INSERT
    relation_oid: int
    new_tuple_byte: int
    new_tuple: TupleData

    def decode_buffer(self) -> None:
        if self.byte1 != 'I':
            raise ValueError(f'Invalid message type for {Insert.__name__}: \'{self.byte1}\'')

        self.relation_oid = self.read_int32()
        self.new_tuple_byte = self.read_utf8()
        self.new_tuple = self.read_tuple_data()


class Update(ReplicationMessagePayload):  # https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html#PROTOCOL-LOGICALREP-MESSAGE-FORMATS-UPDATE
    relation_oid: int
    old_tuple_byte: Optional[int] = None
    old_tuple: Optional[TupleData] = None
    new_tuple_byte: int
    new_tuple: TupleData

    def decode_buffer(self) -> None:
        if self.byte1 != 'U':
            raise ValueError(f'Invalid message type for {Update.__name__}: \'{self.byte1}\'')

        self.relation_oid = self.read_int32()

        submessage_byte = self.read_utf8()
        if submessage_byte in {'K', 'O'}:
            self.old_tuple_byte = submessage_byte
            self.old_tuple = self.read_tuple_data()
        elif submessage_byte == 'N':
            self.new_tuple_byte = submessage_byte
            self.new_tuple = self.read_tuple_data()
        else:
            raise ValueError(f'Invalid submessage type for {Update.__name__}: \'{submessage_byte}\'')


class Delete(ReplicationMessagePayload):  # https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html#PROTOCOL-LOGICALREP-MESSAGE-FORMATS-DELETE
    relation_oid: int
    old_tuple_byte: int
    old_tuple: TupleData

    def decode_buffer(self) -> None:
        if self.byte1 != 'D':
            raise ValueError(f'Invalid message type for {Delete.__name__}: \'{self.byte1}\'')

        self.relation_oid = self.read_int32()
        self.old_tuple_byte = self.read_utf8()
        if self.old_tuple_byte not in {'K', 'O'}:
            raise ValueError(f'Invalid tuple type for {Delete.__name__}: \'{self.old_tuple_byte}\'')
        self.old_tuple = self.read_tuple_data()


class Truncate(ReplicationMessagePayload):  # https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html#PROTOCOL-LOGICALREP-MESSAGE-FORMATS-TRUNCATE
    n_relations: int
    truncate_option: int
    relation_oids: list[int]

    def decode_buffer(self) -> None:
        if self.byte1 != 'T':
            raise ValueError(f'Invalid message type for {Truncate.__name__}: \'{self.byte1}\'')

        self.n_relations = self.read_int32()
        self.truncate_option = self.read_int8()
        self.relation_oids = []

        for _ in range(self.n_relations):
            self.relation_oids.append(self.read_int32())


class Decoder:
    def __init__(self, dsn: str) -> None:
        self.transaction = None

        self.map__dtype_oid__dtype = {}  # { oid: dtype }
        self.map__relation_oid__table: dict[int, Table] = {}  # { oid: PgTable }

        self.conn = psycopg2.connect(dsn)
        self.cursor = self.conn.cursor()
        self.db_name = self.conn.get_dsn_parameters()["dbname"]
        logger.debug(f'Connected to db: {self.db_name} (Decoder)')

    def fetch_oid_dtype(self, oid: int, atttypemod: int) -> str:
        self.cursor.execute(f'SELECT FORMAT_TYPE({oid}, {atttypemod}) AS fmt')
        return self.cursor.fetchone()[0]

    def fetch_columns_nullable(self, table_schema: str, table_name: str, column_names: list[str]) -> dict:
        column_names_str = ', '.join([f'\'{column_name}\'' for column_name in column_names])
        self.cursor.execute(f'SELECT attname AS column_name, attnotnull AS nullable FROM pg_attribute WHERE attrelid = \'{table_schema}.{table_name}\'::regclass AND attname IN ({column_names_str})')
        return {column_name: (False if attnotnull else True) for column_name, attnotnull in self.cursor.fetchall()}

    def convert_tuple_data_to_py_data(self, table: Table, tuple_data: TupleData, is_pk_only: bool = False) -> dict:
        if not is_pk_only:
            return {column.name: MAP__PG_DTYPE__PY_DTYPE.get(column.dtype, str)(datum.value) for column, datum in zip(table.columns, tuple_data.data)}
        else:
            table_columns = [column for column in table.columns if column.pk]
            return {column.name: MAP__PG_DTYPE__PY_DTYPE.get(column.dtype, str)(datum.value) for column, datum in zip(table_columns, tuple_data.data) if column.pk}

    def decode(self, msg: ReplicationMessage) -> TransactionEvent | list[TransactionEvent]:
        msg_type = msg.payload[:1].decode('utf-8')
        match msg_type:
            case 'R':
                self.decode_message_relation(msg)
            case 'B':
                if self.transaction:
                    raise ValueError(f'Previous transaction not closed: {self.transaction}')
                self.transaction = self.decode_begin(msg)  # Denotes beginning of a transaction
            case 'I':
                if not self.transaction:
                    raise ValueError(f'No transaction found for message: {msg}')
                return self.decode_insert(msg)
            case 'U':
                if not self.transaction:
                    raise ValueError(f'No transaction found for message: {msg}')
                return self.decode_update(msg)
            case 'D':
                if not self.transaction:
                    raise ValueError(f'No transaction found for message: {msg}')
                return self.decode_delete(msg)
            case 'T':
                if not self.transaction:
                    raise ValueError(f'No transaction found for message: {msg}')
                return self.decode_truncate(msg)
            case 'C':
                self.transaction = None  # Denotes end of a transaction
            case _:
                return

    def decode_message_relation(self, msg: ReplicationMessage) -> None:
        relation = Relation(msg.payload)
        msg.payload = None

        # Translate table (relation)
        if relation.oid not in self.map__relation_oid__table:
            self.map__relation_oid__table[relation.oid] = Table(
                db=self.db_name,
                tschema=relation.schema,
                name=relation.name,
                oid=relation.oid,
                columns=[],
            )

        map__relation_column__column = {column.name: column for column in self.map__relation_oid__table[relation.oid].columns}

        # Translate columns
        existing_column_names = {column.name for column in self.map__relation_oid__table[relation.oid].columns}
        new_column_names = {column.name for column in relation.columns if column.name not in existing_column_names}
        if new_column_names:
            map__column_name__nullable = self.fetch_columns_nullable(self.map__relation_oid__table[relation.oid].tschema, self.map__relation_oid__table[relation.oid].name, new_column_names)

            for relation_column in relation.columns:
                if relation_column in map__relation_column__column:
                    continue

                # Specify data type
                if relation_column.dtype_oid not in self.map__dtype_oid__dtype:
                    self.map__dtype_oid__dtype[relation_column.dtype_oid] = self.fetch_oid_dtype(relation_column.dtype_oid, relation_column.atttypmod)

                # Update table columns
                self.map__relation_oid__table[relation.oid].columns.append(Column(
                    pk=relation_column.pk,
                    name=relation_column.name,
                    dtype_oid=relation_column.dtype_oid,
                    dtype=self.map__dtype_oid__dtype[relation_column.dtype_oid],
                    is_nullable=map__column_name__nullable[relation_column.name],
                ))

        # Validate number of columns
        if len(relation.columns) != len(self.map__relation_oid__table[relation.oid].columns):
            raise ValueError(f'Number of columns mismatch for relation \'{relation.oid}\', left side: {relation.columns}, right side: {self.map__relation_oid__table[relation.oid].columns}')

    def decode_begin(self, msg: ReplicationMessage) -> Transaction:
        begin = Begin(msg.payload)
        msg.payload = None
        return Transaction(
            lsn=begin.lsn,
            commit_ts=begin.commit_ts,
            xid=begin.transaction_xid,
        )

    def decode_insert(self, msg: ReplicationMessage) -> TransactionEvent:
        insert = Insert(msg.payload)
        msg.payload = None
        return TransactionEvent(
            op=insert.byte1,  # Must be 'I'
            replication_msg=msg,
            transaction=self.transaction,
            table=self.map__relation_oid__table[insert.relation_oid],
            data=self.convert_tuple_data_to_py_data(self.map__relation_oid__table[insert.relation_oid], insert.new_tuple),
        )

    def decode_update(self, msg: ReplicationMessage) -> list[TransactionEvent]:
        update = Update(msg.payload)
        transaction_events = []
        msg.payload = None
        if update.old_tuple is not None:
            transaction_events.append(TransactionEvent(
                op=EnumOp.BEFORE,  # Hardcoded into 'B' for 'Before'
                replication_msg=msg,
                transaction=self.transaction,
                table=self.map__relation_oid__table[update.relation_oid],
                data=self.convert_tuple_data_to_py_data(self.map__relation_oid__table[update.relation_oid], update.old_tuple, is_pk_only=update.old_tuple_byte == 'K'),
            ))

        transaction_events.append(TransactionEvent(
            op=update.byte1,  # Must be 'U'
            replication_msg=msg,
            transaction=self.transaction,
            table=self.map__relation_oid__table[update.relation_oid],
            data=self.convert_tuple_data_to_py_data(self.map__relation_oid__table[update.relation_oid], update.new_tuple),
        ))

        return transaction_events

    def decode_delete(self, msg: ReplicationMessage) -> TransactionEvent:
        delete = Delete(msg.payload)
        msg.payload = None
        return TransactionEvent(
            op=delete.byte1,  # Must be 'D'
            replication_msg=msg,
            transaction=self.transaction,
            table=self.map__relation_oid__table[delete.relation_oid],
            data=self.convert_tuple_data_to_py_data(self.map__relation_oid__table[delete.relation_oid], delete.old_tuple, is_pk_only=delete.old_tuple_byte == 'K'),
        )

    def decode_truncate(self, msg: ReplicationMessage) -> list[TransactionEvent]:
        truncate = Truncate(msg.payload)
        msg.payload = None
        return [TransactionEvent(
            op=truncate.byte1,  # Must be 'T'
            replication_msg=msg,
            transaction=self.transaction,
            table=self.map__relation_oid__table[relation_oid],
            data=None,
        ) for relation_oid in truncate.relation_oids]
