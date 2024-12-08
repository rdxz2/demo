import io

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional, Any


class EnumOp(str, Enum):
    INSERT = 'I'
    BEFORE = 'B'
    UPDATE = 'U'
    DELETE = 'D'
    TRUNCATE = 'T'


@dataclass
class PgcColumn:
    # pk: bool
    name: str
    # dtype_oid: int
    dtype: str
    bq_dtype: str
    proto_dtype: str
    # is_nullable: bool
    # ordinal_position: int


@dataclass
class PgTable:
    db: str
    tschema: str
    name: str
    oid: int
    columns: list[PgcColumn]

    fqn: str
    proto_classname: str
    proto_filename: str

    def __init__(self, **kwargs):
        self.fqn = f'{kwargs["db"]}.{kwargs["schema"]}.{kwargs["name"]}'
        self.proto_classname = f'{kwargs["db"].capitalize()}{kwargs["schema"].capitalize()}{kwargs["name"].capitalize()}'  # db.schema.table -> DbSchemaTable
        self.proto_filename = f'{kwargs["db"]}_{kwargs["schema"]}_{kwargs["name"]}'
        # super().__init__(**kwargs)

        self.db = kwargs['db']
        self.tschema = kwargs['schema']
        self.name = kwargs['name']
        self.oid = kwargs['oid']
        self.columns = kwargs['columns']


@dataclass
class Transaction:
    lsn: int
    commit_ts: datetime
    xid: int


@dataclass
class ReplicationMessage:  # The original replication message
    data_start: int  # Begin LSN
    send_time: datetime
    data_size: int
    wal_end: int
    payload: Optional[bytes] = None


@dataclass
class TransactionEvent:  # Decoded replication message
    op: EnumOp
    replication_msg: ReplicationMessage
    transaction: Transaction
    table: PgTable
    data: Optional[dict[str, Any]]


@dataclass
class FileDescriptor:
    filename: str
    file: io.TextIOWrapper
    size: int


# Decoder specifics

@dataclass(frozen=True)
class Datum:
    category: str
    length: Optional[int] = None
    value: Optional[str] = None


@dataclass(frozen=True)
class TupleData:
    n_columns: int
    data: list[Datum]


@dataclass(frozen=True)
class RelationColumn:
    pk: int
    name: int
    dtype_oid: int
    atttypmod: int


# Uploader specifics


@dataclass
class BqColumn:
    name: str
    dtype: str


@dataclass
class BqTable:
    name: str
    columns: list[BqColumn] = field(default_factory=list)

    def __eq__(self, other: 'BqTable') -> bool:
        if not isinstance(other, BqTable):
            return False
        return self.name == other.name and len({''.join(map(str, x.__dict__.values())) for x in self.columns} - {''.join(map(str, x.__dict__.values())) for x in other.columns}) == 0
