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
class PgColumn:
    name: str
    dtype: str
    bq_dtype: str
    proto_dtype: str


@dataclass
class PgTable:
    columns: list[PgColumn]

    fqn: str
    proto_classname: str
    proto_filename: str


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
    ord: int
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
