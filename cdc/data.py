import io

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from pydantic import BaseModel, ConfigDict
from typing import Optional, Any


class EnumOp(str, Enum):
    INSERT = 'I'
    BEFORE = 'B'
    UPDATE = 'U'
    DELETE = 'D'
    TRUNCATE = 'T'


class Column(BaseModel):
    pk: bool
    name: str
    dtype_oid: int
    dtype: str
    is_nullable: bool


class Table(BaseModel):
    db: str
    tschema: str  # 'schema' is a reserved keyword by pydantic
    name: str
    oid: int
    columns: list[Column]


class Transaction(BaseModel):
    lsn: int
    commit_ts: datetime
    xid: int


class ReplicationMessage(BaseModel):
    data_start: int  # Begin LSN
    payload: Optional[bytes] = None
    send_time: datetime
    data_size: int
    wal_end: int


class TransactionEvent(BaseModel):
    op: EnumOp
    replication_msg: ReplicationMessage
    transaction: Transaction
    table: Table
    data: Optional[dict[str, Any]]


class FileDescriptor(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)  # Prevent io.TextIOWrapper from throwing pydantic error

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
