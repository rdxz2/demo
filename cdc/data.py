import io

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from pydantic import BaseModel, ConfigDict, Field
from typing import Optional, Any


class EnumOp(str, Enum):
    INSERT = 'I'
    BEFORE = 'B'
    UPDATE = 'U'
    DELETE = 'D'
    TRUNCATE = 'T'


class PgcColumn(BaseModel):
    pk: bool
    name: str
    dtype_oid: int
    dtype: str
    bq_dtype: str
    proto_dtype: str
    is_nullable: bool
    ordinal_position: int


class PgTable(BaseModel):
    db: str
    tschema: str = Field(..., alias='schema')  # 'schema' is a reserved keyword by pydantic
    name: str
    oid: int
    columns: list[PgcColumn]

    fqn: str
    proto_classname: str
    proto_filename: str

    def __init__(self, **kwargs):
        kwargs['fqn'] = f'{kwargs["db"]}.{kwargs["schema"]}.{kwargs["name"]}'
        kwargs['proto_classname'] = f'{kwargs["db"].capitalize()}{kwargs["schema"].capitalize()}{kwargs["name"].capitalize()}'  # db.schema.table -> DbSchemaTable
        kwargs['proto_filename'] = f'{kwargs["db"]}_{kwargs["schema"]}_{kwargs["name"]}'
        super().__init__(**kwargs)


class Transaction(BaseModel):
    lsn: int
    commit_ts: datetime
    xid: int


class ReplicationMessage(BaseModel):  # The original replication message
    data_start: int  # Begin LSN
    payload: Optional[bytes] = None
    send_time: datetime
    data_size: int
    wal_end: int


class TransactionEvent(BaseModel):  # Decoded replication message
    op: EnumOp
    replication_msg: ReplicationMessage
    transaction: Transaction
    table: PgTable
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


# Uploader specifics


class BqColumn(BaseModel):
    name: str
    dtype: str


class BqTable(BaseModel):
    name: str
    columns: list[BqColumn] = []

    def __eq__(self, other: 'BqTable') -> bool:
        if not isinstance(other, BqTable):
            return False
        return self.name == other.name and len({''.join(map(str, x.__dict__.values())) for x in self.columns} - {''.join(map(str, x.__dict__.values())) for x in other.columns}) == 0
