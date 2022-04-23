# -*- coding: utf-8 -*-
import atexit
import hashlib
import os
import shutil
import tempfile
from typing import TYPE_CHECKING, Any, Dict, Iterable, Mapping, Optional, Union

from kiara.models import KiaraModel
from pydantic import Field, PrivateAttr, validator

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine
    from sqlalchemy.engine.reflection import Inspector
    from sqlalchemy.sql.elements import TextClause


class KiaraDatabase(KiaraModel):
    @classmethod
    def create_in_temp_dir(
        cls,
        init_statement: Union[None, str, "TextClause"] = None,
        init_data: Optional[Mapping[str, Any]] = None,
    ):

        temp_f = tempfile.mkdtemp()
        db_path = os.path.join(temp_f, "db.sqlite")

        def cleanup():
            shutil.rmtree(db_path, ignore_errors=True)

        atexit.register(cleanup)

        db = cls(db_file_path=db_path)
        db.create_if_not_exists()

        if init_statement:
            db._unlock_db()
            db.execute_sql(statement=init_statement, data=init_data, invalidate=True)
            db._lock_db()

        return db

    db_file_path: str = Field(description="The path to the sqlite database file.")

    _cached_engine = PrivateAttr(default=None)
    _cached_inspector = PrivateAttr(default=None)
    _table_names = PrivateAttr(default=None)
    _table_schemas = PrivateAttr(default=None)
    _file_hash: Optional[str] = PrivateAttr(default=None)
    _lock: bool = PrivateAttr(default=True)
    _immutable: bool = PrivateAttr(default=None)

    def _retrieve_id(self) -> str:
        return self.file_hash

    def _retrieve_category_id(self) -> str:
        return "instance.database"

    def _retrieve_data_to_hash(self) -> Any:
        return {
            "file_hash": self.file_hash,
        }

    @validator("db_file_path", allow_reuse=True)
    def ensure_absolute_path(cls, path: str):

        path = os.path.abspath(path)
        if not os.path.exists(os.path.dirname(path)):
            raise ValueError(f"Parent folder for database file does not exist: {path}")
        return path

    @property
    def db_url(self) -> str:
        return f"sqlite:///{self.db_file_path}"

    @property
    def file_hash(self) -> str:

        if self._file_hash is not None:
            return self._file_hash

        sha256_hash = hashlib.sha3_256()
        with open(self.db_file_path, "rb") as f:
            # Read and update hash string value in blocks of 4K
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)

        self._file_hash = sha256_hash.hexdigest()
        return self._file_hash

    def get_sqlalchemy_engine(self) -> "Engine":

        if self._cached_engine is not None:
            return self._cached_engine

        from sqlalchemy import create_engine

        def _pragma_on_connect(dbapi_con, con_record):
            dbapi_con.execute("PRAGMA query_only = ON")

        self._cached_engine = create_engine(self.db_url, future=True)

        if self._lock:
            from sqlalchemy import event

            event.listen(self._cached_engine, "connect", _pragma_on_connect)

        return self._cached_engine

    def _lock_db(self):
        self._lock = True
        self._invalidate()

    def _unlock_db(self):
        if self._immutable:
            raise Exception("Can't unlock db, it's immutable.")
        self._lock = False
        self._invalidate()

    def create_if_not_exists(self):

        from sqlalchemy_utils import create_database, database_exists

        if not database_exists(self.db_url):
            create_database(self.db_url)

    def execute_sql(
        self,
        statement: Union[str, "TextClause"],
        data: Optional[Mapping[str, Any]] = None,
        invalidate: bool = False,
    ):
        """Execute an sql script.

        Arguments:
          statement: the sql statement
          data: (optional) data, to be bound to the statement
          invalidate: whether to invalidate cached values within this object
        """

        from sqlalchemy import text

        if isinstance(statement, str):
            statement = text(statement)

        if data:
            statement.bindparams(**data)

        self.create_if_not_exists()
        conn = self.get_sqlalchemy_engine().raw_connection()
        conn.execute(statement)

        conn.commit()
        conn.close()

        if invalidate:
            self._invalidate()

    def _invalidate(self):
        self._cached_engine = None
        self._cached_inspector = None
        self._table_names = None
        self._table_schemas = None
        self._file_hash = None

    def copy_database_file(self, target: str):

        os.makedirs(os.path.dirname(target))

        shutil.copy2(self.db_file_path, target)

        new_db = KiaraDatabase(db_file_path=target)
        if self._file_hash:
            new_db._file_hash = self._file_hash
        return new_db

    def get_sqlalchemy_inspector(self) -> "Inspector":

        if self._cached_inspector is not None:
            return self._cached_inspector

        from sqlalchemy.inspection import inspect

        self._cached_inspector = inspect(self.get_sqlalchemy_engine())
        return self._cached_inspector

    @property
    def table_names(self) -> Iterable[str]:
        if self._table_names is not None:
            return self._table_names

        self._table_names = self.get_sqlalchemy_inspector().get_table_names()
        return self._table_names

    def get_schema_for_table(self, table_name: str):

        if self._table_schemas is not None:
            if table_name not in self._table_schemas.keys():
                raise Exception(
                    f"Can't get table schema, database does not contain table with name '{table_name}'."
                )
            return self._table_schemas[table_name]

        ts: Dict[str, Dict[str, Any]] = {}
        inspector = self.get_sqlalchemy_inspector()
        for tn in inspector.get_table_names():
            columns = self.get_sqlalchemy_inspector().get_columns(tn)
            ts[tn] = {}
            for c in columns:
                ts[tn][c["name"]] = c

        self._table_schemas = ts
        if table_name not in self._table_schemas.keys():
            raise Exception(
                f"Can't get table schema, database does not contain table with name '{table_name}'."
            )

        return self._table_schemas[table_name]
