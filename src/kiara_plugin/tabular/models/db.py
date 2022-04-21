# -*- coding: utf-8 -*-
import atexit
import os
import shutil
import tempfile
from typing import TYPE_CHECKING, Any, Dict, Iterable, Optional

from kiara.models import KiaraModel
from pydantic import Field, PrivateAttr, validator

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine
    from sqlalchemy.engine.reflection import Inspector


class KiaraDatabase(KiaraModel):
    @classmethod
    def create_in_temp_dir(cls, init_sql: Optional[str] = None):

        temp_f = tempfile.mkdtemp()
        db_path = os.path.join(temp_f, "db.sqlite")

        def cleanup():
            shutil.rmtree(db_path, ignore_errors=True)

        atexit.register(cleanup)

        db = cls(db_file_path=db_path)
        db.create_if_not_exists()

        if init_sql:
            db.execute_sql(sql_script=init_sql, invalidate=True)

        return db

    db_file_path: str = Field(description="The path to the sqlite database file.")
    _cached_engine = PrivateAttr(default=None)
    _cached_inspector = PrivateAttr(default=None)
    _table_names = PrivateAttr(default=None)
    _table_schemas = PrivateAttr(default=None)

    def get_id(self) -> str:
        return self.db_file_path

    def get_category_alias(self) -> str:
        return "instance.metadata.database"

    @validator("db_file_path", allow_reuse=True)
    def ensure_absolute_path(cls, path: str):

        path = os.path.abspath(path)
        if not os.path.exists(os.path.dirname(path)):
            raise ValueError(f"Parent folder for database file does not exist: {path}")
        return path

    @property
    def db_url(self) -> str:
        return f"sqlite:///{self.db_file_path}"

    def get_sqlalchemy_engine(self) -> "Engine":

        if self._cached_engine is not None:
            return self._cached_engine

        from sqlalchemy import create_engine

        self._cached_engine = create_engine(self.db_url, future=True)
        # with self._cached_engine.connect() as con:
        #     con.execute(text("PRAGMA query_only = ON"))

        return self._cached_engine

    def create_if_not_exists(self):

        from sqlalchemy_utils import create_database, database_exists

        if not database_exists(self.db_url):
            create_database(self.db_url)

    def execute_sql(self, sql_script: str, invalidate: bool = False):
        """Execute an sql script.

        Arguments:
          sql_script: the sql script
          invalidate: whether to invalidate cached values within this object
        """

        self.create_if_not_exists()
        conn = self.get_sqlalchemy_engine().raw_connection()
        cursor = conn.cursor()
        cursor.executescript(sql_script)
        conn.commit()
        conn.close()

        if invalidate:
            self._cached_inspector = None
            self._table_names = None
            self._table_schemas = None

    def copy_database_file(self, target: str):

        os.makedirs(os.path.dirname(target))

        shutil.copy2(self.db_file_path, target)

        new_db = KiaraDatabase(db_file_path=target)
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
