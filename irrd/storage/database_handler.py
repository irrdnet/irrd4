import logging
from collections import defaultdict
from datetime import datetime, timezone
from functools import lru_cache
from io import StringIO
from typing import List, Set, Dict, Any, Tuple, Iterator, Union, Optional

import redis
import sqlalchemy as sa
import ujson
from sqlalchemy.dialects import postgresql as pg

from irrd.conf import get_setting
from irrd.rpki.status import RPKIStatus
from irrd.rpsl.parser import RPSLObject
from irrd.rpsl.rpsl_objects import OBJECT_CLASS_MAPPING
from irrd.scopefilter.status import ScopeFilterStatus
from irrd.vendor import postgres_copy
from . import get_engine
from .models import RPSLDatabaseObject, RPSLDatabaseJournal, DatabaseOperation, RPSLDatabaseStatus, \
    ROADatabaseObject, JournalEntryOrigin
from .preload import Preloader
from .queries import (BaseRPSLObjectDatabaseQuery, DatabaseStatusQuery,
                      RPSLDatabaseObjectStatisticsQuery, ROADatabaseObjectQuery)

logger = logging.getLogger(__name__)
MAX_RECORDS_BUFFER_BEFORE_INSERT = 15000
RPSLDatabaseResponse = Iterator[Dict[str, Any]]


class DatabaseHandler:
    """
    Interface for other parts of IRRD to talk to the database.

    Note that no writes to the database are final until commit()
    has been called - and rollback() can be called at any time
    to all submitted changes.
    """
    journaling_enabled: bool
    _transaction: sa.engine.base.Transaction
    _rpsl_pk_source_seen: Set[str]
    # The RPSL upsert buffer is a list of tuples. Each tuple first has a dict
    # with all database column names and their values, and then the origin of the change
    # and the serial of the change at the NRTM source, if any.
    _rpsl_upsert_buffer: List[Tuple[dict, JournalEntryOrigin, Optional[int]]]
    # The ROA insert buffer is a list of dicts with columm names and their values.
    _roa_insert_buffer: List[Dict[str, Union[str, int]]]

    def __init__(self, readonly=False):
        """
        Create a new database handler.

        If readonly is True, this instance will expect read queries only.
        No transaction will be started, all queries will use autocommit.
        """
        self.readonly = readonly
        self.journaling_enabled = not readonly
        self._connection = get_engine().connect()
        if self.readonly:
            self._connection.execution_options(isolation_level="AUTOCOMMIT")
        else:
            self._start_transaction()
            self.preloader = Preloader(enable_queries=False)

    def refresh_connection(self) -> None:
        """
        Refresh the database connection.
        Needed when trying to re-use this instance when the
        SQL server has restarted.
        """
        try:
            if not self.readonly:
                self.rollback(start_transaction=False)
        except Exception:  # pragma: no cover
            pass
        try:
            self.close()
        except Exception:  # pragma: no cover
            pass
        self._connection = get_engine().connect()
        if self.readonly:
            self._connection.execution_options(isolation_level="AUTOCOMMIT")
        else:
            self._start_transaction()

    def _start_transaction(self) -> None:
        """Start a fresh transaction."""
        self._transaction = self._connection.begin()
        self._rpsl_pk_source_seen: Set[str] = set()
        self._rpsl_upsert_buffer = []
        self._roa_insert_buffer = []
        self._object_classes_modified: Set[str] = set()
        self._rpsl_guaranteed_no_existing = True
        self.status_tracker = DatabaseStatusTracker(self, journaling_enabled=self.journaling_enabled)

    def disable_journaling(self):
        self.journaling_enabled = False
        self.status_tracker.journaling_enabled = False

    def enable_journaling(self):
        self.journaling_enabled = True
        self.status_tracker.journaling_enabled = True

    def commit(self) -> None:
        """
        Commit any pending changes to the database and start a fresh transaction.
        """
        self._flush_rpsl_object_writing_buffer()
        self._flush_roa_writing_buffer()
        self.status_tracker.finalise_transaction()
        try:
            self._transaction.commit()
            if self._object_classes_modified:
                self.preloader.signal_reload(self._object_classes_modified)
            self._start_transaction()
        except Exception as exc:  # pragma: no cover
            self._transaction.rollback()
            logger.error('Exception occurred while committing changes, rolling back', exc_info=exc)
            raise

    def rollback(self, start_transaction=True) -> None:
        """Roll back the current transaction, discarding all submitted changes."""
        self._rpsl_upsert_buffer = []
        self._rpsl_pk_source_seen = set()
        self.status_tracker.reset()
        self._transaction.rollback()
        if start_transaction:
            self._start_transaction()

    def execute_query(self,
                      query: Union[BaseRPSLObjectDatabaseQuery, DatabaseStatusQuery, RPSLDatabaseObjectStatisticsQuery, ROADatabaseObjectQuery],
                      flush_rpsl_buffer=True,
                      refresh_on_error=False,
                      ) -> RPSLDatabaseResponse:
        """
        Execute an RPSLDatabaseQuery within the current transaction.
        If flush_rpsl_buffer is set, the RPSL object buffer is flushed first.
        If refresh_on_error is set, if any exception occurs, will refresh
        the connection and retry.
        """
        def execute_query():
            # To be able to query objects that were just created, flush the buffer.
            if not self.readonly and flush_rpsl_buffer:
                self._flush_rpsl_object_writing_buffer()
            statement = query.finalise_statement()
            return self._connection.execute(statement)

        try:
            result = execute_query()
        except Exception as exc:  # pragma: no cover
            if refresh_on_error:
                self.refresh_connection()
                result = execute_query()
            else:
                raise exc

        for row in result.fetchall():
            yield dict(row)
        result.close()

    def execute_statement(self, statement):
        """Execute a raw SQLAlchemy statement, without flushing the upsert buffer."""
        return self._connection.execute(statement)

    def upsert_rpsl_object(self,
                           rpsl_object: RPSLObject,
                           origin: JournalEntryOrigin,
                           rpsl_guaranteed_no_existing=False,
                           source_serial: Optional[int]=None) -> None:
        """
        Schedule an RPSLObject for insertion/updating.

        This method will insert the object, or overwrite an existing object
        if it has the same RPSL primary key and source. No other checks are
        applied before overwriting.

        Writes may not be issued to the database immediately for performance
        reasons, but commit() will ensure all writes are flushed to the DB first.

        The origin indicates the origin of this change, see JournalEntryOrigin
        for the various options. The source_serial is the serial that an NRTM
        source assigned to this change, if any.

        If rpsl_guaranteed_no_existing is set to True, the caller guarantees that this
        PK is unique in the database. This essentially only applies to inserting
        RPKI psuedo-IRR objects.
        """
        if not rpsl_guaranteed_no_existing:
            self._rpsl_guaranteed_no_existing = False
        ip_first = str(rpsl_object.ip_first) if rpsl_object.ip_first else None
        ip_last = str(rpsl_object.ip_last) if rpsl_object.ip_last else None

        ip_size = None
        if rpsl_object.ip_first and rpsl_object.ip_last:
            ip_size = rpsl_object.ip_last.int() - rpsl_object.ip_first.int() + 1

        # In some cases, multiple updates may be submitted for the same object.
        # PostgreSQL will not allow rows proposed for insertion to have duplicate
        # constrained values - so if a second object appears with a pk/source
        # seen before, the buffer must be flushed right away, or the two updates
        # will conflict.
        source = rpsl_object.parsed_data['source']
        rpsl_pk_source = rpsl_object.pk() + '-' + source
        if rpsl_pk_source in self._rpsl_pk_source_seen:
            self._flush_rpsl_object_writing_buffer()

        update_time = datetime.now(timezone.utc)
        object_dict = {
            'rpsl_pk': rpsl_object.pk(),
            'source': source,
            'object_class': rpsl_object.rpsl_object_class,
            'parsed_data': rpsl_object.parsed_data,
            'object_text': rpsl_object.render_rpsl_text(last_modified=update_time),
            'ip_version': rpsl_object.ip_version(),
            'ip_first': ip_first,
            'ip_last': ip_last,
            'ip_size': ip_size,
            'prefix_length': rpsl_object.prefix_length,
            'asn_first': rpsl_object.asn_first,
            'asn_last': rpsl_object.asn_last,
            'rpki_status': rpsl_object.rpki_status,
            'scopefilter_status': rpsl_object.scopefilter_status,
            'updated': update_time,
        }

        self._rpsl_upsert_buffer.append((object_dict, origin, source_serial))

        self._rpsl_pk_source_seen.add(rpsl_pk_source)
        self._object_classes_modified.add(rpsl_object.rpsl_object_class)

        if len(self._rpsl_upsert_buffer) > MAX_RECORDS_BUFFER_BEFORE_INSERT:
            self._flush_rpsl_object_writing_buffer()

    def insert_roa_object(self, ip_version: int, prefix_str: str, asn: int, max_length: int, trust_anchor: str) -> None:
        """
        Schedule a ROA for insertion.

        Writes for ROAs are only issued when commit() is called,
        as they use a single COPY command. This is possible because
        ROA objects are never updated, only inserted.
        """
        self._roa_insert_buffer.append({
            'ip_version': ip_version,
            'prefix': prefix_str,
            'asn': asn,
            'max_length': max_length,
            'trust_anchor': trust_anchor,
        })

    def update_rpki_status(self, rpsl_objs_now_valid: List[Dict[str, str]]=[],
                           rpsl_objs_now_invalid: List[Dict[str, str]]=[],
                           rpsl_objs_now_not_found: List[Dict[str, str]]=[]) -> None:
        """
        Update the RPKI status for the given RPSL PKs.
        Only PKs whose status have changed should be included.

        Objects that moved to or from invalid generate a journal
        entry, so that mirrors follow the (in)visibility depending
        on RPKI status.
        """
        table = RPSLDatabaseObject.__table__
        if rpsl_objs_now_valid:
            pks = {o['rpsl_pk'] for o in rpsl_objs_now_valid}
            stmt = table.update().where(table.c.rpsl_pk.in_(pks)).values(rpki_status=RPKIStatus.valid)
            self.execute_statement(stmt)
        if rpsl_objs_now_invalid:
            pks = {o['rpsl_pk'] for o in rpsl_objs_now_invalid}
            stmt = table.update().where(table.c.rpsl_pk.in_(pks)).values(rpki_status=RPKIStatus.invalid)
            self.execute_statement(stmt)
        if rpsl_objs_now_not_found:
            pks = {o['rpsl_pk'] for o in rpsl_objs_now_not_found}
            stmt = table.update().where(table.c.rpsl_pk.in_(pks)).values(rpki_status=RPKIStatus.not_found)
            self.execute_statement(stmt)

        for rpsl_obj in rpsl_objs_now_valid + rpsl_objs_now_not_found:
            if rpsl_obj['old_status'] == RPKIStatus.invalid:
                self.status_tracker.record_operation(
                    operation=DatabaseOperation.add_or_update,
                    rpsl_pk=rpsl_obj['rpsl_pk'],
                    source=rpsl_obj['source'],
                    object_class=rpsl_obj['object_class'],
                    object_text=rpsl_obj['object_text'],
                    origin=JournalEntryOrigin.rpki_status,
                    source_serial=None,
                )
        for rpsl_obj in rpsl_objs_now_invalid:
            self.status_tracker.record_operation(
                operation=DatabaseOperation.delete,
                rpsl_pk=rpsl_obj['rpsl_pk'],
                source=rpsl_obj['source'],
                object_class=rpsl_obj['object_class'],
                object_text=rpsl_obj['object_text'],
                origin=JournalEntryOrigin.rpki_status,
                source_serial=None,
            )
        if rpsl_objs_now_valid or rpsl_objs_now_invalid or rpsl_objs_now_not_found:
            self._object_classes_modified.add('route')

    def update_scopefilter_status(self, rpsl_objs_now_in_scope: List[Dict[str, str]]=[],
                                  rpsl_objs_now_out_scope_as: List[Dict[str, str]]=[],
                                  rpsl_objs_now_out_scope_prefix: List[Dict[str, str]]=[]) -> None:
        """
        Update the scopefilter status for the given RPSL PKs.
        Only PKs whose status have changed should be included.

        Objects that moved to or from in_scope generate a journal
        entry, so that mirrors follow the (in)visibility depending
        on scopefilter status.
        """
        table = RPSLDatabaseObject.__table__
        if rpsl_objs_now_in_scope:
            pks = {o['rpsl_pk'] for o in rpsl_objs_now_in_scope}
            stmt = table.update().where(table.c.rpsl_pk.in_(pks)).values(scopefilter_status=ScopeFilterStatus.in_scope)
            self.execute_statement(stmt)
        if rpsl_objs_now_out_scope_as:
            pks = {o['rpsl_pk'] for o in rpsl_objs_now_out_scope_as}
            stmt = table.update().where(table.c.rpsl_pk.in_(pks)).values(scopefilter_status=ScopeFilterStatus.out_scope_as)
            self.execute_statement(stmt)
        if rpsl_objs_now_out_scope_prefix:
            pks = {o['rpsl_pk'] for o in rpsl_objs_now_out_scope_prefix}
            stmt = table.update().where(table.c.rpsl_pk.in_(pks)).values(scopefilter_status=ScopeFilterStatus.out_scope_prefix)
            self.execute_statement(stmt)

        for rpsl_obj in rpsl_objs_now_in_scope:
            self.status_tracker.record_operation(
                operation=DatabaseOperation.add_or_update,
                rpsl_pk=rpsl_obj['rpsl_pk'],
                source=rpsl_obj['source'],
                object_class=rpsl_obj['object_class'],
                object_text=rpsl_obj['object_text'],
                origin=JournalEntryOrigin.scope_filter,
                source_serial=None,
            )
            self._object_classes_modified.add(rpsl_obj['object_class'])

        for rpsl_obj in rpsl_objs_now_out_scope_as + rpsl_objs_now_out_scope_prefix:
            if rpsl_obj['old_status'] == ScopeFilterStatus.in_scope:
                self.status_tracker.record_operation(
                    operation=DatabaseOperation.delete,
                    rpsl_pk=rpsl_obj['rpsl_pk'],
                    source=rpsl_obj['source'],
                    object_class=rpsl_obj['object_class'],
                    object_text=rpsl_obj['object_text'],
                    origin=JournalEntryOrigin.scope_filter,
                    source_serial=None,
                )
                self._object_classes_modified.add(rpsl_obj['object_class'])

    def delete_rpsl_object(self, origin: JournalEntryOrigin, rpsl_object: Optional[RPSLObject]=None,
                           source: Optional[str]=None, rpsl_pk: Optional[str]=None,
                           source_serial: Optional[int]=None) -> None:
        """
        Delete an RPSL object from the database.

        The origin indicates the origin of this change, see JournalEntryOrigin
        for the various options.
        """
        self._flush_rpsl_object_writing_buffer()
        table = RPSLDatabaseObject.__table__
        if not source and rpsl_object:
            source = rpsl_object.parsed_data['source']
        if not rpsl_pk and rpsl_object:
            rpsl_pk = rpsl_object.pk()
        stmt = table.delete(
            sa.and_(table.c.rpsl_pk == rpsl_pk, table.c.source == source),
        ).returning(table.c.pk, table.c.rpsl_pk, table.c.source, table.c.object_class, table.c.object_text)
        results = self._connection.execute(stmt)

        if results.rowcount == 0:
            logger.error(f'Attempted to remove object {rpsl_pk}/{source}, but no database row matched')
            return None
        if results.rowcount > 1:  # pragma: no cover
            # This should not be possible, as rpsl_pk/source are a composite unique value in the database scheme.
            # Therefore, a query should not be able to affect more than one row - and we also can not test this
            # scenario. Due to the possible harm of a bug in this area, we still check for it anyways.
            affected_pks = ','.join([r[0] for r in results.fetchall()])
            msg = f'Attempted to remove object {rpsl_pk}/{source}, but multiple objects were affected, '
            msg += f'internal pks affected: {affected_pks}'
            logger.critical(msg)
            raise ValueError(msg)

        result = results.fetchone()
        self.status_tracker.record_operation(
            operation=DatabaseOperation.delete,
            rpsl_pk=result['rpsl_pk'],
            source=result['source'],
            object_class=result['object_class'],
            object_text=result['object_text'],
            origin=origin,
            source_serial=source_serial,
        )
        self._object_classes_modified.add(result['object_class'])

    def _flush_rpsl_object_writing_buffer(self) -> None:
        """
        Flush the current object writing buffer to the database.

        This happens in one large INSERT .. ON CONFLICT DO UPDATE ..
        statement, which is more performant than individual
        queries in case of large datasets.
        """
        if not self._rpsl_upsert_buffer:
            return

        rpsl_composite_key = ['rpsl_pk', 'source']
        stmt = pg.insert(RPSLDatabaseObject).values([x[0] for x in self._rpsl_upsert_buffer])

        if not self._rpsl_guaranteed_no_existing:
            columns_to_update = {
                c.name: c
                for c in stmt.excluded
                if c.name not in rpsl_composite_key and c.name != 'pk'
            }

            stmt = stmt.on_conflict_do_update(
                index_elements=rpsl_composite_key,
                set_=columns_to_update,
            )

        try:
            self._connection.execute(stmt)
        except Exception as exc:  # pragma: no cover
            self._transaction.rollback()
            logger.error(f'Exception occurred while executing statement: {stmt}, rolling back', exc_info=exc)
            raise

        for obj, origin, source_serial in self._rpsl_upsert_buffer:
            # RPKI invalid objects never generated a journal entry,
            # as mirrors should not see them. This does not affect
            # RPKI excluded sources, because they are always not_found.
            # Same applies for scope filter.
            rpki_acceptable = obj['rpki_status'] != RPKIStatus.invalid
            scope_acceptable = obj['scopefilter_status'] == ScopeFilterStatus.in_scope

            if rpki_acceptable and scope_acceptable:
                self.status_tracker.record_operation(
                    operation=DatabaseOperation.add_or_update,
                    rpsl_pk=obj['rpsl_pk'],
                    source=obj['source'],
                    object_class=obj['object_class'],
                    object_text=obj['object_text'],
                    origin=origin,
                    source_serial=source_serial,
                )

        self._rpsl_pk_source_seen = set()
        self._rpsl_upsert_buffer = []

    def _flush_roa_writing_buffer(self):
        """
        Flush the current ROA buffer to the database.
        As ROAs are only ever inserted, never updated,
        this is done with a single COPY command.
        """
        if not self._roa_insert_buffer:
            return

        columns = list(self._roa_insert_buffer[0].keys())
        roa_rows = []
        for roa in self._roa_insert_buffer:
            # ROA data is very simple, so we can use a naive CSV generator.
            roa_data = ','.join([str(roa[k]) for k in columns])
            roa_rows.append(roa_data)

        roa_csv = StringIO('\n'.join(roa_rows))
        roa_csv.seek(0)
        postgres_copy.copy_from(roa_csv, ROADatabaseObject, self._connection, columns=columns, format='csv')
        self._roa_insert_buffer = []

    def delete_all_rpsl_objects_with_journal(self, source):
        """
        Delete all RPSL objects for a source from the database,
        all journal entries and the database status.
        This is intended for cases where a full re-import is done.
        Note that no journal records are kept of this change itself.
        """
        self._flush_rpsl_object_writing_buffer()
        table = RPSLDatabaseObject.__table__
        stmt = table.delete(table.c.source == source)
        self._connection.execute(stmt)
        table = RPSLDatabaseJournal.__table__
        stmt = table.delete(table.c.source == source)
        self._connection.execute(stmt)
        table = RPSLDatabaseStatus.__table__
        stmt = table.delete(table.c.source == source)
        self._connection.execute(stmt)
        # All objects are presumed to have been changed.
        self._object_classes_modified.update(OBJECT_CLASS_MAPPING.keys())

    def delete_all_roa_objects(self):
        """
        Delete all ROA objects from the database.
        ROAs are always imported in bulk.
        """
        self._roa_insert_buffer = []
        stmt = ROADatabaseObject.__table__.delete()
        self._connection.execute(stmt)

    def set_force_reload(self, source):
        """
        Set the force_reload flag for a source.
        Upon the next mirror update, all data for the source will be
        discarded and reloaded from the source.
        """
        table = RPSLDatabaseStatus.__table__
        synchronised_serials = is_serial_synchronised(self, source, settings_only=True)
        stmt = table.update().where(table.c.source == source).values(
            force_reload=True,
            synchronised_serials=synchronised_serials,
            serial_oldest_seen=None,
            serial_newest_seen=None,
        )
        self._connection.execute(stmt)
        logger.info(f'force_reload flag set for {source}, serial synchronisation based on '
                    f'current settings: {synchronised_serials}')

    def record_serial_newest_mirror(self, source: str, serial: int) -> None:
        """
        Record that a mirror was updated to a certain serial.
        """
        self.status_tracker.record_serial_newest_mirror(source, serial)

    def record_serial_seen(self, source: str, serial: int):
        """
        Record that a serial was seen for a source
        """
        self.status_tracker.record_serial_seen(source, serial)

    def record_mirror_error(self, source: str, error: str) -> None:
        """
        Record an error seen in a mirrored database.
        Only the most recent error is stored in the DB status.
        """
        self.status_tracker.record_mirror_error(source, error)

    def record_serial_exported(self, source: str, serial: int) -> None:
        """
        Record an export of a source at a particular serial.
        """
        self.status_tracker.record_serial_exported(source, serial)

    def close(self) -> None:
        self._connection.close()


class DatabaseStatusTracker:
    """
    Keep track of the status of sources, and their journal, if enabled.
    Changes to the database should always call record_operation() on this object,
    and finalise_transaction() before committing.

    If journaling is enabled, a new entry in the journal will be made.
    If a journal entry was made, a record is kept in
    memory with all serials encountered/created for this source.

    When finalising, the RPSLDatabaseStatus table is updated to correctly
    reflect the range of serials known for a particular source.
    """
    journaling_enabled: bool
    _new_serials_per_source: Dict[str, Set[int]]
    _sources_seen: Set[str]
    _newest_mirror_serials: Dict[str, int]
    _mirroring_error: Dict[str, str]
    _exported_serials: Dict[str, int]

    c_journal = RPSLDatabaseJournal.__table__.c
    c_status = RPSLDatabaseStatus.__table__.c

    def __init__(self, database_handler: DatabaseHandler, journaling_enabled=True) -> None:
        self.database_handler = database_handler
        self.journaling_enabled = journaling_enabled
        self.reset()

    def record_serial_newest_mirror(self, source: str, serial: int):
        """
        Record that a mirror was updated to a certain serial.
        """
        self._sources_seen.add(source)
        self._newest_mirror_serials[source] = serial

    def record_serial_seen(self, source: str, serial: int):
        """
        Record that a serial was seen for a source
        """
        self._sources_seen.add(source)
        self._new_serials_per_source[source].add(serial)

    def record_mirror_error(self, source: str, error: str) -> None:
        """
        Record an error seen in a mirrored database.
        Only the most recent error is stored in the DB status.
        """
        self._sources_seen.add(source)
        self._mirroring_error[source] = error

    def record_serial_exported(self, source: str, serial: int) -> None:
        """
        Record an export of a source at a particular serial.
        Only the most recent serial is stored in the DB status.
        """
        self._sources_seen.add(source)
        self._exported_serials[source] = serial

    def record_operation(self, operation: DatabaseOperation, rpsl_pk: str, source: str, object_class: str,
                         object_text: str, origin: JournalEntryOrigin, source_serial: Optional[int]) -> None:
        """
        Make a record in the journal of a change to an object.

        Will only record changes when self.journaling_enabled is set,
        and the database.SOURCE.keep_journal is set.
        The source will always be added to _sources_seen.

        Note that this method locks the journal table for writing to ensure a
        gapless set of NRTM serials.
        """
        self._sources_seen.add(source)
        if self.journaling_enabled and get_setting(f'sources.{source}.keep_journal'):
            serial_nrtm: Union[int, sa.sql.expression.Select]
            journal_tablename = RPSLDatabaseJournal.__tablename__

            if self._is_serial_synchronised(source):
                serial_nrtm = source_serial
            else:
                self.database_handler.execute_statement(f'LOCK TABLE {journal_tablename} IN EXCLUSIVE MODE')
                serial_nrtm = sa.select([sa.text('COALESCE(MAX(serial_nrtm), 0) + 1')])
                serial_nrtm = serial_nrtm.where(RPSLDatabaseJournal.__table__.c.source == source)
                serial_nrtm = serial_nrtm.as_scalar()
            stmt = RPSLDatabaseJournal.__table__.insert().values(
                rpsl_pk=rpsl_pk,
                source=source,
                operation=operation,
                object_class=object_class,
                object_text=object_text,
                serial_nrtm=serial_nrtm,
                origin=origin,
            ).returning(self.c_journal.serial_nrtm)

            insert_result = self.database_handler.execute_statement(stmt)
            inserted_serial = insert_result.fetchone()['serial_nrtm']
            self._new_serials_per_source[source].add(inserted_serial)

            r = redis.Redis.from_url(get_setting('redis_url'))
            r.publish('irrd-nrtm', ujson.encode({
                'serial': inserted_serial,
                'operation': operation,
                'object_text': object_text,

            }, ensure_ascii=False).encode('utf-8'))


    def finalise_transaction(self):
        """
        - Create a new status object for all seen sources if it does not exist.
        - Reset the force_reload flag.
        - If new serials were recorded for a source, update the database
          serial stats in the status object.
        - Update the latest source errors.
        """
        for source in self._sources_seen:
            stmt = pg.insert(RPSLDatabaseStatus).values(
                source=source,
                force_reload=False,
                synchronised_serials=self._is_serial_synchronised(source),
                updated=datetime.now(timezone.utc),
            )
            stmt = stmt.on_conflict_do_update(
                index_elements=['source'],
                set_={
                    'force_reload': False,
                    'synchronised_serials': self._is_serial_synchronised(source),
                },
            )
            self.database_handler.execute_statement(stmt)

        for source, serials in self._new_serials_per_source.items():
            serial_oldest_journal_q = sa.select([
                sa.func.min(self.c_journal.serial_nrtm)
            ]).where(self.c_journal.source == source)
            result = self.database_handler.execute_statement(serial_oldest_journal_q)
            serial_oldest_journal = next(result)[0]

            serial_newest_journal_q = sa.select([
                sa.func.max(self.c_journal.serial_nrtm)
            ]).where(self.c_journal.source == source)
            result = self.database_handler.execute_statement(serial_newest_journal_q)
            serial_newest_journal = next(result)[0]

            serial_oldest_seen = sa.select([
                sa.func.least(
                    sa.func.min(self.c_status.serial_oldest_seen),
                    serial_oldest_journal,
                    min(serials)
                )
            ]).where(self.c_status.source == source)
            serial_newest_seen = sa.select([
                sa.func.greatest(
                    sa.func.max(self.c_status.serial_newest_seen),
                    serial_newest_journal,
                    max(serials)
                ),
            ]).where(self.c_status.source == source)

            stmt = RPSLDatabaseStatus.__table__.update().where(self.c_status.source == source).values(
                serial_oldest_seen=serial_oldest_seen,
                serial_newest_seen=serial_newest_seen,
                serial_oldest_journal=serial_oldest_journal,
                serial_newest_journal=serial_newest_journal,
                updated=datetime.now(timezone.utc),
            )
            self.database_handler.execute_statement(stmt)

        for source, error in self._mirroring_error.items():
            stmt = RPSLDatabaseStatus.__table__.update().where(self.c_status.source == source).values(
                last_error=error,
                last_error_timestamp=datetime.now(timezone.utc),
                updated=datetime.now(timezone.utc),
            )
            self.database_handler.execute_statement(stmt)

        for source, serial in self._newest_mirror_serials.items():
            stmt = RPSLDatabaseStatus.__table__.update().where(self.c_status.source == source).values(
                serial_newest_mirror=serial,
                updated=datetime.now(timezone.utc),
            )
            self.database_handler.execute_statement(stmt)

        for source, serial in self._exported_serials.items():
            stmt = RPSLDatabaseStatus.__table__.update().where(self.c_status.source == source).values(
                serial_last_export=serial,
            )
            self.database_handler.execute_statement(stmt)

        self.reset()

    @lru_cache(maxsize=100)
    def _is_serial_synchronised(self, source: str) -> bool:
        # Cached wrapper method
        return is_serial_synchronised(self.database_handler, source)

    def reset(self):
        self._new_serials_per_source = defaultdict(set)
        self._sources_seen = set()
        self._newest_mirror_serials = dict()
        self._mirroring_error = dict()
        self._exported_serials = dict()
        self._is_serial_synchronised.cache_clear()


def is_serial_synchronised(database_handler: DatabaseHandler, source: str, settings_only=False) -> bool:
    """
    Determine whether a source should use / is using serial synchronisation
    from the NRTM mirror source.
    If settings_only is set, only look at whether serial synchronisation should
    be enabled based on the current config. Otherwise, also looks at the current
    flag in the database, which catches cases where serials have gone out of
    sync in the past.
    """
    if settings_only:
        db_status = True
    else:
        db_query = DatabaseStatusQuery().source(source)
        db_result = database_handler.execute_query(db_query, flush_rpsl_buffer=False)
        try:
            db_status = next(db_result)['synchronised_serials']
        except StopIteration:
            db_status = True
    settings_status = all([
        not get_setting('scopefilter') or get_setting(f'sources.{source}.scopefilter_excluded'),
        not get_setting('rpki.roa_source') or get_setting(f'sources.{source}.rpki_excluded'),
        get_setting(f'sources.{source}.nrtm_host')
    ])
    return db_status and settings_status
