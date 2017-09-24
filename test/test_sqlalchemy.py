import pprint
import sys

from sqlalchemy import inspect, select
from sqlalchemy.exc import IntegrityError, InvalidRequestError

import pytest

from .models import Thing

from .conftest import DEFAULT_NAME, DEFAULT_CREATED_BY, EXTRA_NAME

missing = object()


# TODO: make 'updater' and 'verifier' fixtures that are composable
# and capture (a) every different type of update and the state in which
# sqlalchemy resources can be left after an updatek, and (b) every
# different type of verification test that should be run after
# updates to ensure everything still works as expected and that
# things are returned to the state they should in...
# And then replace all fixtures that currently exist
# with a parametrized m x n test function that makes uses of all
# the possible pairs of (updater, verifier) fixtures...


def get(session, name, created_by=missing):
    kw = dict(name=name)
    if created_by is not missing:
        kw['created_by'] = created_by
    return session.query(Thing).filter_by(**kw).first()


def put(session, name, created_by=missing):
    kw = dict(name=name)
    if created_by is not missing:
        kw['created_by'] = created_by
    t = Thing(**kw)
    session.add(t)
    session.flush()
    return t


def count(session):
    return session.query(Thing).count()


def test_exactly_one_object_exists(session):
    assert session.query(Thing).count() == 1


def test_object_has_original_name(session, original_name):
    assert session.query(Thing).one().name == original_name


def test_object_has_original_id(session, original_id):
    assert session.query(Thing).one().id == original_id


def test_update_obj_and_flush(session, obj):
    assert obj
    ins = inspect(obj)
    assert ins.session is session
    obj = session.query(Thing).filter_by(id=obj.id).one()
    new_name = 'new_name'
    obj.name = new_name
    session.add(obj)
    session.flush()
    assert session.query(Thing).filter_by(id=obj.id).one().name == new_name


def test_instance_name_reverted_to_default(db):
    assert db.session.query(Thing).count() == 1
    instance = db.session.query(Thing).first()
    assert instance
    assert instance.name == DEFAULT_NAME


@pytest.mark.xfail
def test_xfail_integrity_constraint_error_uncaught(session):
    put(session, None)


def test_preseeded_obj_exists_after_integrity_constraint_error1(session):
    assert count(session) > 0
    assert get(session, DEFAULT_NAME)


def test_fail_integrity_constraint_error_caught(session):
    with pytest.raises(IntegrityError) as err:
        put(session, DEFAULT_NAME, created_by=DEFAULT_CREATED_BY)
    assert 'duplicate key value' in str(err.value)


def test_fail_integrity_constraint_error_caught_then_commit(session):
    with pytest.raises(IntegrityError) as err:
        put(session, DEFAULT_NAME, created_by=DEFAULT_CREATED_BY)
    assert 'duplicate key value' in str(err.value)
    with pytest.raises(InvalidRequestError) as err:
        session.commit()
    assert 'transaction has been rolled back due to a previous' in str(err.value)


def test_fail_integrity_constraint_error_caught_then_rollback(session):
    with pytest.raises(IntegrityError) as err:
        put(session, DEFAULT_NAME, created_by=DEFAULT_CREATED_BY)
    assert 'duplicate key value' in str(err.value)
    session.rollback()


def test_create_extra_obj_no_commit(session):
    t = put(session, EXTRA_NAME)
    assert get(session, EXTRA_NAME) == t


def test_extra_does_not_exist(session):
    assert not get(session, EXTRA_NAME)


def test_create_extra_obj_commit(session):
    t = put(session, EXTRA_NAME)
    session.commit()
    assert get(session, EXTRA_NAME) == t


def test_extra_does_not_exist_after_previous_commit(session):
    assert not get(session, EXTRA_NAME)


def test_bypass_session_and_commit_no_new_tx_no_sess_begin(db):
    conn = db.connection
    obj = db.obj
    t = Thing.__table__
    new_name = 'bypassing-session'

    def getall(conn):
        return sorted(conn.execute(select([t.c.name])))

    with conn.begin_nested():
        assert getall(conn) == [(DEFAULT_NAME,)]
    with conn.begin():
        assert getall(conn) == [(DEFAULT_NAME,)]

    with db.session.begin_nested():
        assert getall(conn) == [(DEFAULT_NAME,)]
    with db.session.begin(subtransactions=True):
        assert getall(conn) == [(DEFAULT_NAME,)]
    with db.session.begin(subtransactions=True):
        assert getall(conn) == [(DEFAULT_NAME,)]
        stmt = t.update().values(name=new_name).where(t.c.id == obj.id)
        conn.execute(stmt)
        assert getall(conn) == [(new_name,)]

    with db.session.begin_nested():
        assert getall(conn) == [(new_name,)]

    with db.connection.begin():
        db.session.execute(stmt)
        assert getall(conn) == [(new_name,)]

    with db.connection.begin_nested():
        db.session.execute(stmt)
        assert getall(conn) == [(new_name,)]

    with db.session.begin(subtransactions=True):
        db.session.execute(stmt)
        assert getall(conn) == [(new_name,)]


def test_bypass_session_and_commit_no_new_tx_no_sess_begin_nested(db):
    d = db
    conn = d.session.connection()
    obj = d.obj
    t = Thing.__table__
    new_name = 'bypassing-session'

    def getall(conn):
        return sorted(conn.execute(select([t.c.name])))

    assert getall(conn) == [(DEFAULT_NAME,)]
    stmt = t.update().values(name=new_name).where(t.c.id == obj.id)

    with conn.begin():
        assert getall(conn) == [(DEFAULT_NAME,)]

    with d.session.begin_nested():
        d.session.execute(stmt)
        assert getall(conn) == [(new_name,)]

    assert getall(conn) == [(new_name,)]


def test_bypass_session_name_change_reverted_query_connection_no_sess1(db):
    d = db
    assert list(d.session.query(Thing.name).all()) == [(DEFAULT_NAME,)]


def test_use_preseeded_thing_session_using_method_no_sess1(obj):
    assert obj.exists()


def test_bypass_session_and_commit_new_tx_no_sess(db):
    conn = db.connection
    obj = db.obj
    t = Thing.__table__
    new_name = 'bypassing-session'

    def getall(conn):
        return sorted(conn.execute(select([t.c.name])))

    assert getall(conn) == [(obj.name,)]
    stmt = t.update().values(name=new_name).where(t.c.id == obj.id)

    with conn.begin():
        conn.execute(stmt)

    with conn.begin_nested():
        conn.execute(stmt)
    assert getall(conn) == [(new_name,)]

    with conn.begin():
        assert getall(conn) == [(new_name,)]


def test_bypass_session_name_change_reverted_query_connection_no_sess2(db):
    conn = db.connection
    t = Thing.__table__
    rows = sorted(conn.execute(select([t.c.name])))
    assert rows == [(DEFAULT_NAME,)]


def test_bypass_session_name_change_reverted_query_session_no_sess2(db):
    session = db.session
    session.flush()
    t = Thing.__table__
    rows = sorted((x.name, ) for x in session.query(Thing).all())
    assert rows == [(DEFAULT_NAME,)]


def test_use_preseeded_thing_session_using_method_no_sess2(session, obj):
    assert obj.exists()


def test_bypass_session_and_commit_new_tx_nested_no_sess(db):
    conn = db.connection
    obj = db.obj
    t = type(obj).__table__
    new_name = 'bypassing-session'

    def getall(conn):
        return sorted(conn.execute(select([t.c.name])))

    assert getall(conn) == [(obj.name,)]
    stmt = t.update().values(name=new_name).where(t.c.id == obj.id)
    with conn.begin_nested():
        conn.execute(stmt)
        assert getall(conn) == [(new_name,)]
    assert getall(conn) == [(new_name,)]
    with conn.begin():
        assert getall(conn) == [(new_name,)]


def test_bypass_session_name_change_reverted_query_connection_no_sess3(db):
    conn = db.connection
    t = Thing.__table__
    with conn.begin_nested():
        rows = sorted(conn.execute(select([t.c.name])))
        assert rows == [(DEFAULT_NAME,)]


def test_bypass_session_name_change_reverted_query_session_no_sess3(db):
    session = db.session
    session.flush()
    rows = sorted((x.name, ) for x in session.query(Thing).all())
    assert rows == [(DEFAULT_NAME,)]


def test_x_session_use_preseeded_thing_session_using_method_no_sess3(obj):
    assert obj.exists()


def test_bypass_session_and_commit_no_new_tx(db, session):
    conn = db.connection
    obj = db.obj
    t = type(obj).__table__
    new_name = 'bypassing-session'

    def getall(conn):
        return sorted(conn.execute(select([t.c.name])))

    assert getall(conn) == [(DEFAULT_NAME,)]
    stmt = t.update().values(name=new_name).where(t.c.id == obj.id)
    with conn.begin_nested():
        conn.execute(stmt)
        assert getall(conn) == [(new_name,)]

    with conn.begin_nested():
        conn.execute(stmt)
    assert getall(conn) == [(new_name,)]


def test_bypass_session_name_change_reverted_query_connection1(db):
    conn = db.connection
    rows = sorted(conn.execute(select([Thing.__table__.c.name])))
    assert rows == [(DEFAULT_NAME,)]


def test_bypass_session_name_change_reverted_query_session1(db):
    session = db.session
    session.flush()
    rows = sorted((x.name, ) for x in session.query(Thing).all())
    assert rows == [(DEFAULT_NAME,)]


def test_use_preseeded_thing_session_using_method1(session, obj):
    assert obj.exists()


def test_xbypass_session_and_commit_new_tx(db, session):
    conn = db.connection
    obj = db.obj
    t = Thing.__table__
    new_name = 'bypassing-session'

    def getall(conn):
        return sorted(conn.execute(select([t.c.name])))

    assert getall(conn) == [(obj.name,)]
    stmt = t.update().values(name=new_name).where(t.c.id == obj.id)

    with conn.begin():
        conn.execute(stmt)
        assert getall(conn) == [(new_name,)]

    with conn.begin_nested():
        conn.execute(stmt)
        assert getall(conn) == [(new_name,)]

    assert getall(conn) == [(new_name,)]


def test_bypass_session_name_change_reverted_query_connection2(db):
    conn = db.connection
    rows = sorted(conn.execute(select([Thing.__table__.c.name])))
    assert rows == [(DEFAULT_NAME,)]


def test_bypass_session_name_change_reverted_query_session2(db):
    session = db.session
    session.flush()
    rows = sorted((x.name, ) for x in session.query(Thing).all())
    assert rows == [(DEFAULT_NAME,)]


def test_use_preseeded_thing_session_using_method2(session, obj):
    assert obj.exists()


def test_bypass_session_and_commit_new_tx_nested(db, session):
    conn = db.connection
    obj = db.obj
    new_name = 'bypassing-session'
    t = Thing.__table__

    def getall(conn):
        return sorted(conn.execute(select([t.c.name])))

    assert getall(conn) == [(obj.name,)]
    stmt = t.update().values(name=new_name).where(t.c.id == obj.id)
    with conn.begin_nested():
        conn.execute(stmt)
    assert getall(conn) == [(new_name,)]


def test_bypass_session_name_change_reverted_query_connection3(db):
    conn = db.connection
    t = Thing.__table__
    rows = sorted(conn.execute(select([t.c.name])))
    assert rows == [(DEFAULT_NAME,)]


def test_bypass_session_name_change_reverted_query_session3(db):
    session = db.session
    session.flush()
    t = Thing.__table__
    rows = sorted((x.name, ) for x in session.query(Thing).all())
    assert rows == [(DEFAULT_NAME,)]


def test_use_preseeded_thing_session_using_method3(session, obj):
    assert obj.exists()
