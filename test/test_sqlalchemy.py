#pylint: disable=missing-docstring,unused-argument

import functools
from functools import partial
import sys

from sqlalchemy import inspect, insert, select, update, and_
from sqlalchemy.exc import IntegrityError, InvalidRequestError

import pytest

from .models import Thing

from .conftest import DEFAULT_NAME, DEFAULT_CREATED_BY, EXTRA_NAME

MISSING = object()
TABLE = Thing.__table__
UPDATE1_NAME = 'update1-name'
UPDATE2_NAME = 'update2-name'

UPDATE1_CREATED_BY = 'changed-by-update1'
UPDATE2_CREATED_BY = 'changed-by-update2'

INSERT_NAME = 'insert1-name'
INSERT_CREATED_BY = 'insert1-created-by'

from .conftest import AUTOUSE_SESSION

put = functools.partial(print, file=sys.stderr)


def get(session, name, created_by=MISSING):
    kw = dict(name=name)
    if created_by is not MISSING:
        kw['created_by'] = created_by
    return session.query(Thing).filter_by(**kw).first()


def put_(session, name, created_by=MISSING):
    kw = dict(name=name)
    if created_by is not MISSING:
        kw['created_by'] = created_by
    obj = Thing(**kw)
    session.add(obj)
    session.flush()
    return obj


def core_count_sql(db):
    res = db.connection.execute('select count(*) from thing')
    return res.scalar()


def core_count_select(db):
    stmt = select([TABLE.c.id])
    return len(list(db.connection.execute(stmt)))


def core_count_table(db):
    stmt = TABLE.select()
    return len(list(db.connection.execute(stmt)))


def orm_count(db):
    return db.session.query(Thing).count()


def count_core_query_select(db):
    return db.connection.execute(select([TABLE])).rowcount


def count_core_query_table(db):
    return db.connection.execute(TABLE.select()).rowcount


def count_session_query(db):
    return db.session.query(Thing).count()


def core_query_select2(db, column, query):
    if isinstance(column, tuple):
        fromstmt = [getattr(TABLE.c, name) for name in column]
    condition = and_(getattr(TABLE.c, k) == v for k, v in query.items())
    stmt = select([getattr(TABLE.c, column)]).where(condition)
    res = db.connection.execute(stmt)


def core_query_select(db, field='name', value=MISSING):
    column = getattr(TABLE.c, field)
    stmt = select([column]).where(TABLE.c.id == db.original_id)
    res = db.connection.execute(stmt)
    assert res.rowcount == 1
    value = list(res)[0][0]
    return value


def core_query_table(db, field='name', value=MISSING):
    column = getattr(TABLE.c, field)
    stmt = TABLE.select().with_only_columns([column]).where(TABLE.c.id == db.original_id)
    res = db.connection.execute(stmt)
    assert res.rowcount == 1
    value = list(res)[0][0]
    return value


def session_query(db, field='name', populate_existing=True, value=MISSING):
    # if not query_args:
    #     query_args['id'] = db.original_id
    q = db.session.query(Thing)
    if populate_existing:
        q = q.populate_existing()
    # q = q.filter_by(**query_args)
    q = q.filter_by(id=db.original_id)
    res = q.one()
    return getattr(res, field)


def core_update_update(db, field='name', value=None):
    # put('core_update_update:', db, field, value)
    vals = {'name': value}
    stmt = update(TABLE).where(TABLE.c.id == db.original_id).values(**vals)
    res = db.session.connection().execute(stmt)
    updated = res.rowcount
    res.close()
    assert updated == 1
    return updated


def core_update_table(db, field='name', value=None):
    vals = {'name': value}
    stmt = TABLE.update().where(TABLE.c.id == db.original_id).values(**vals)
    res = db.session.connection().execute(stmt)
    updated = res.rowcount
    res.close()
    assert updated == 1
    return updated


def orm_update(db, field='name', value=None, flush=True):
    q = db.session.query(Thing).populate_existing().filter_by(id=db.original_id)
    obj = q.first()
    assert obj
    setattr(obj, field, value)
    db.session.add(obj)
    if flush:
        db.session.flush()
    return 1


def core_insert_func(db, state):
    stmt = insert(TABLE).values(**state)
    res = db.connection.execute(stmt)
    num_updates = res.rowcount
    res.close()
    assert num_updates == 1
    return num_updates


def core_insert_method(db, state):
    stmt = TABLE.insert().values(**state)
    res = db.connection.execute(stmt)
    num_updates = res.rowcount
    res.close()
    assert num_updates == 1
    return num_updates


def orm_insert(db, state):
    obj = Thing(**state)
    db.session.add(obj)
    db.session.flush()
    return 1


def runfunc(db, func):
    if func is None:
        return

    if not isinstance(func, (tuple, list)):
        # a callable that does its own asserts and checks
        assert callable(func)
        return func(db)

    # a function with zero or more args and an expected value
    assert len(func) > 1
    fn, *args, expected_value = func
    # put('running fn %s with db arg and *args:' % (fn,), args, 'expected_value:', expected_value)
    assert fn(db, *args) == expected_value


@pytest.mark.parametrize('postcheck', [
    (core_count_sql, 1),
    (core_count_select, 1),
    (core_count_table, 1),
    (orm_count, 1),
    (core_query_select, 'name', UPDATE1_NAME),
    (core_query_table, 'name', UPDATE1_NAME),
    (session_query, 'name', UPDATE1_NAME),
    (core_query_select, 'created_by', DEFAULT_CREATED_BY),
    (core_query_table, 'created_by', DEFAULT_CREATED_BY),
    (session_query, 'created_by', DEFAULT_CREATED_BY),
])
@pytest.mark.parametrize('update', [
   (core_update_update, 'name', UPDATE1_NAME, 1),
   (core_update_table, 'name', UPDATE1_NAME, 1),
   (partial(orm_update, flush=True), 'name', UPDATE1_NAME, 1),
])
@pytest.mark.parametrize('precheck', [
    (count_core_query_select, 1),
    (count_core_query_table, 1),
    (count_core_query_select, 1),
    (core_query_select, 'name', DEFAULT_NAME),
    (core_query_table, 'name', DEFAULT_NAME),
    (session_query, 'name', DEFAULT_NAME),
    (core_query_select, 'created_by', DEFAULT_CREATED_BY),
    (core_query_table, 'created_by', DEFAULT_CREATED_BY),
    (session_query, 'created_by', DEFAULT_CREATED_BY),
])
def test_update_existing(db, session, precheck, update, postcheck):
    runfunc(db, precheck)
    runfunc(db, update)
    runfunc(db, postcheck)


@pytest.mark.parametrize('postcheck', [
    (core_count_sql, 2),
    (core_count_select, 2),
    (core_count_table, 2),
    (orm_count, 2),
    # (core_query_select, 'name', UPDATE1_NAME),
    # (core_query_table, 'name', UPDATE1_NAME),
    # (session_query, 'name', UPDATE1_NAME),
    # (core_query_select, 'created_by', DEFAULT_CREATED_BY),
    # (core_query_table, 'created_by', DEFAULT_CREATED_BY),
    # (session_query, 'created_by', DEFAULT_CREATED_BY),
])
@pytest.mark.parametrize('insert', [
   (core_insert_func, {'name': INSERT_NAME, 'created_by': INSERT_CREATED_BY}, 1),
#    (core_insert_method, 'name', UPDATE1_NAME, 1),
#    (orm_insert, 'name', UPDATE1_NAME, 1),
])
@pytest.mark.parametrize('precheck', [
    (count_core_query_select, 1),
    (count_core_query_table, 1),
    (count_core_query_select, 1),
    (core_query_select, 'name', DEFAULT_NAME),
    (core_query_table, 'name', DEFAULT_NAME),
    (session_query, 'name', DEFAULT_NAME),
    (core_query_select, 'created_by', DEFAULT_CREATED_BY),
    (core_query_table, 'created_by', DEFAULT_CREATED_BY),
    (session_query, 'created_by', DEFAULT_CREATED_BY),
])
def test_insert(db, session, precheck, insert, postcheck):
    runfunc(db, precheck)
    runfunc(db, insert)
    runfunc(db, postcheck)


# def pytest_generate_tests(metafunc):
#     func = metafunc.function
#     idlist = []
#     argnames = ['update', 'check']
#     argvalues = []
#     if getattr(func, NEED_CHECKERS, False) and getattr(func, NEED_UPDATERS, False):
#         for updater in UPDATERS:
#             for checker in CHECKERS:
#                 idlist.append(updater.__name__ + '-' + checker.__name__)
#                 argvalues.append((updater, checker))
#         # idlist = ['x1', 'x2', 'x3', 'x4']
#         # argnames = ['update', 'check']
#         # argvalues = [(update1, check1), (update1, check2), (update2, check1), (update2, check2)]
#         # metafunc.parametrize(argnames, argvalues, ids=idlist, scope="class")
#         # put({'idlist': list(idlist), 'argnames': argnames, 'argvalues': argvalues})
#         metafunc.parametrize(argnames, argvalues, ids=idlist, scope="class")


"""

@pytest.mark.xfail
def test_xfail_integrity_constraint_error_uncaught(session):
    put_(session, None)


def test_preseeded_obj_exists_after_integrity_constraint_error1(session):
    assert count(session) > 0
    assert get(session, DEFAULT_NAME)


def test_fail_integrity_constraint_error_caught(session):
    with pytest.raises(IntegrityError) as err:
        put_(session, DEFAULT_NAME, created_by=DEFAULT_CREATED_BY)
    assert 'duplicate key value' in str(err.value)


def test_fail_integrity_constraint_error_caught_then_commit(session):
    with pytest.raises(IntegrityError) as err:
        put_(session, DEFAULT_NAME, created_by=DEFAULT_CREATED_BY)
    assert 'duplicate key value' in str(err.value)
    with pytest.raises(InvalidRequestError) as err:
        session.commit()
    assert 'transaction has been rolled back due to a previous' in str(err.value)


def test_fail_integrity_constraint_error_caught_then_rollback(session):
    with pytest.raises(IntegrityError) as err:
        put_(session, DEFAULT_NAME, created_by=DEFAULT_CREATED_BY)
    assert 'duplicate key value' in str(err.value)
    session.rollback()


def test_create_extra_obj_no_commit(session):
    obj = put_(session, EXTRA_NAME)
    assert get(session, EXTRA_NAME) == obj


def test_extra_does_not_exist(session):
    assert not get(session, EXTRA_NAME)


def test_create_extra_obj_commit(session):
    obj = put_(session, EXTRA_NAME)
    session.commit()
    assert get(session, EXTRA_NAME) == obj


def test_extra_does_not_exist_after_previous_commit(session):
    assert not get(session, EXTRA_NAME)


def test_bypass_session_and_commit_no_new_tx_no_sess_begin(db):
    conn = db.connection
    obj = db.obj
    new_name = 'bypassing-session'

    def getall(conn):
        return sorted(conn.execute(select([TABLE.c.name])))

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
        stmt = TABLE.update().values(name=new_name).where(TABLE.c.id == obj.id)
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
    conn = db.session.connection()
    obj = db.obj
    new_name = 'bypassing-session'

    def getall(conn):
        return sorted(conn.execute(select([TABLE.c.name])))

    assert getall(conn) == [(DEFAULT_NAME,)]
    stmt = TABLE.update().values(name=new_name).where(TABLE.c.id == obj.id)

    with conn.begin():
        assert getall(conn) == [(DEFAULT_NAME,)]

    with db.session.begin_nested():
        db.session.execute(stmt)
        assert getall(conn) == [(new_name,)]

    assert getall(conn) == [(new_name,)]


def test_bypass_session_name_change_reverted_query_connection_no_sess1(db):
    assert list(db.session.query(Thing.name).all()) == [(DEFAULT_NAME,)]


def test_use_preseeded_thing_session_using_method_no_sess1(obj):
    assert obj.exists()


def test_bypass_session_and_commit_new_tx_no_sess(db):
    conn = db.connection
    obj = db.obj
    new_name = 'bypassing-session'

    def getall(conn):
        return sorted(conn.execute(select([TABLE.c.name])))

    assert getall(conn) == [(obj.name,)]
    stmt = TABLE.update().values(name=new_name).where(TABLE.c.id == obj.id)

    with conn.begin():
        conn.execute(stmt)

    with conn.begin_nested():
        conn.execute(stmt)
    assert getall(conn) == [(new_name,)]

    with conn.begin():
        assert getall(conn) == [(new_name,)]


def test_bypass_session_name_change_reverted_query_connection_no_sess2(db):
    conn = db.connection
    rows = sorted(conn.execute(select([TABLE.c.name])))
    assert rows == [(DEFAULT_NAME,)]


def test_bypass_session_name_change_reverted_query_session_no_sess2(db):
    session = db.session
    session.flush()
    rows = sorted((x.name, ) for x in session.query(Thing).all())
    assert rows == [(DEFAULT_NAME,)]


def test_use_preseeded_thing_session_using_method_no_sess2(session, obj):
    assert obj.exists()


def test_bypass_session_and_commit_new_tx_nested_no_sess(db):
    conn = db.connection
    obj = db.obj
    new_name = 'bypassing-session'

    def getall(conn):
        return sorted(conn.execute(select([TABLE.c.name])))

    assert getall(conn) == [(obj.name,)]
    stmt = TABLE.update().values(name=new_name).where(TABLE.c.id == obj.id)
    with conn.begin_nested():
        conn.execute(stmt)
        assert getall(conn) == [(new_name,)]
    assert getall(conn) == [(new_name,)]
    with conn.begin():
        assert getall(conn) == [(new_name,)]


def test_bypass_session_name_change_reverted_query_connection_no_sess3(db):
    conn = db.connection
    with conn.begin_nested():
        rows = sorted(conn.execute(select([TABLE.c.name])))
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
    new_name = 'bypassing-session'

    def getall(conn):
        return sorted(conn.execute(select([TABLE.c.name])))

    assert getall(conn) == [(DEFAULT_NAME,)]
    stmt = TABLE.update().values(name=new_name).where(TABLE.c.id == obj.id)
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
    new_name = 'bypassing-session'

    def getall(conn):
        return sorted(conn.execute(select([TABLE.c.name])))

    assert getall(conn) == [(obj.name,)]
    stmt = TABLE.update().values(name=new_name).where(TABLE.c.id == obj.id)

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

    def getall(conn):
        return sorted(conn.execute(select([TABLE.c.name])))

    assert getall(conn) == [(obj.name,)]
    stmt = TABLE.update().values(name=new_name).where(TABLE.c.id == obj.id)
    with conn.begin_nested():
        conn.execute(stmt)
    assert getall(conn) == [(new_name,)]


def test_bypass_session_name_change_reverted_query_connection3(db):
    conn = db.connection
    rows = sorted(conn.execute(select([TABLE.c.name])))
    assert rows == [(DEFAULT_NAME,)]


def test_bypass_session_name_change_reverted_query_session3(db):
    session = db.session
    session.flush()
    rows = sorted((x.name, ) for x in session.query(Thing).all())
    assert rows == [(DEFAULT_NAME,)]


def test_use_preseeded_thing_session_using_method3(session, obj):
    assert obj.exists()


count1 = count_core_query_select
count2 = count_core_query_table
count = count_session_query


# @pytest.fixture
def update1_(db):
    extra_name = 'extra_name'

    add_extra = True
    expected_count = 1

    if add_extra:
        stmt = TABLE.insert().values(name=extra_name)
        res = db.session.connection().execute(stmt)
        assert res.rowcount == 1
        res.close()
        expected_count += 1
        assert count1(db) == expected_count

    name = UPDATE1_NAME
    stmt = TABLE.update().where(TABLE.c.id == db.original_id).values(name=name)
    res = db.session.connection().execute(stmt)
    assert res.rowcount == 1

    assert count1(db) == expected_count
    assert count2(db) == expected_count

    stmt = select([TABLE.c.id, TABLE.c.name]).where(TABLE.c.id == db.original_id)
    results = list(db.session.connection().execute(stmt))
    assert results == [(db.original_id, name)]

    result = db.session.query(TABLE.c.name).order_by(TABLE.c.id).first()
    assert result == (name,)

    assert count1(db) == expected_count
    assert count2(db) == expected_count

    # original object unchanged
    obj = db.obj
    assert obj.name == DEFAULT_NAME

    # the session cache seems to have the old name still too
    put('\n\ndoing session.query...')
    res = db.session.query(Thing).order_by(TABLE.c.id).first()
    put('\n\nquery complete; access name attr next...')
    assert res.name == DEFAULT_NAME
    q = db.session.query(Thing).filter(Thing.id == db.original_id)

    expected_name = DEFAULT_NAME if not add_extra else name
    expected_name = DEFAULT_NAME

    assert q.first().name == expected_name
    assert q.order_by(Thing.id).first().name == expected_name
    assert q.order_by(TABLE.c.id).first().name == expected_name
    if add_extra:
        assert q.one().name == expected_name
    assert list(q.all())[0].name == expected_name
    assert db.session.query(Thing).get(db.original_id).name == expected_name

    # If an object already exists in the session cache for a given primary key,
    # the state that is fetched from the database when doing queries using
    # the session will not be used even if it is different than the state
    # of the object in the cache. To ensure correct state, do one of
    # query.populate_existing(),  session.refresh(obj), session.expire(obj),
    # or session.expire_all().
    q = db.session.query(Thing).filter(Thing.id >= -1)
    results = list(q.order_by(Thing.id).all())
    assert len(results) == expected_count == 2
    result0 = results[0]
    assert result0.id == db.original_id
    assert result0.name == DEFAULT_NAME
    assert q.order_by(Thing.id).first().name == DEFAULT_NAME

    # results = list(q.order_by(Thing.id).populate_existing().all())
    # assert len(results) == expected_count == 2
    # result0 = results[0]
    # assert result0.id == db.original_id
    # assert result0.name == name
    # assert q.order_by(Thing.id).first().name == name

    # name = UPDATE1_NAME
    # stmt = TABLE.update().where(TABLE.c.id == db.original_id).values(name=name)
    # res = db.session.connection().execute(stmt)
    # assert res.rowcount == 1
"""
