#pylint: disable=missing-docstring,unused-argument

from contextlib import contextmanager
import functools
import sys

from sqlalchemy import inspect, select, update
from sqlalchemy.exc import IntegrityError, InvalidRequestError

import pytest

from .models import Thing

from .conftest import DEFAULT_NAME, DEFAULT_CREATED_BY, EXTRA_NAME

MISSING = object()
TABLE = Thing.__table__
UPDATE1_NAME = 'update1-name'
UPDATE2_NAME = 'update2-name'

UPDATERS = []
CHECKERS = []

NEED_UPDATERS = '_need_updaters'
NEED_CHECKERS = '_need_checkers'

put = functools.partial(print, file=sys.stderr)


def get_column(db, id, name):
    column = getattr(TABLE.c, name)
    stmt = select([column]).where(TABLE.c.id == id)
    res = db.connection.execute(stmt)
    rows = list(res)
    res.close()
    assert len(rows) == 1
    return rows[0][0]


def updater(f):
    UPDATERS.append(f)

    @functools.wraps(f)
    def wrapper(*args, **kw):
        return f(*args, **kw)
    return wrapper


def checker(f):
    CHECKERS.append(f)

    @functools.wraps(f)
    def wrapper(*args, **kw):
        return f(*args, **kw)
    return wrapper


def all_updaters(f):
    setattr(f, NEED_UPDATERS, True)
    return f


def all_checkers(f):
    setattr(f, NEED_CHECKERS, True)
    return f


def all_updaters_and_checkers(f):
    setattr(f, NEED_UPDATERS, True)
    setattr(f, NEED_CHECKERS, True)
    return f


def count(session):
    return session.query(Thing).count()


def count1(db):
    return db.session.query(Thing).count()


def count2(db):
    stmt = TABLE.select()
    return db.connection.execute(stmt).rowcount


@updater
def update_name_core_table(db):
    name = UPDATE1_NAME
    stmt = TABLE.update().where(TABLE.c.id == db.original_id).values(name=name)
    res = db.session.connection().execute(stmt)
    assert res.rowcount == 1


@updater
def update_name_core_update(db):
    name = UPDATE1_NAME
    stmt = update(TABLE).where(TABLE.c.id == db.original_id).values(name=name)
    res = db.session.connection().execute(stmt)
    assert res.rowcount == 1
    assert get_column(db, db.original_id, 'name') == name


@updater
def update2(db):
    created_by = 'changed-by-update2'
    stmt = TABLE.update().where(TABLE.c.id == db.original_id).values(
        created_by=created_by)
    res = db.session.connection().execute(stmt)
    assert res.rowcount == 1
    assert get_column(db, db.original_id, 'created_by') == created_by


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

    results = list(q.order_by(Thing.id).populate_existing().all())
    assert len(results) == expected_count == 2
    result0 = results[0]
    assert result0.id == db.original_id
    assert result0.name == name
    assert q.order_by(Thing.id).first().name == name


# def update2(db):
#     obj = db.session.query(Thing).one()
#     obj.name = UPDATE2_NAME
#     db.session.add(obj)
#     db.session.flush()


@checker
def query_count_unchanged_sql(db):
    res = db.connection.execute('select count(*) from thing')
    assert res.scalar() == 1


@checker
def query_count_unchanged_core_select(db):
    stmt = select([TABLE.c.id])
    assert len(list(db.connection.execute(stmt))) == 1


@checker
def query_count_unchanged_session(db):
    assert db.session.query(Thing).count() == 1



def pytest_generate_tests(metafunc):
    func = metafunc.function
    idlist = []
    argnames = ['update', 'check']
    argvalues = []
    if getattr(func, NEED_CHECKERS, False) and getattr(func, NEED_UPDATERS, False):
        for updater in UPDATERS:
            for checker in CHECKERS:
                idlist.append(updater.__name__ + '-' + checker.__name__)
                argvalues.append((updater, checker))
        # idlist = ['x1', 'x2', 'x3', 'x4']
        # argnames = ['update', 'check']
        # argvalues = [(update1, check1), (update1, check2), (update2, check1), (update2, check2)]
        # metafunc.parametrize(argnames, argvalues, ids=idlist, scope="class")
        put({'idlist': list(idlist), 'argnames': argnames, 'argvalues': argvalues})
        metafunc.parametrize(argnames, argvalues, ids=idlist, scope="class")


@all_updaters_and_checkers
def test_ittmp(db, update, check):
    check(db)
    update(db)


@pytest.mark.parametrize('updater,updated_name', [
    # (update1, UPDATE1_NAME),
    (update2, UPDATE2_NAME),
])
def test_it(db, updater, updated_name):
    updater(db)
    put('\n\n\nupdated')

    # verify using core
    stmt = select([TABLE.c.id, TABLE.c.name]).where(TABLE.c.id == db.original_id)
    results = list(db.session.connection().execute(stmt))
    assert results == [(db.original_id, updated_name)]

    # verify using orm
    put('\nchecking name using sqlalchemy.core...')
    stmt = TABLE.select().with_only_columns([TABLE.c.name]).where(TABLE.c.id == db.original_id)
    current_name = db.session.connection().execute(stmt).scalar()
    assert current_name == updated_name
    put('completed checking name using sqlalchemy.core')

    put('\ngetting current name using session...')
    current_name = db.session.query(Thing).order_by(Thing.id).first().name
    put('finished getting current name')
    assert current_name == updated_name


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
