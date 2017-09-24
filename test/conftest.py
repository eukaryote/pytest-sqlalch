import pprint
import sys
from types import SimpleNamespace

import pytest

from sqlalchemy import event, inspect
from sqlalchemy.exc import InternalError, ResourceClosedError
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.session import Session as OrmSession

from .models import Base, Thing, make_engine

DEFAULT_NAME = 'name-default'
DEFAULT_CREATED_BY = 'user-default'

EXTRA_NAME = 'name-extra'
EXTRA_CREATED_BY = 'user-extra'

Session = sessionmaker()


def put(*args, **kwargs):
    if 'file' not in kwargs:
        kwargs['file'] = sys.stderr
    if kwargs['file'] is not sys.stderr and 'flush' not in kwargs:
        kwargs['flush'] = True
    print(*args, **kwargs)


def debug_tx(sess, trans):
    """Print debug info for session and all transactions in chain."""

    def debug(tx):
        """Show debug info for all transactions in chain."""
        info = {
            'tx': tx,
            'tx.nested': tx.nested,
            'tx._parent': tx._parent,
        }
        put(pprint.pformat(info))
        if tx._parent:
            debug(tx._parent)

    put('\n\n', pprint.pformat({'session': sess}))
    debug(trans)


@pytest.fixture(scope='session')
def engine():
    return make_engine(echo=False)


@pytest.fixture(scope='session')
def db(engine):
    """
    Fixture that does one-time setup and teardown of db tables.

    Transation management and rolling back after each test is provided
    via the `session` fixture, which is autouse in order to greatly
    simplify transaction handling needed to support all the corner
    cases that are tested by the test suite.
    """

    with engine.connect() as conn:
        Base.metadata.create_all(conn)
    try:
        with engine.connect() as conn:
            s = Session(bind=conn)
            with conn.begin() as tx:
                kw = dict(
                    name=DEFAULT_NAME,
                    created_by=DEFAULT_CREATED_BY
                )
                assert s.query(Thing).count() == 0
                obj = Thing(**kw)
                s.add(obj)
                s.flush()
                # stmt = Thing.__table__.insert().values(**kw)
                # conn.execute(stmt)
                # obj = s.query(Thing).filter_by(**kw).one()

            assert inspect(obj).session is s
            assert OrmSession.object_session(obj) is s

        # with engine.connect() as conn:
        #     s = Session(bind=conn)
        #     obj = s.query(Thing).filter_by(**kw).one()
        #     assert s.query(Thing).count() == 1

            yield SimpleNamespace(
                engine=engine,
                session=s,
                connection=conn,
                obj=obj,
                original_id=obj.id,
                original_name=obj.name,
                original_created_by=obj.created_by,
            )
    finally:
        Base.metadata.drop_all(engine)
        engine.dispose()


@pytest.fixture
def orm_object_from_session_fixture(session):
    return session.query(Thing).one()


@pytest.fixture
def orm_object_from_db_fixture(db):
    return db.session.query(Thing).one()


@pytest.fixture
def original_id(db):
    return db.original_id



@pytest.fixture
def original_name(db):
    return db.original_name


@pytest.fixture
def original_created_by(db):
    return db.original_created_by


@pytest.fixture(scope='function')
def obj(db):
    return db.obj


@pytest.fixture(scope='function', autouse=True)
def session(db):
    conn = db.connection
    s = db.session

    # with conn.begin():
    #     assert s.query(Thing).count() == 1
    #     assert s.query(Thing).first().name == DEFAULT_NAME

    tx_base = conn.begin()
    tx_sub = conn.begin_nested()
    tx_session = s.begin_nested()

    # keep track of whether session transaction has ended
    tx_session_ended = False

    @event.listens_for(s, 'after_transaction_end')
    def after_transaction_end(sess, tx):
        nonlocal tx_session, tx_session_ended
        if tx is tx_session:
            tx_session_ended = True
        if tx.nested and not tx._parent.nested:
            tx_session = s.begin_nested()

    yield s

    # print('\ninfo:', pprint.pformat({
    #     'tx_session_ended': tx_session_ended,
    #     'conn.info': conn.info,
    #     'conn.in_transaction()': conn.in_transaction(),
    #     'tx_base.is_active': tx_base.is_active,
    #     'tx_sub.is_active': tx_sub.is_active,
    #     'tx_session.is_active': tx_session.is_active,
    #     's._is_clean()': s._is_clean(),
    #     's.is_active': s.is_active,
    # }), file=sys.stderr)

    if not tx_session_ended:
        s.rollback()

    tx_base.rollback()
