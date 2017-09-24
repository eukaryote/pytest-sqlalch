from sqlalchemy import create_engine, Column, Integer, String, inspect
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.session import Session
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.schema import UniqueConstraint

CONNECT_URL =  "postgresql://example:example@172.18.0.2:5432/example"

Base = declarative_base()


class Thing(Base):

    __tablename__ = 'thing'
    __table_args__ = (
        UniqueConstraint(
            'name', 'created_by', name='thing_name_created_by_uc'
        ),
    )

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    created_by = Column(String)

    def exists(self):
        # session = inspect(self).session
        session = Session.object_session(self)
        return bool(session.query(type(self)).filter_by(id=self.id).first())

    def __str__(self):
        return '<Thing (id=%s, name="%s", created_by="%s")>' % (
            self.id, self.name, self.created_by
        )

    __repr__ = __str__


def make_engine(echo=True):
    engine = create_engine(CONNECT_URL, echo=echo)
    return engine
