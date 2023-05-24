import os
from datetime import datetime
from sqlalchemy import create_engine, Integer, String, Column, DateTime, Numeric, Date
from sqlalchemy.ext.declarative import declarative_base


basedir = os.path.abspath(os.path.dirname(__file__))
path = 'sqlite:///' + os.path.join(basedir, 'app_data.db')
engine = create_engine(path)
Base = declarative_base()


class TableInfo(Base):
    __tablename__ = 'table_info'
    id = Column(Integer(), primary_key=True)
    name_t = Column(String(100), nullable=False)
    name_t_ish = Column(String(100), nullable=False)
    date_start = Column(DateTime(), nullable=False)
    date_end = Column(DateTime(), nullable=False)
    info = Column(String(200), nullable=False)
    param1 = Column(Numeric(10, 2))
    info_param1 = Column(String(100))
    param2 = Column(Numeric(10, 2))
    info_param2 = Column(String(100))
    param3 = Column(Numeric(10, 2))
    info_param3 = Column(String(100))
    param4 = Column(Numeric(10, 2))
    info_param4 = Column(String(100))
    created_on = Column(Date(), default=datetime.now)

    def __repr__(self):
        return f"""{self.id}\t{self.name_t_ish}\t{self.name_t}\t{self.date_start}\t{self.date_end}\t{self.info}\t{self.created_on}
{self.param1}\t{self.info_param1}\t
{self.param2}\t{self.info_param2}\t
{self.param3}\t{self.info_param3}\t
{self.param4}\t{self.info_param4}\t
"""


if __name__ == "__main__":
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)