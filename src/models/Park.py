from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, ForeignKey

Base = declarative_base()

class Park(Base):
    __tablename__ = 'parks'

    id = Column(Integer, primary_key=True)
    name = Column(String(50), unique=True)
    country = Column(String(50))
    continent = Column(String(50))
    latitude = Column(String(50))
    longitude = Column(String(50))
    timezone = Column(String(50))
    group_id = Column(Integer, ForeignKey('groups.id'), nullable=False)  