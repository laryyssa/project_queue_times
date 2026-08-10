from models.base import Base
from sqlalchemy import Column, Integer, String, ForeignKey


class Park(Base):
    __tablename__ = 'parks'
    db_name = "silver"

    id = Column(Integer, primary_key=True)
    name = Column(String(50))
    country = Column(String(50))
    continent = Column(String(50))
    latitude = Column(String(50))
    longitude = Column(String(50))
    timezone = Column(String(50))

    group_id = Column(
        Integer, 
        ForeignKey('groups.id'), 
        nullable=False
    )  