from models.base import Base
from sqlalchemy import Column, Integer, String

class Group(Base):
    __tablename__ = 'groups'
    db_name = "silver"

    id = Column(Integer, primary_key=True)
    name = Column(String(50), unique=True)
