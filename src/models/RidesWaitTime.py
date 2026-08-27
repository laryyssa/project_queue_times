from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, ForeignKey, Boolean
from models.base import Base

class RideWaitTimes(Base):
    __tablename__ = 'rides_wait_times'
    db_name = "silver"

    id = Column(Integer, primary_key=True, autoincrement=True)
    wait_time = Column(Integer, nullable=True)
    last_updated = Column(String(50), nullable=False)
    is_open = Column(Boolean, nullable=True)

    ride_id = Column(
        Integer, 
        ForeignKey("rides.id"), 
        nullable=False
    )