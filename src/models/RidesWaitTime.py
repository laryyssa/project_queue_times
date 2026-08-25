from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, ForeignKey, Boolean

Base = declarative_base()

class RideWaitTimes(Base):
    __tablename__ = 'rides_wait_times'
    db_name = "silver"

    id = Column(Integer, primary_key=True)
    ride_id = Column(Integer, ForeignKey("rides.id"), nullable=False)
    wait_time = Column(Integer, nullable=False)
    timestamp = Column(String(50), nullable=False)
    is_open = Column(Boolean, nullable=False)
