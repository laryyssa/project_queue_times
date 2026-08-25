from models.base import Base
from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.orm import relationship

class Land(Base):
    __tablename__ = 'lands'
    db_name = "silver"
    
    id = Column(Integer, primary_key=True)
    name = Column(String(255))

    park_id = Column(
        Integer, 
        ForeignKey("parks.id"), 
        nullable=False
    )
    