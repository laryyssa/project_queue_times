import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from models.base import Base
from models.Lands import Land
from models.Rides import Ride
# from models.Parks import Park

from utils.db import get_engine

if __name__ == "__main__":
    engine = get_engine("silver")
    Base.metadata.create_all(engine)
    print("Tabelas criadas:", list(Base.metadata.tables.keys()))