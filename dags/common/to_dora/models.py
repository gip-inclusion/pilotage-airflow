from typing import ClassVar

from sqlalchemy import Column
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.types import DateTime, String


# je l'appelle raw même si elle a été legerement modifié par pilotage
DB_SCHEMA = "raw_les_emplois"

# NOTE: when upgrading to sqlalchemy 2.0 or higher, we'll need to use the class DeclarativeBase
RawLesEmploisBase = declarative_base()


class Utilisateurs(RawLesEmploisBase):
    __tablename__ = "utilisateurs"
    __table_args__: ClassVar[dict[str, str]] = {"schema": DB_SCHEMA}

    id = Column(String)
    email = Column(String, index=True, primary_key=True)
    type = Column(String)
    dernière_connection = Column(DateTime)
    id_structure = Column(String, primary_key=True)
    id_organisation = Column(String, primary_key=True)
    id_institution = Column(String, primary_key=True)

    def __repr__(self) -> str:
        return (
            f"<Utilisateurs(id={self.id!r}, "
            f"email={self.email!r}, "
            f"type={self.type!r}, "
            f"dernière_connection={self.dernière_connection!r} )>"
        )
