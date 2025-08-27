# ruff: noqa: N815 (mixed-case-variable-in-class-scope)

from sqlalchemy import Column
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.types import DECIMAL, DateTime, Integer, String

from dags.common import db


DB_SCHEMA = "france_travail"


# NOTE: when upgrading to sqlalchemy 2.0 or higher, we'll need to use the class DeclarativeBase
FranceTravailBase = declarative_base()


def create_tables():
    db.create_schema(DB_SCHEMA)
    FranceTravailBase.metadata.create_all(db.connection_engine())


class JobSeekerStats(FranceTravailBase):
    """
    Contains statistics on job-seeker by region and characteristic.
    Imported from the France Travail API
    https://francetravail.io/produits-partages/catalogue/marche-travail/documentation#/api-reference/operations/rechercherStatDemandeurs
    """

    __tablename__ = "job_seeker_stats"
    __table_args__ = {"schema": DB_SCHEMA}

    # Primary key fields.
    codeNomenclature = Column(String, primary_key=True)
    codeTypeCaract = Column(String, primary_key=True)
    codeCaract = Column(String, primary_key=True)
    codeTypeTerritoire = Column(String, primary_key=True)
    codeTerritoire = Column(String, primary_key=True)
    codePeriode = Column(String, primary_key=True)

    codeActivite = Column(String)
    codeTypeActivite = Column(String)
    codeTypePeriode = Column(String)
    datMaj = Column(DateTime)
    libActivite = Column(String)
    libCaract = Column(String)
    libNomenclature = Column(String)
    libPeriode = Column(String)
    libTerritoire = Column(String)
    nombre = Column(Integer)
    pourcentage = Column(DECIMAL)

    def __repr__(self) -> str:
        return (
            f"<JobSeekerStats(codeCaract={self.codeCaract!r}, "
            f"codeNomenclature={self.codeNomenclature!r}, "
            f"codePeriode={self.codePeriode!r}, "
            f"codeTerritoire={self.codeTerritoire!r}, "
            f"codeTypeCaract={self.codeTypeCaract!r}, "
            f"codeTypeTerritoire={self.codeTypeTerritoire!r})>"
        )

    @classmethod
    def primary_key_columns(cls):
        return [pk_column.name for pk_column in cls.__table__.primary_key.columns]
