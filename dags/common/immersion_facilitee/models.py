import sqlalchemy
from sqlalchemy import CheckConstraint, Column
from sqlalchemy.dialects.postgresql import ARRAY, UUID
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.types import DateTime, String

from dags.common import db


DB_SCHEMA = "immersion_facilitee"

# NOTE: when upgrading to sqlalchemy 2.0 or higher, we'll need to use the class DeclarativeBase

ImmersionFaciliteeBase = declarative_base()


def check_siret(col_name: str) -> sqlalchemy.CheckConstraint:
    """
    Check constraint for SIRET numbers.
    SIRET numbers must be 14 digits long.
    """
    return CheckConstraint("siret ~ '^[0-9]{14}$'", name=f"check_siret_{col_name}")


def create_tables():
    db.create_schema(DB_SCHEMA)
    ImmersionFaciliteeBase.metadata.create_all(db.connection_engine())


class Conventions(ImmersionFaciliteeBase):

    __tablename__ = "conventions"
    __table_args__ = {"schema": DB_SCHEMA}

    id = Column(UUID(as_uuid=True), primary_key=True)

    status = Column(String)
    statusJustification = Column(String, nullable=True)
    agencyId = Column(String)
    dateSubmission = Column(DateTime)
    dateStart = Column(DateTime)
    dateEnd = Column(DateTime)
    dateApproval = Column(DateTime)
    dateValidation = Column(DateTime, nullable=True)
    siret = Column(String, check_siret("siret"))
    immersionObjective = Column(String)
    immersionAppellationRomeCode = Column(String)
    immersionAppellationRomeLabel = Column(String)
    immersionAppellationAppellationCode = Column(String)
    immersionAppellationAppellationLabel = Column(String)
    immersionActivities = Column(String)
    immersionSkills = Column(String)
    establishmentNumberEmployeesRange = Column(String)
    internshipKind = Column(String)
    agencyName = Column(String)
    agencyDepartment = Column(String)
    agencyKind = Column(String)
    agencySiret = Column(String, check_siret("agencySiret"))
    beneficiaryPIIHashes = Column(ARRAY(String))

    def __repr__(self) -> str:
        return (
            f"<Conventions(id={self.id!r}, "
            f"status={self.status!r}, "
            f"dateApproval={self.dateApproval!r}, "
            f"agencyName={self.agencyName!r}, "
            f"agencySiret={self.agencySiret!r}, "
            f"agencyDepartment={self.agencyDepartment!r}, "
            f"businessName={self.businessName!r}, "
            f"siret={self.siret!r} )>"
        )

    @classmethod
    def primary_key_columns(cls):
        return [pk_column.name for pk_column in cls.__table__.primary_key.columns]
