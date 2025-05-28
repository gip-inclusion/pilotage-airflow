from sqlalchemy import CheckConstraint, Column
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.types import DateTime, String
from sqlalchemy.dialects.postgresql import ARRAY

from dags.common import db


DB_SCHEMA = "immersion_facilitee"

# NOTE: when upgrading to sqlalchemy 2.0 or higher, we'll need to use the class DeclarativeBase

ImmersionFaciliteeBase = declarative_base()


def create_tables():
    db.create_schema(DB_SCHEMA)
    ImmersionFaciliteeBase.metadata.create_all(db.connection_engine())


class Conventions(ImmersionFaciliteeBase):

    __tablename__ = "conventions"
    __table_args__ = (
        CheckConstraint("char_length(siret)=14 AND siret ~ '^[0-9]{14}$'", name="siret_check"),
        CheckConstraint(
            'char_length("agencySiret")=14 AND "agencySiret" ~ \'^[0-9]{14}$\'', name="agency_siret_check"
        ),
        {"schema": DB_SCHEMA},
    )

    id = Column(String, primary_key=True)

    status = Column(String)
    statusJustification = Column(String)
    agencyId = Column(String)
    dateSubmission = Column(DateTime)
    dateStart = Column(DateTime)
    dateEnd = Column(DateTime)
    dateApproval = Column(DateTime)
    dateValidation = Column(DateTime, nullable=True)
    siret = Column(String(length=14))
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
    agencySiret = Column(String(length=14))
    agencyRefersToId = Column(String)
    agencyRefersToName = Column(String)
    agencyRefersToKind = Column(String)
    updatedAt = Column(DateTime)
    beneficiaryId = Column(ARRAY(String))

    def __repr__(self) -> str:
        return (
            f"<Conventions(id={self.id!r}, "
            f"status={self.status!r}, "
            f"dateApproval={self.dateApproval!r}, "
            f"agencyName={self.agencyName!r}, "
            f"agencySiret={self.agencySiret!r}, "
            f"agencyDepartment={self.agencyDepartment!r}, "
            f"businessName={self.businessName!r}, "
            f"siret={self.siret!r}, "
        )

    @classmethod
    def primary_key_columns(cls):
        return [pk_column.name for pk_column in cls.__table__.primary_key.columns]
