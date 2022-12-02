from __future__ import annotations
import typing as t

import sqlalchemy as sa
import numpy as np
import pandas as pd
from sqlalchemy.orm import relationship, backref
from sqlalchemy import event
from sqlalchemy import (
    Column,
    String,
    Float,
    ForeignKey,
    Integer,
    CheckConstraint,
    BigInteger,
    PrimaryKeyConstraint,
    DateTime,
)
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class Project(Base):

    __tablename__ = "project"

    project_id = Column(Integer, autoincrement=True)
    project_name = Column(String(256), unique=True, nullable=False)
    date_created = Column(DateTime, nullable=True)
    config_repr = Column(String, nullable=True)
    values_ = relationship("Value", backref="project", lazy="dynamic")
    anomalies_ = relationship("Anomaly", backref="project", lazy="dynamic")

    __table_args__ = (PrimaryKeyConstraint("project_id", name="project_pk"),)

    def create_entry(self, project_name: str):
        sa.insert(self).values(project_name=project_name)

    @staticmethod
    def return_if_exists(session, project_name: str) -> t.Union[Project, None]:
        entry = (
            session.query(Project).filter(Project.project_name == project_name).first()
        )
        return entry


class Value(Base):

    __tablename__ = "value"

    value_id = Column(Integer, autoincrement=True)
    project_id = Column(Integer, ForeignKey("project.project_id"), nullable=False)
    column_name = Column(String, nullable=False)
    date = Column(DateTime, nullable=False)
    metric = Column(String, nullable=False)
    arguments = Column(String, nullable=True)
    type = Column(String, nullable=False, default="custom")
    return_format = Column(String, nullable=False, default="float")
    batch_name = Column(String, nullable=False)
    run_name = Column(String, nullable=False)
    value = Column(String, nullable=True)
    insert_time = Column(DateTime, nullable=False)
    anomaly_model_id = Column(
        Integer, ForeignKey("anomaly_model.anomaly_model_id"), nullable=True
    )
    meta = Column(String, nullable=True)

    # model = relationship("Model", back_populates="values")
    anomalies = relationship("Anomaly", cascade="all, delete-orphan", backref="value")

    # project = relationship(Project, primaryjoin=project_id == Project.project_id)

    __table_args__ = (PrimaryKeyConstraint("value_id", name="value_pk"),)

    @staticmethod
    def delete_existing_batch(session, batch_name: str, project_id) -> None:
        to_delete = (
            session.query(Value)
            .filter(
                sa.and_(
                    Value.batch_name == str(batch_name),
                    Value.project_id == project_id,
                )
            )
            .all()
        )
        for value in to_delete:
            session.delete(value)


def validate_date_columns(target, value, oldvalue, initiator):
    if isinstance(value, np.datetime64):
        value = pd.to_datetime(value).to_pydatetime()
    return value


event.listen(Value.date, "set", validate_date_columns, retval=True)


class Anomaly(Base):

    __tablename__ = "anomaly"

    anomaly_id = Column(Integer, autoincrement=True)
    value_id = Column(Integer, ForeignKey("value.value_id"), nullable=False)
    project_id = Column(Integer, ForeignKey("project.project_id"), nullable=False)
    severity = Column(Float, nullable=True)
    trend_function_name = Column(String, nullable=True)

    __table_args__ = (PrimaryKeyConstraint("anomaly_id", name="anomaly_pk"),)


class AnomalyModel(Base):

    __tablename__ = "anomaly_model"

    anomaly_model_id = Column(Integer, autoincrement=True)
    model_blob = Column(sa.types.LargeBinary)
    model_type = Column(String)
    values = relationship("Value", backref="anomaly_model", lazy="dynamic")

    __table_args__ = (
        PrimaryKeyConstraint("anomaly_model_id", name="anomaly_model_pk"),
    )
