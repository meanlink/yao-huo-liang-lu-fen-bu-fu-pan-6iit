from sqlalchemy import String, Double, ForeignKey, Boolean, Integer, JSON, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column
from src.config.database import Base
class Report(Base):
    __tablename__ = 'reports'
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(160))
    job_id: Mapped[int] = mapped_column(ForeignKey('jobs.id'))
    destination: Mapped[str] = mapped_column(String(120))
    interval_seconds: Mapped[int]
    next_run: Mapped[float] = mapped_column(Double)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)
class Delivery(Base):
    __tablename__ = 'deliveries'
    __table_args__ = (UniqueConstraint('report_id', 'scheduled_for', name='uq_delivery_batch'),)
    id: Mapped[int] = mapped_column(primary_key=True)
    report_id: Mapped[int] = mapped_column(ForeignKey('reports.id'))
    scheduled_for: Mapped[float] = mapped_column(Double, default=0.0)
    status: Mapped[str] = mapped_column(String(20), default='pending')
    attempts: Mapped[int] = mapped_column(Integer, default=0)
    lease_owner: Mapped[str] = mapped_column(String(64), default='')
    lease_until: Mapped[float] = mapped_column(Double, default=0.0)
    started_at: Mapped[float | None] = mapped_column(Double, nullable=True)
    created_at: Mapped[float] = mapped_column(Double)
    finished_at: Mapped[float | None] = mapped_column(Double, nullable=True)
    checksum: Mapped[str] = mapped_column(String(64), default='')
    filename: Mapped[str] = mapped_column(String(160), default='')
    destination: Mapped[str] = mapped_column(String(120))
    error: Mapped[str] = mapped_column(String(1000), default='')
    snapshot: Mapped[dict] = mapped_column(JSON, default=dict)
