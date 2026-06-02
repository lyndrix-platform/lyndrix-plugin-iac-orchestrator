from sqlalchemy import Column, Integer, String, DateTime, ForeignKey
from sqlalchemy.sql import func
from sqlalchemy.orm import declarative_base
from sqlalchemy.dialects.mysql import LONGTEXT 

Base = declarative_base()

class IaCJob(Base):
    __tablename__ = "iac_orchestrator_jobs"
    
    id = Column(Integer, primary_key=True, index=True)
    pipeline_type = Column(String(50), nullable=False)
    status = Column(String(20), default="RUNNING")
    
    # --- NEW PROGRESS TRACKING COLUMNS ---
    progress = Column(Integer, default=0)
    current_step = Column(String(255), default="Initializing")
    
    start_time = Column(DateTime(timezone=True), server_default=func.now())
    end_time = Column(DateTime(timezone=True), nullable=True)
    
    # We will keep 'logs' temporarily to avoid breaking old records, 
    # but we won't use it for new jobs. 
    logs = Column(LONGTEXT, default="[]") 
    pending_tasks = Column(LONGTEXT, default="[]")

class IaCJobTask(Base):
    __tablename__ = "iac_orchestrator_job_tasks"
    
    id = Column(Integer, primary_key=True, index=True)
    job_id = Column(Integer, ForeignKey("iac_orchestrator_jobs.id"), nullable=False)
    task_num = Column(Integer, nullable=False)
    task_name = Column(String(100), nullable=False)
    task_label = Column(String(255), nullable=False)
    start_time = Column(DateTime(timezone=True), nullable=True)
    end_time = Column(DateTime(timezone=True), nullable=True)
    duration_ms = Column(Integer, default=0)
    status = Column(String(20), default="pending")
    error = Column(LONGTEXT, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

class IaCState(Base):
    __tablename__ = "iac_orchestrator_state"

    # A unique ID for the state, e.g., "last_known_good"
    id = Column(String(100), primary_key=True)
    
    # The full state snapshot as a JSON string
    state_data = Column(LONGTEXT, nullable=False)
    
    # The git commit hash from iac_controller this state corresponds to
    commit_hash = Column(String(40), nullable=True)
    
    last_updated = Column(DateTime(timezone=True), onupdate=func.now(), server_default=func.now())