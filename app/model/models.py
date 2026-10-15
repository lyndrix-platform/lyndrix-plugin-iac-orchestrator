from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Text
from sqlalchemy.sql import func
from sqlalchemy.orm import declarative_base
from sqlalchemy.dialects.mysql import LONGTEXT

Base = declarative_base()

# LONGTEXT on MariaDB/MySQL (as before), plain TEXT elsewhere (SQLite in tests).
LONGTEXT = Text().with_variant(LONGTEXT, "mysql", "mariadb")

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

    # --- CLUSTER QUEUE (plugin 1.4): control plane enqueues, runner instances claim ---
    payload = Column(LONGTEXT, nullable=True)            # trigger payload (JSON) for the executor
    dedupe_key = Column(String(255), nullable=True)      # concurrency key (single_service:<svc>, rollout:all, …)
    owner_instance = Column(String(100), nullable=True)  # LYNDRIX_INSTANCE_ID of the executing runner
    claimed_at = Column(DateTime(timezone=True), nullable=True)
    heartbeat_at = Column(DateTime(timezone=True), nullable=True)
    runners = Column(LONGTEXT, nullable=True)            # live runner/task snapshot (JSON) for every instance's UI


class IaCJobLog(Base):
    """One log line per row — written by the executing runner, read by every instance.

    The job log used to be a file on the runner host (``logs_dir/job_<id>.log``);
    the file is still written for debugging, but the UI reads from here so the
    control plane can show logs regardless of which instance answers.
    """
    __tablename__ = "iac_orchestrator_job_logs"

    id = Column(Integer, primary_key=True)
    job_id = Column(Integer, ForeignKey("iac_orchestrator_jobs.id"), nullable=False, index=True)
    seq = Column(Integer, nullable=False)
    ts = Column(DateTime(timezone=True), server_default=func.now())
    line = Column(LONGTEXT, nullable=False)

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