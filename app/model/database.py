import json
import logging
from datetime import datetime
from core.api import db_instance
from .models import IaCJob, IaCState, IaCJobTask
from sqlalchemy import or_

log = logging.getLogger("IaC:Database")

class JobDatabase:
    def _get_session(self):
        """Safely retrieves a database session if the central engine is connected."""
        if not db_instance.is_connected or not db_instance.SessionLocal:
            log.error("JobDatabase: Cannot get session, Core Database is disconnected.")
            return None
        return db_instance.SessionLocal()

    def create_job(self, pipeline_type: str) -> int:
        """Creates a new job record and returns its ID."""
        session = self._get_session()
        if not session:
            return -1

        try:
            new_job = IaCJob(
                pipeline_type=pipeline_type,
                status="RUNNING",
                progress=0,                      # NEW
                current_step="Pending Start",    # NEW
                logs="[]",
                pending_tasks="[]" 
            )
            session.add(new_job)
            session.commit()
            session.refresh(new_job)
            return new_job.id
        except Exception as e:
            log.error(f"Failed to create job in DB: {e}")
            session.rollback()
            return -1
        finally:
            if session:
                session.close()

    # Changed signature to remove logs_list
    def update_job(self, job_id: int, status: str):
        """Saves the final status to the database."""
        if job_id == -1:
            return

        session = self._get_session()
        if not session:
            return

        try:
            job = session.query(IaCJob).filter(IaCJob.id == job_id).first()
            if job:
                job.status = status
                if status in ["SUCCESS", "FAILED", "ERROR", "ABORTED"]: # Added ABORTED for the Kill Switch
                    job.end_time = datetime.now()
                    job.progress = 100 if status == "SUCCESS" else job.progress # Snap to 100% on success
                session.commit()
        except Exception as e:
            log.error(f"Failed to update job {job_id} in DB: {e}")
            session.rollback()
        finally:
            if session:
                session.close()

    def get_recent_jobs(self, limit: int = 20) -> list:
        """Fetches metadata for the UI table."""
        session = self._get_session()
        if not session:
            return []

        try:
            jobs = session.query(
                IaCJob.id,
                IaCJob.pipeline_type,
                IaCJob.start_time,
                IaCJob.end_time,
                IaCJob.status,
                IaCJob.progress
            ).order_by(IaCJob.id.desc()).limit(limit).all()

            return [
                {
                    "id": job.id,
                    "pipeline_type": job.pipeline_type,
                    "status": job.status,
                    "progress": job.progress or 0,
                    "start_time": job.start_time.strftime("%Y-%m-%d %H:%M:%S") if job.start_time else "N/A",
                    "end_time": job.end_time.strftime("%H:%M:%S") if job.end_time else "Running"
                }
                for job in jobs
            ]
        finally:
            if session:
                session.close()

    def get_latest_job_for_pipeline_type(self, pipeline_type: str, since_epoch: int = 0) -> dict | None:
        """Returns the newest job for an exact pipeline type, optionally after epoch seconds."""
        session = self._get_session()
        if not session:
            return None

        try:
            query = session.query(
                IaCJob.id,
                IaCJob.pipeline_type,
                IaCJob.status,
                IaCJob.progress,
                IaCJob.current_step,
                IaCJob.start_time,
                IaCJob.end_time,
            ).filter(IaCJob.pipeline_type == pipeline_type)

            if since_epoch and since_epoch > 0:
                # Allow a small skew window so CI/Orchestrator clock drift does not
                # hide the freshly created deployment job from the status endpoint.
                since_dt = datetime.fromtimestamp(max(0, since_epoch - 60))
                query = query.filter(
                    or_(
                        IaCJob.start_time >= since_dt,
                        IaCJob.end_time >= since_dt,
                    )
                )

            job = query.order_by(IaCJob.id.desc()).first()
            if not job:
                return None

            return {
                "id": job.id,
                "pipeline_type": job.pipeline_type,
                "status": job.status,
                "progress": job.progress or 0,
                "current_step": job.current_step or "",
                "start_time": job.start_time,
                "end_time": job.end_time,
            }
        finally:
            if session:
                session.close()

    def get_job_logs(self, job_id: int) -> list:
        """Fetches the raw log array for the popup window."""
        session = self._get_session()
        if not session:
            return ["Database connection lost."]

        try:
            job = session.query(IaCJob.logs).filter(IaCJob.id == job_id).first()
            if job and job.logs:
                return json.loads(job.logs)
            return ["No logs found."]
        finally:
            if session:
                session.close()

    def update_pending_tasks(self, job_id: int, pending_list: list):
        """Updates the queue of services yet to be deployed."""
        session = self._get_session()
        if not session:
            return

        try:
            job = session.query(IaCJob).filter(IaCJob.id == job_id).first()
            if job:
                job.pending_tasks = json.dumps(pending_list)
                session.commit()
        except Exception as e:
            log.error(f"Failed to update pending tasks for {job_id}: {e}")
            session.rollback()
        finally:
            if session:
                session.close()

    def get_pending_tasks(self, job_id: int) -> list:
        """Retrieves the surviving queue list for a specific job."""
        session = self._get_session()
        if not session:
            return []

        try:
            job = session.query(IaCJob).filter(IaCJob.id == job_id).first()
            if job and job.pending_tasks:
                return json.loads(job.pending_tasks)
            return []
        finally:
            if session:
                session.close()

    def get_jobs_by_status(self, status: str) -> list:
        """Finds all jobs currently in a specific state (e.g., RUNNING)."""
        session = self._get_session()
        if not session:
            return []

        try:
            return session.query(IaCJob).filter(IaCJob.status == status).all()
        except Exception as e:
            log.error(f"Failed to fetch jobs by status '{status}': {e}")
            return []
        finally:
            if session:
                session.close()

    def clear_all_jobs(self, keep_running: bool = True) -> int:
        """Deletes job history (the data behind the Overview statistics).

        By default keeps any currently RUNNING jobs so an active pipeline is not
        disrupted. Returns the number of deleted rows (-1 on failure).
        """
        session = self._get_session()
        if not session:
            return -1
        try:
            job_query = session.query(IaCJob)
            if keep_running:
                job_query = job_query.filter(IaCJob.status != "RUNNING")
            job_ids = [j.id for j in job_query.all()]
            if not job_ids:
                return 0
            # Delete child tasks first to satisfy the FK constraint.
            session.query(IaCJobTask).filter(IaCJobTask.job_id.in_(job_ids)).delete(synchronize_session=False)
            deleted = session.query(IaCJob).filter(IaCJob.id.in_(job_ids)).delete(synchronize_session=False)
            session.commit()
            log.info(f"Cleared {deleted} job record(s) from statistics (keep_running={keep_running}).")
            return int(deleted or 0)
        except Exception as e:
            log.error(f"Failed to clear job statistics: {e}")
            session.rollback()
            return -1
        finally:
            if session:
                session.close()

    # --- STATE MANAGEMENT METHODS ---

    def get_state(self, state_id: str) -> dict:
        """Fetches and decodes a state snapshot from the database."""
        session = self._get_session()
        if not session: return None
        try:
            state_record = session.query(IaCState).filter(IaCState.id == state_id).first()
            if state_record and state_record.state_data:
                return {
                    "data": json.loads(state_record.state_data),
                    "commit_hash": state_record.commit_hash
                }
            return None
        except Exception as e:
            log.error(f"Failed to get state '{state_id}': {e}")
            return None
        finally:
            if session: session.close()

    def update_state(self, state_id: str, new_state_data: dict, commit_hash: str):
        """Creates or updates a state snapshot in the database."""
        session = self._get_session()
        if not session: return
        try:
            state_record = session.query(IaCState).filter(IaCState.id == state_id).first()
            encoded_data = json.dumps(new_state_data)

            if state_record:
                state_record.state_data = encoded_data
                state_record.commit_hash = commit_hash
            else:
                new_record = IaCState(id=state_id, state_data=encoded_data, commit_hash=commit_hash)
                session.add(new_record)
            session.commit()
        except Exception as e:
            log.error(f"Failed to update state '{state_id}': {e}")
            session.rollback()
        finally:
            if session: session.close()
                
                
    def update_progress(self, job_id: int, progress: int = None, current_step: str = None):
        """Live updates the progress bar and current action text."""
        session = self._get_session()
        if not session or job_id == -1:
            return

        try:
            job = session.query(IaCJob).filter(IaCJob.id == job_id).first()
            if job:
                if progress is not None: 
                    job.progress = progress
                if current_step:
                    job.current_step = str(current_step)[:250] 
                session.commit()
        except Exception as e:
            log.error(f"Failed to update progress for job {job_id}: {e}")
            session.rollback()
        finally:
            if session:
                session.close()

    def insert_job_task(self, job_id: int, task_num: int, task_name: str, task_label: str, 
                       start_time: float, end_time: float, duration_ms: int, status: str, error: str = None):
        """Records a completed task within a job."""
        session = self._get_session()
        if not session or job_id == -1:
            return

        try:
            from datetime import datetime as dt
            task = IaCJobTask(
                job_id=job_id,
                task_num=task_num,
                task_name=task_name,
                task_label=task_label,
                start_time=dt.fromtimestamp(start_time) if start_time else None,
                end_time=dt.fromtimestamp(end_time) if end_time else None,
                duration_ms=duration_ms,
                status=status,
                error=error
            )
            session.add(task)
            session.commit()
        except Exception as e:
            log.error(f"Failed to insert job task for job {job_id}: {e}")
            session.rollback()
        finally:
            if session:
                session.close()
                
    def get_job_tasks_for_stats(self, limit: int = 500) -> list:
        """
        Fetch IaCJobTask rows for the most recent host_provision jobs.

        Returns task-level records so the lifecycle dashboard can correctly
        attribute Configure and Deploy phase counts to their sub-tasks rather
        than rolling everything up into a single Provision entry.
        """
        session = self._get_session()
        if not session:
            return []
        try:
            rows = (
                session.query(
                    IaCJobTask.task_name,
                    IaCJobTask.status,
                    IaCJobTask.start_time,
                    IaCJobTask.end_time,
                    IaCJobTask.duration_ms,
                )
                .join(IaCJob, IaCJobTask.job_id == IaCJob.id)
                .filter(IaCJob.pipeline_type.like("host_provision%"))
                .order_by(IaCJobTask.id.desc())
                .limit(limit)
                .all()
            )
            return [
                {
                    "task_name": r.task_name,
                    "status": r.status,
                    "start_time": r.start_time,
                    "end_time": r.end_time,
                    "duration_ms": r.duration_ms or 0,
                }
                for r in rows
            ]
        finally:
            if session:
                session.close()

    def get_jobs_for_stats(self, limit: int = 500) -> list:
        """
        Fetch a lightweight, raw-typed slice of recent jobs for statistics.

        Unlike :meth:`get_recent_jobs` (which pre-formats timestamps for tables),
        this returns native datetimes so callers can compute durations and trends.
        """
        session = self._get_session()
        if not session:
            return []

        try:
            jobs = session.query(
                IaCJob.id,
                IaCJob.pipeline_type,
                IaCJob.status,
                IaCJob.progress,
                IaCJob.start_time,
                IaCJob.end_time,
            ).order_by(IaCJob.id.desc()).limit(limit).all()

            return [
                {
                    "id": job.id,
                    "pipeline_type": job.pipeline_type,
                    "status": job.status,
                    "progress": job.progress or 0,
                    "start_time": job.start_time,
                    "end_time": job.end_time,
                }
                for job in jobs
            ]
        finally:
            if session:
                session.close()

    def get_service_history(self, service_name: str, limit: int = 15) -> list:
        """Fetches recent jobs involving a specific service with strict filtering."""
        session = self._get_session()
        if not session: return []
        try:
            # Search for the service name in the type string OR inside the pending_tasks JSON blob
            search = f"%{service_name}%"
            jobs = session.query(IaCJob).filter(
                or_(IaCJob.pipeline_type.like(search), IaCJob.pending_tasks.like(search))
            ).order_by(IaCJob.id.desc()).limit(limit).all()

            return [{
                "id": j.id,
                "pipeline_type": j.pipeline_type,
                "status": j.status,
                "progress": j.progress or 0,
                "start_time": j.start_time.strftime("%Y-%m-%d %H:%M:%S") if j.start_time else "N/A"
            } for j in jobs]
        finally:
            if session: session.close()
