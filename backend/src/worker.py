import time, json, logging
from pathlib import Path
from sqlalchemy import select
from redis import Redis
from src.config.database import Session
from src.config.settings import REDIS_URL
from src.models import Job, Dataset, Report
from src.services.business import analyze
from src.services import reports as delivery_service
from src.services.audit import record
logging.basicConfig(level=logging.INFO)
def execute(job_id):
    with Session() as db:
        j=db.get(Job,job_id)
        if not j or j.status!='queued':return
        j.status='running'; db.commit()
        try:
            d=db.get(Dataset,j.dataset_id)
            result=analyze(d.records,j.params)
            json.dumps(result,allow_nan=False)
            j.result=result;j.status='completed';j.finished_at=time.time()
            record(db,'worker','job.completed',{'id':j.id})
        except Exception as e:
            logging.exception('Analysis failed')
            j.status='failed';j.error=str(e)[:990];j.finished_at=time.time()
            record(db,'worker','job.failed',{'id':j.id,'error':j.error})
        db.commit()
def dispatch_due(report_id=None):
    """Deliver each due batch once. Failures stay recorded with their reason and due time."""
    with Session() as db:
        query=select(Report.id).where(Report.enabled==True,Report.next_run<=time.time())
        if report_id is not None:query=query.where(Report.id==report_id)
        due=list(db.scalars(query.limit(20)))
    for rid in due:
        outcome=delivery_service.dispatch(rid)
        if not outcome:continue
        with Session() as db:
            if outcome['status']=='delivered':
                record(db,'worker','report.delivered',{'id':rid,'delivery':outcome['id']})
            elif outcome['status']=='failed':
                record(db,'worker','report.failed',{'id':rid,'delivery':outcome['id'],'error':outcome['error']})
            db.commit()
def tick():
    with Session() as db:
        queued=list(db.scalars(select(Job.id).where(Job.status=='queued').limit(3)))
    for job_id in queued:execute(job_id)
    dispatch_due()
def main():
    r=Redis.from_url(REDIS_URL,socket_timeout=3)
    # This Compose stack has a single worker. A crash must not strand its jobs or
    # leave a half-finished brief: interrupted batches return to pending and are
    # completed by the next tick under the same (report, batch time) key.
    with Session() as db:
        for j in db.scalars(select(Job).where(Job.status=='running')):
            j.status='queued';record(db,'worker','job.recovered',{'id':j.id})
        db.commit()
    recovered=delivery_service.recover()
    if recovered:logging.info('Recovered %s interrupted delivery batch(es)',recovered)
    while True:
        Path('/tmp/heartbeat').touch()
        try:
            item=r.blpop('analysis:queue',timeout=1)
            if item:execute(int(item[1]))
            tick()
        except Exception:
            logging.exception('Worker retry');time.sleep(2)
if __name__=='__main__':main()
