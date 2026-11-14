import time, os, csv, io, hashlib, json, logging, uuid
from fastapi import HTTPException
from sqlalchemy import select, update, and_, or_
from sqlalchemy.exc import IntegrityError
from src.config.database import Session
from src.config.settings import ARTIFACT_DIR
from src.models import Job, Report, Delivery

# A processing lease covers one small archive write. A second worker may only take
# over a claimed batch after the lease expires; the (report, batch time) unique key
# still guarantees exactly one delivery row per batch.
LEASE_SECONDS = int(os.getenv('DELIVERY_LEASE_SECONDS', '60'))


class Paused(Exception):
    pass


def create(db, data):
    j=db.get(Job,data.job_id)
    if not j or j.status!='completed' or not j.approved:
        raise HTTPException(409,'请先选择已完成且获批准的分析结果')
    r=Report(name=data.name, job_id=j.id, destination=data.destination, interval_seconds=data.interval_seconds, next_run=time.time()+data.delay_seconds, enabled=True)
    db.add(r); db.flush()
    return r


def resume(db, report):
    """After a pause, skip missed slots instead of flooding the inbox with stale briefs."""
    now=time.time()
    # A batch interrupted by a pause or a crash must not linger as a deliverable
    # backlog once the cadence moves on.
    db.execute(update(Delivery).where(Delivery.report_id==report.id,Delivery.status=='pending',
                                      Delivery.scheduled_for<=now)
               .values(status='skipped',error='计划暂停期间未分发',finished_at=now,lease_owner=''))
    if report.next_run<=now:
        report.next_run=now+report.interval_seconds


def _next_run(batch_time, interval, now):
    nxt=batch_time+interval
    # A late batch is not replayed as a backlog; the cadence resumes from now.
    return nxt if nxt>now else now+interval


def _snapshot(report, job, checksum, batch_time):
    return {'report': {'id': report.id, 'name': report.name, 'destination': report.destination},
            'job': {'id': job.id, 'name': job.name, 'approved': job.approved, 'params': job.params},
            'result': job.result, 'checksum': checksum, 'scheduled_for': batch_time,
            'generated_at': time.time()}


def _csv_bytes(job):
    out=io.StringIO()
    out.write('﻿')
    table=(job.result or {}).get('table',{})
    cols=table.get('columns',[])
    writer=csv.writer(out)
    def safe(v):
        if isinstance(v,(int,float)) and not isinstance(v,bool): return v
        text=str(v)
        return "'"+text if text.startswith(('=','+','-','@','\t','\r')) else text
    writer.writerow([safe(c['label']) for c in cols])
    for row in table.get('rows',[]): writer.writerow([safe(row.get(c['key'],'')) for c in cols])
    return out.getvalue().encode('utf-8')


def _write_atomic(path, data):
    os.makedirs(os.path.dirname(path),exist_ok=True)
    tmp=path+f'.tmp-{os.getpid()}-{uuid.uuid4().hex}'
    try:
        with open(tmp,'wb') as f:
            f.write(data);f.flush();os.fsync(f.fileno())
        os.replace(tmp,path)
    finally:
        if os.path.exists(tmp):
            try:os.remove(tmp)
            except OSError:pass


def _claim(report_id, batch_time=None, retry=False):
    """Create or find the delivery row for one scheduled batch and claim it atomically.

    The unique (report_id, scheduled_for) key is the idempotency guarantee: a restart,
    a retried tick or a second worker can never produce a second brief for one batch.
    """
    with Session() as db:
        report=db.execute(select(Report).where(Report.id==report_id).with_for_update()).scalar_one_or_none()
        if not report:return None
        if batch_time is None:batch_time=report.next_run
        if not report.enabled:return None
        now=time.time()
        if batch_time>now+0.001 and not retry:return None
        d=db.scalar(select(Delivery).where(Delivery.report_id==report_id,Delivery.scheduled_for==batch_time))
        if d is None:
            d=Delivery(report_id=report_id,scheduled_for=batch_time,status='pending',attempts=0,
                       created_at=now,destination=report.destination)
            db.add(d)
            try:
                db.flush();db.commit()
            except IntegrityError:
                # Another worker inserted the same batch first; use its row.
                db.rollback()
                d=db.scalar(select(Delivery).where(Delivery.report_id==report_id,Delivery.scheduled_for==batch_time))
                if d is None:return None
        if d.status=='delivered':
            if abs(report.next_run-batch_time)<1e-9:
                report.next_run=_next_run(batch_time,report.interval_seconds,now)
                db.commit()
            return None
        if d.status=='failed' and not retry:return None
        if d.status=='processing' and (d.lease_until or 0)>now:return None
        token=uuid.uuid4().hex
        result=db.execute(update(Delivery).where(
            Delivery.id==d.id,
            or_(Delivery.status.in_(('pending','failed')),
                and_(Delivery.status=='processing',Delivery.lease_until<=now))
        ).values(status='processing',attempts=Delivery.attempts+1,lease_owner=token,
                 lease_until=now+LEASE_SECONDS,started_at=now,finished_at=None,error=''))
        db.commit()
        if result.rowcount!=1:return None
        return {'report_id':report_id,'delivery_id':d.id,'batch_time':batch_time,'token':token}


def _settle(delivery_id, token, values):
    with Session() as db:
        result=db.execute(update(Delivery).where(Delivery.id==delivery_id,Delivery.status=='processing',
                                                 Delivery.lease_owner==token).values(**values))
        db.commit()
        return result.rowcount==1


def dispatch(report_id, batch_time=None, retry=False):
    """Deliver at most one batch. Returns {'id','status'} or None when nothing was due.

    Files are written atomically before the delivery row and the next run time are
    committed together in one transaction; an interruption in either step is recoverable
    and re-running the same batch rewrites the same files instead of creating another brief.
    """
    claim=_claim(report_id,batch_time,retry)
    if not claim:return None
    delivery_id, token, batch_time = claim['delivery_id'], claim['token'], claim['batch_time']
    filename=f'report-{claim["report_id"]}-batch-{delivery_id}.csv'
    try:
        with Session() as db:
            report=db.get(Report,claim['report_id'])
            if not report:raise ValueError('分发计划不存在')
            job=db.get(Job,report.job_id)
            if not job or job.status!='completed' or not job.approved:
                raise ValueError('原分析结果未完成或未获批准，无法分发')
            if not report.enabled:raise Paused('计划已暂停，本批次未分发')
            data=_csv_bytes(job)
            checksum=hashlib.sha256(data).hexdigest()
            snapshot=_snapshot(report,job,checksum,batch_time)
            _write_atomic(os.path.join(ARTIFACT_DIR,filename),data)
            _write_atomic(os.path.join(ARTIFACT_DIR,filename+'.json'),
                          json.dumps(snapshot,ensure_ascii=False,allow_nan=False).encode('utf-8'))
        with Session() as db:
            report=db.execute(select(Report).where(Report.id==claim['report_id']).with_for_update()).scalar_one_or_none()
            now=time.time()
            updated=db.execute(update(Delivery).where(Delivery.id==delivery_id,Delivery.status=='processing',
                                                      Delivery.lease_owner==token)
                               .values(status='delivered',checksum=checksum,filename=filename,
                                       snapshot=snapshot,finished_at=now,error='',lease_owner=''))
            if updated.rowcount==1 and report and abs(report.next_run-batch_time)<1e-9:
                # Advance only after this batch is durably delivered; a failed batch
                # keeps its due time so the schedule does not silently skip it.
                report.next_run=_next_run(batch_time,report.interval_seconds,now)
            db.commit()
            if updated.rowcount!=1:return None
        return {'id':delivery_id,'status':'delivered'}
    except Paused as e:
        _settle(delivery_id,token,{'status':'skipped','error':str(e)[:990],'finished_at':time.time(),'lease_owner':''})
        return {'id':delivery_id,'status':'skipped'}
    except Exception as e:
        logging.exception('Delivery failed for report %s batch %s',claim['report_id'],batch_time)
        _settle(delivery_id,token,{'status':'failed','error':str(e)[:990],'finished_at':time.time(),'lease_owner':''})
        return {'id':delivery_id,'status':'failed','error':str(e)[:990]}


def retry(delivery_id):
    """Re-run one failed batch under the same key and the same file name."""
    with Session() as db:
        d=db.get(Delivery,delivery_id)
        if not d:raise HTTPException(404,'归档批次不存在')
        if d.status=='delivered':raise HTTPException(409,'该批次已交付，无需重试')
        if d.status=='processing':raise HTTPException(409,'该批次正在交付，请稍后刷新')
        if d.status!='failed':raise HTTPException(409,'仅失败的批次可以重试')
        report=db.get(Report,d.report_id)
        if not report:raise HTTPException(404,'分发计划不存在')
        if not report.enabled:raise HTTPException(409,'计划已暂停，请先启用再重试')
        report_id, batch_time = d.report_id, d.scheduled_for
    return dispatch(report_id,batch_time,retry=True)


def recover():
    """Return interrupted batches to pending so a restarted worker finishes them.

    A concurrent worker that simply takes longer may lose its claim here and redo the
    write, but the unique batch key and the lease-token guard keep it to one delivery.
    """
    with Session() as db:
        rows=db.execute(update(Delivery).where(Delivery.status=='processing')
                        .values(status='pending',lease_owner='',lease_until=0,error=''))
        db.commit()
        return rows.rowcount or 0
