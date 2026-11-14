import time, json
from sqlalchemy import select, func
from src.config.database import Session
from migrations.initial import migrate
from src.models import User, Dataset, Job, Report, Delivery, Audit
from src.services.business import metadata, seed_records, analyze
from src.services.reports import dispatch
from src.utils.security import hash_password
from src.utils.serialization import checksum


def baseline():
    """The demo's one baseline brief goes through the same batch path as a scheduled run."""
    m = metadata()
    with Session() as db:
        admin = db.scalar(select(User).where(User.role == 'admin'))
        d = db.scalar(select(Dataset).where(Dataset.name == m['dataset_name']))
        if not d:
            rows = seed_records()
            d = Dataset(name=m['dataset_name'], source=m['short'] + '-202611.csv', records=rows,
                        checksum=checksum(rows), created_at=time.time())
            db.add(d); db.flush()
        job_name = '基线复核 · ' + m['name']
        j = db.scalar(select(Job).where(Job.name == job_name))
        if not j:
            params = {p['key']: p['default'] for p in m.get('parameters', [])}
            result = analyze(d.records, params)
            json.dumps(result, ensure_ascii=False, allow_nan=False)
            j = Job(name=job_name, dataset_id=d.id, params=params, status='completed', result=result,
                    created_by=admin.id, created_at=time.time() - 3600, finished_at=time.time() - 3595,
                    approved=True)
            db.add(j); db.flush()
        r = db.scalar(select(Report).where(Report.job_id == j.id))
        if not r:
            due = time.time() - 1
            r = Report(name='每日业务复核简报', job_id=j.id, destination='业务复核组 · 本地收件箱',
                       interval_seconds=86400, next_run=due, enabled=True)
            db.add(r); db.flush()
        else:
            due = r.next_run if r.next_run <= time.time() else time.time() - 1
        pending = not db.scalar(select(Delivery.id).where(Delivery.report_id == r.id).limit(1))
        db.commit()
        report_id = r.id
    if pending:
        outcome = dispatch(report_id, due)
        with Session() as db:
            db.add(Audit(actor='seed', action='demo.baseline',
                         detail={'dataset': d.id, 'delivery': (outcome or {}).get('id')}, at=time.time()))
            db.commit()


def seed():
    migrate()
    with Session() as db:
        for role, name in [('admin', '林知 · 业务负责人'), ('analyst', '陈舟 · 分析员'), ('viewer', '周禾 · 观察员')]:
            email = role + '@demo.local'
            if not db.scalar(select(User).where(User.email == email)):
                db.add(User(email=email, name=name, role=role, password=hash_password('Demo123!')))
        db.commit()
    baseline()
    with Session() as db:
        counts = {model.__tablename__: db.scalar(select(func.count()).select_from(model))
                  for model in [User, Dataset, Job, Report, Delivery, Audit]}
    print(json.dumps(counts, ensure_ascii=False))
    return counts


if __name__ == '__main__':
    seed()
