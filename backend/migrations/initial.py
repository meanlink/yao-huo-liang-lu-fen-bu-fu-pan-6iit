import logging
from sqlalchemy import inspect, text
from src.config.database import Base, engine
from src import models

# Columns added after the first release. create_all() only creates missing tables,
# so an existing deployment needs its delivery archive upgraded in place.
DELIVERY_COLUMNS = {
    'scheduled_for': 'DOUBLE PRECISION NOT NULL DEFAULT 0',
    'status': "VARCHAR(20) NOT NULL DEFAULT 'delivered'",
    'attempts': 'INTEGER NOT NULL DEFAULT 0',
    'lease_owner': "VARCHAR(64) NOT NULL DEFAULT ''",
    'lease_until': 'DOUBLE PRECISION NOT NULL DEFAULT 0',
    'started_at': 'DOUBLE PRECISION',
    'finished_at': 'DOUBLE PRECISION',
    'error': "VARCHAR(1000) NOT NULL DEFAULT ''",
    'snapshot': "JSON NOT NULL DEFAULT '{}'",
}


def migrate():
    Base.metadata.create_all(engine)
    inspector = inspect(engine)
    if 'deliveries' not in set(inspector.get_table_names()):
        return
    existing = {c['name'] for c in inspector.get_columns('deliveries')}
    missing = {name: ddl for name, ddl in DELIVERY_COLUMNS.items() if name not in existing}
    if missing:
        with engine.begin() as conn:
            for name, ddl in missing.items():
                conn.execute(text(f'ALTER TABLE deliveries ADD COLUMN {name} {ddl}'))
            # Historic archives predate batch tracking: treat each as one delivered batch.
            conn.execute(text('UPDATE deliveries SET scheduled_for = created_at WHERE scheduled_for = 0'))
            conn.execute(text("UPDATE deliveries SET status = 'delivered' WHERE status = '' OR status IS NULL"))
    # The unique (report, batch time) index is what makes one batch deliverable once.
    inspector = inspect(engine)
    names = {i['name'] for i in inspector.get_indexes('deliveries')}
    names |= {c['name'] for c in inspector.get_unique_constraints('deliveries')}
    if 'uq_delivery_batch' in names:
        return
    try:
        with engine.begin() as conn:
            conn.execute(text('CREATE UNIQUE INDEX uq_delivery_batch ON deliveries (report_id, scheduled_for)'))
    except Exception:
        logging.exception('Could not enforce unique delivery batch index; duplicate historical rows may exist')


if __name__ == '__main__':
    migrate()
