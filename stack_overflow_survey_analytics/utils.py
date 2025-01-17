import sqlalchemy as sa
import os
import yaml


def usd_formatter(num, position):
    num = float('{:.3g}'.format(num))
    magnitude = 0
    while abs(num) >= 1000:
        magnitude += 1
        num /= 1000.0
    return '${}{}'.format('{:f}'.format(num).rstrip('0').rstrip('.'), ['', 'K', 'M', 'B', 'T'][magnitude])


def load_sqlalchemy_engine():
    profiles_fn = os.path.expanduser('~/.dbt/profiles.yml')
    with open(profiles_fn) as profiles_fh:
        profiles = yaml.load(profiles_fh, Loader=yaml.FullLoader)
        stack_overflow_db_profile = profiles['default']['outputs']['stack_overflow_surveys']
        db_url = sa.engine.url.URL(**{
            'drivername': 'postgresql',
            'username': stack_overflow_db_profile['user'],
            'password': stack_overflow_db_profile['pass'],
            'host': stack_overflow_db_profile['host'],
            'port': stack_overflow_db_profile['port'],
            'database': stack_overflow_db_profile['dbname'],
        })
        return sa.engine.create_engine(db_url)