import unittest

import sqlalchemy
from testcontainers.postgres import PostgresContainer


class MockedPostgresTest(unittest.TestCase):
    @classmethod
    def test_docker_run_postgress(cls) -> None:
        postgres_container = PostgresContainer("postgres:9.5")
        with postgres_container as postgres:
            e = sqlalchemy.create_engine(postgres.get_connection_url())
            result = e.execute("SELECT version()")

    @classmethod
    def tearDownClass(cls) -> None:
        print('Done')


if __name__ == '__main__':
    try:
        unittest.main()
    except Exception:
        MockedPostgresTest().tearDownClass()
