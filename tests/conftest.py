import pytest

from .context import db


@pytest.fixture(scope="session")
def batch3dfier_db(request):
    dbs = db.db(dbname='batch3dfier_db', host='localhost', port='5433',
                user='batch3dfier', password='batch3d_test')

    def disconnect():
        dbs.close()
    request.addfinalizer(disconnect)

    return(dbs)
