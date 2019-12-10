def pytest_configure(config):
    config.addinivalue_line(
        "markers", "dbtest: Tests that require a database to run"
    )
