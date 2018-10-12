from mock import patch, MagicMock


class TestCache:
    def setup(self):
        pass
        # setup() before each test method

    def test_init(self):
        from hedgedata import Cache
        Cache(offline=True)
