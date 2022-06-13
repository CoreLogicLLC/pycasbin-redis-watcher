import pytest

redis_published_message = ""


@pytest.fixture
def fake_redis_server(mocker):
    import fakeredis

    r = fakeredis.FakeStrictRedis()
    mock_redis = mocker.patch("casbin_redis_watcher.watcher.get_redis")
    mock_redis.return_value = r


@pytest.fixture
def redis_watcher():
    from casbin_redis_watcher import RedisWatcher

    rw = RedisWatcher(
        redis_host="testing", redis_port=2000, start_process=True
    )
    return rw


def test_redis_watcher_init(redis_watcher, fake_redis_server):
    assert redis_watcher.redis_url == 'testing'
    assert redis_watcher.redis_port == 2000
    assert redis_watcher.parent_conn is not None
    assert redis_watcher.subscribed_process is not None


def test_update(redis_watcher, fake_redis_server):
    redis_watcher.update()
    assert redis_watcher.should_reload() is True


def test_no_reload(redis_watcher):
    assert not redis_watcher.should_reload()


def test_default_update_callback(redis_watcher):
    assert redis_watcher.update_callback() is None


def test_set_update_callback(redis_watcher):
    def tst_callback():
        pass

    redis_watcher.set_update_callback(tst_callback)
    assert redis_watcher.update_callback == tst_callback
