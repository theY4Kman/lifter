#!/usr/bin/env python
# -*- coding: utf-8 -*-
import datetime
import unittest

import mock
import pytest
from pytest_lambda import lambda_fixture

from lifter import caches, exceptions, models
from lifter.backends.python import IterableStore


class TModel(models.Model):

    class Meta:
        name = 'test_model'
        app_name = 'test'

    def __repr__(self):
        return self.name


class TestCache:

    @pytest.fixture
    def objects(self):
        parents = [
            TModel(name='parent_1'),
            TModel(name='parent_2'),
        ]
        return [
            TModel(name='test_1', order=2, a=1, parent=parents[0], label='alabama', surname='Mister T'),
            TModel(name='test_2', order=3, a=1, parent=parents[0], label='arkansas', surname='Colonel'),
            TModel(name='test_3', order=1, a=2, parent=parents[1], label='texas', surname='Lincoln'),
            TModel(name='test_4', order=4, a=2, parent=parents[1], label='washington', surname='clint'),
        ]

    cache = lambda_fixture(lambda: caches.DummyCache())
    store = lambda_fixture(
        lambda objects, cache: IterableStore(objects, identifier='test', cache=cache)
    )
    manager = lambda_fixture(lambda store: store.query(TModel))

    def test_store_uses_store_model_app_name_and_hashed_query_for_cache_key(self, manager, store):
        query = manager.all().query
        cache_parts = [
            store.identifier,
            TModel._meta.app_name,
            TModel._meta.name,
            store.hash_query(query),
        ]

        expected = ':'.join(cache_parts)
        actual = store.get_cache_key(query, TModel)
        assert actual == expected

    def test_store_tries_to_return_from_cache_before_executing_query(self, manager):
        with mock.patch('lifter.store.Store.get_from_cache', side_effect=exceptions.NotInCache()) as m:
            qs = manager.all()
            query = qs.query
            list(qs)
            m.assert_called_with(query, TModel)

    def test_store_stores_result_in_cache_when_queyr_is_executed(self, manager, cache):
        r = manager.count()
        cache_key = list(cache._data.keys())[0]
        expires_on, value = cache._data[cache_key]

        expected = r
        actual = value
        assert actual == expected

    @mock.patch('lifter.caches.Cache._get')
    def test_can_disable_cache(self, mocked, manager, cache):
        with cache.disable():
            r = manager.count()
            manager.count()
            mocked.assert_not_called()

class TestDummyCache:
    cache = lambda_fixture(lambda: caches.DummyCache())

    def test_can_store_value(self, cache):
        cache.set('key', 'value')
        assert cache.get('key') == 'value'

    def test_can_get_or_default(self, cache):
        assert cache.get('key', 'default') == 'default'

    def test_can_get_or_set(self, cache):
        r = cache.get_or_set('key', 'value')
        assert r == 'value'

    def test_can_pass_callable_to_set(self, cache):
        f = lambda: 'yolo'

        r = cache.get_or_set('key', f)
        assert r == 'yolo'

    def test_can_provide_timeout(self, cache):
        now = datetime.datetime.now()
        cache.set('key', 'value', 3600)
        with mock.patch('lifter.caches.Cache.get_now', return_value=now + datetime.timedelta(seconds=3599)):
            assert cache.get('key') == 'value'

        with mock.patch('lifter.caches.Cache.get_now', return_value=now + datetime.timedelta(seconds=3601)):
            assert cache.get('key') is None
