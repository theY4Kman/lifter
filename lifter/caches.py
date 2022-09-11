"""
Cache yo.
"""
from __future__ import annotations

import datetime
from typing import Any, Callable, Generic, TypeVar

from . import exceptions


class NotSet:
    pass


CacheT = TypeVar('CacheT', bound='Cache')


class CacheToggler(Generic[CacheT]):
    def __init__(self, cache: CacheT, new_value: bool):
        self.cache = cache
        self.new_value = new_value
        self.previous_value = self.cache.enabled

    def __enter__(self) -> CacheT:
        self.cache.enabled = self.new_value
        return self.cache

    def __exit__(self, exc_type, exc_value, exc_tb):
        self.cache.enabled = self.previous_value


V = TypeVar('V')


class Cache:
    def __init__(self, default_timeout: float | None = None, enabled: bool = True):
        self.default_timeout = default_timeout
        self.enabled = enabled

    def get(self, key: str, default: V = None, reraise: bool = False) -> Any | V:
        """
        Get the given key from the cache, if present.
        A default value can be provided in case the requested key is not present,
        otherwise, None will be returned.

        :param key: the key to query
        :param default: the value to return if the key does not exist in cache
        :param reraise: whether an exception should be thrown if value not found (default: False)

        Example usage:

        .. code-block:: python

            >>> cache = DummyCache()

            >>> cache.set('my_key', 'my_value')
            'my_value'
            >>> cache.get('my_key')
            'my_value'

            >>> cache.get('not_present', 'default_value')
            'default_value'

            >>> cache.get('not_present', reraise=True)
            Traceback (most recent call last):
              ...
            lifter.exceptions.NotInCache: 'not_present'

        """
        if not self.enabled:
            if reraise:
                raise exceptions.DisabledCache()
            return default

        try:
            return self._get(key)
        except exceptions.NotInCache:
            if reraise:
                raise
            return default

    def set(self, key: str, value: V | Callable[[], V], timeout=NotSet) -> V:
        """
        Set the given key to the given value in the cache.
        A timeout may be provided, otherwise, the :py:attr:`Cache.default_timeout`
        will be used.

        :param key: the key to which the value will be bound
        :param value: the value to store in the cache
        :param timeout: the expiration delay for the value. None means it will never expire.

        Example usage:

        .. code-block:: python

            # this cached value will expire after half an hour
            cache.set('my_key', 'value', 1800)

        """
        if not self.enabled:
            return False

        if callable(value):
            value = value()
        if timeout == NotSet:
            timeout = self.default_timeout

        self._set(key, value, timeout)
        return value

    def get_or_set(self, key: str, value: V) -> Any | V:
        try:
            return self.get(key, reraise=True)
        except exceptions.NotInCache:
            return self.set(key, value)

    def enable(self) -> CacheToggler:
        """
        Returns a context manager to force enabling the cache if it is disabled:

        .. code-block:: python

            with cache.enable():
                manager.count()
        """
        return CacheToggler(self, True)

    def disable(self) -> CacheToggler:
        """
        Returns a context manager to bypass the cache:

        .. code-block:: python

            with cache.disable():
                # Will ignore the cache
                manager.count()
        """
        return CacheToggler(self, False)

    def get_now(self):
        """Return current time

        This method is mocked during testing.
        """
        return datetime.datetime.now()

    def _get(self, key: str) -> Any:
        raise NotImplementedError

    def _set(self, key: str, value, timeout=None) -> bool:
        raise NotImplementedError


class DummyCache(Cache):
    def __init__(self, *args, **kwargs):
        self._data = {}
        super().__init__(*args, **kwargs)

    def _set(self, key: str, value: Any, timeout: float | None = None) -> bool:
        if timeout is not None:
            expires_on = self.get_now() + datetime.timedelta(seconds=timeout)
        else:
            expires_on = None
        self._data[key] = (expires_on, value)
        return True

    def _get(self, key: str) -> Any:
        try:
            expires_on, value = self._data[key]
        except KeyError:
            raise exceptions.NotInCache(key)

        if expires_on is None:
            return value

        if expires_on < self.get_now():
            del self._data[key]
            raise exceptions.NotInCache(key)

        return value
