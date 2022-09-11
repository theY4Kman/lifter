import os
import unittest

import pytest
from pytest_lambda import lambda_fixture

from lifter import adapters, models, parsers, utils

BASE_DIR = os.path.abspath(os.path.dirname(__file__))
DATA_PATH = os.path.join(BASE_DIR, 'data', 'bucket.xml')


class StaticFile(models.Model):
    pass


class Adapter(adapters.ETreeAdapter):
    pass


class TestETreeAdapter:
    @pytest.fixture
    def raw_xml(self) -> str:
        with open(DATA_PATH) as f:
            return f.read()

    results = lambda_fixture(lambda raw_xml: (
        parsers.XMLParser(
            results='./amazon:Contents',
            ns={'amazon': 'http://s3.amazonaws.com/doc/2006-03-01/'},
        )
        .parse(raw_xml)
    ))

    def test_etree_adapter(self, results):
        # sanity check
        assert len(results) == 8

        adapter = adapters.ETreeAdapter()
        for result in results:
            static_file = adapter.parse(result, StaticFile)
            for e in result:
                name = utils.to_snake_case(adapter.tag_to_field_name(e.tag))
                value = getattr(static_file, name)

                assert value == e.text
