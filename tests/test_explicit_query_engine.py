#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
test_lifter
----------------------------------

Tests for `lifter` module.
"""

import random
import sys
import unittest
import mock

import lifter.models
import lifter.aggregates
import lifter.exceptions
import lifter.lookups
from lifter.backends.python import IterableStore


class TObject:
    def __init__(self, name, **kwargs):
        self.name = name
        for key, value in kwargs.items():
            setattr(self, key, value)

    def __repr__(self):
        return self.name

class TestBase(unittest.TestCase):
    PARENTS = [
        TObject(name='parent_1'),
        TObject(name='parent_2'),
    ]
    OBJECTS = [
        TObject(name='test_1', order=2, a=1, parent=PARENTS[0], label='alabama', surname='Mister T'),
        TObject(name='test_2', order=3, a=1, parent=PARENTS[0], label='arkansas', surname='Colonel'),
        TObject(name='test_3', order=1, a=2, parent=PARENTS[1], label='texas', surname='Lincoln'),
        TObject(name='test_4', order=4, a=2, parent=PARENTS[1], label='washington', surname='clint'),
    ]

    DICTS = [o.__dict__ for o in OBJECTS]

    def setUp(self):
        self.manager = IterableStore(self.OBJECTS).query(TModel)
        self.dict_manager = IterableStore(self.DICTS).query(TModel)


class TModel(lifter.models.Model):
    pass


class TestQueries(TestBase):

    def test_model(self):
        manager = IterableStore(self.OBJECTS).query(TModel)
        self.assertEqual(manager.filter(TModel.a == 1), self.OBJECTS[:2])

    def test_default_order(self):
        self.assertEqual(list(self.manager.all()), self.OBJECTS)
        self.assertEqual(list(self.dict_manager.all()), self.DICTS)

    def test_can_get_using_attribute(self):
        self.assertEqual(self.manager.all().get(TModel.name == 'test_1'), self.OBJECTS[0])
        self.assertEqual(self.dict_manager.all().get(TModel.name == 'test_1'), self.DICTS[0])

    def test_can_filter(self):
        self.assertEqual(self.manager.filter(TModel.a == 1), self.OBJECTS[:2])

    def test_get_exclude_and_filter_combine_queries_to_and_by_default(self):
        self.assertEqual(self.manager.all().get(TModel.order > 2, TModel.a == 2), self.OBJECTS[3])
        self.assertEqual(self.manager.all().filter(TModel.order > 2, TModel.a == 2), [self.OBJECTS[3]])
        self.assertEqual(self.manager.all().exclude(TModel.order > 2, TModel.a == 2), self.OBJECTS[:3])

    def test_can_combine_queries_using_or(self):
        self.assertEqual(self.manager.all().filter((TModel.order > 2) | (TModel.a == 2)), self.OBJECTS[1:])
        self.assertEqual(self.manager.all().exclude((TModel.order > 2) | (TModel.a == 2)), [self.OBJECTS[0]])
        self.assertEqual(self.manager.all().exclude(~((TModel.order > 2) | (TModel.a == 2))), self.OBJECTS[1:])
        self.assertEqual(self.manager.all().exclude(~(TModel.order > 2) | (TModel.a == 2)), [self.OBJECTS[1]])

    def test_queryset_is_lazy(self):
        with mock.patch('lifter.query.QuerySet._fetch_all') as fetch:
            qs = self.manager.all().filter(TModel.order == 3)
            fetch.assert_not_called()
            self.assertFalse(qs._populated)
            self.assertEqual(qs._data, [])

        self.assertEqual(qs, [self.OBJECTS[1]])

        with mock.patch('lifter.query.QuerySet._fetch_all') as fetch:
            self.assertEqual(qs, [self.OBJECTS[1]])
            self.assertEqual(qs, [self.OBJECTS[1]])
            self.assertEqual(qs, [self.OBJECTS[1]])
            self.assertTrue(qs._populated)
            fetch.assert_not_called()


    def test_can_combine_filters(self):
        self.assertEqual(self.manager.filter((TModel.a == 1) & (TModel.name == 'test_2')), self.OBJECTS[1:2])
        self.assertEqual(self.manager.filter(TModel.a == 1).filter(TModel.name == 'test_2'), self.OBJECTS[1:2])

        self.assertEqual(self.dict_manager.filter((TModel.a == 1) & (TModel.name == 'test_2')), self.DICTS[1:2])
        self.assertEqual(self.dict_manager.filter(TModel.a == 1).filter(TModel.name == 'test_2'), self.DICTS[1:2])

    @mock.patch('lifter.query.QuerySet.iterator')
    def test_queries_combine_to_a_single_one(self, mocked_iterator):
        queryset = self.manager.filter(TModel.a == 1).filter(TModel.order == 1)
        len(queryset)
        self.assertEqual(mocked_iterator.call_count, 1)

    def test_can_exclude(self):
        self.assertEqual(self.manager.exclude(TModel.a == 1), self.OBJECTS[2:])
        self.assertEqual(self.dict_manager.exclude(TModel.a == 1), self.DICTS[2:])

    def test_can_combine_exclude(self):
        self.assertEqual(self.manager.exclude(TModel.a == 1).exclude(TModel.name == 'test_4'), self.OBJECTS[2:3])
        self.assertEqual(self.manager.exclude((TModel.a == 2) & (TModel.name == 'test_4')), self.OBJECTS[:3])

        self.assertEqual(self.dict_manager.exclude(TModel.a == 1).exclude(TModel.name == 'test_4'), self.DICTS[2:3])
        self.assertEqual(self.dict_manager.exclude((TModel.a == 2) & (TModel.name == 'test_4')), self.DICTS[:3])

    def test_related_lookups(self):
        self.assertEqual(self.manager.filter(TModel.parent.name == 'parent_1'), self.OBJECTS[:2])
        self.assertEqual(self.manager.exclude(TModel.parent.name == 'parent_1'), self.OBJECTS[2:])
        self.assertEqual(self.manager.all().get((TModel.parent.name == 'parent_1') & (TModel.order == 2)), self.OBJECTS[0])

        self.assertEqual(self.dict_manager.filter(TModel.parent.name == 'parent_1'), self.DICTS[:2])
        self.assertEqual(self.dict_manager.exclude(TModel.parent.name == 'parent_1'), self.DICTS[2:])
        self.assertEqual(self.dict_manager.all().get((TModel.parent.name == 'parent_1') & (TModel.order == 2)), self.DICTS[0])


    def test_exception_raised_on_missing_attr(self):
        with self.assertRaises(lifter.exceptions.MissingField):
            list(self.manager.filter(TModel.x == "y"))
        with self.assertRaises(lifter.exceptions.MissingField):
            list(self.dict_manager.filter(TModel.x == "y"))

    def test_can_count(self):
        self.assertEqual(self.manager.filter(TModel.a == 1).count(), 2)

        self.assertEqual(self.dict_manager.filter(TModel.a == 1).count(), 2)



    def test_first(self):
        self.assertIsNone(self.manager.filter(TModel.a == 123).first())
        self.assertIsNotNone(self.manager.filter(TModel.a == 1).first())

        self.assertIsNone(self.dict_manager.filter(TModel.a == 123).first())
        self.assertIsNotNone(self.dict_manager.filter(TModel.a == 1).first())

    def test_last(self):
        self.assertIsNone(self.manager.filter(TModel.a == 123).last())
        self.assertIsNotNone(self.manager.filter(TModel.a == 1).last())

        self.assertIsNone(self.dict_manager.filter(TModel.a == 123).last())
        self.assertIsNotNone(self.dict_manager.filter(TModel.a == 1).last())

    def test_ordering(self):

        self.assertEqual(self.manager.order_by(TModel.order)[:2], [self.OBJECTS[2], self.OBJECTS[0]])
        self.assertEqual(self.manager.order_by(~TModel.order)[:2], [self.OBJECTS[3], self.OBJECTS[1]])

        self.assertEqual(self.dict_manager.order_by(TModel.order)[:2], [self.DICTS[2], self.DICTS[0]])
        self.assertEqual(self.dict_manager.order_by(~TModel.order)[:2], [self.DICTS[3], self.DICTS[1]])

    def test_ordering_using_multiple_paths(self):

        p1 = TModel.a
        p2 = TModel.order
        self.assertEqual(self.manager.order_by(p1, p2)[:2], [self.OBJECTS[0], self.OBJECTS[1]])
        self.assertEqual(self.manager.order_by(~p1, p2)[:2], [self.OBJECTS[2], self.OBJECTS[3]])
        self.assertEqual(self.manager.order_by(p1, ~p2)[:2], [self.OBJECTS[1], self.OBJECTS[0]])
        self.assertEqual(self.manager.order_by(~p1, ~p2)[:2], [self.OBJECTS[3], self.OBJECTS[2]])

    @unittest.skip('If someone find a proper way to unittest random ordering, contribution is welcome')
    def test_random_ordering(self):
        is_py3 = sys.version_info >= (3, 2)

        random.seed(0)
        random_ordered_0 = self.dict_manager.order_by('?')[:2]
        if is_py3:
            self.assertEqual(random_ordered_0, [self.DICTS[3], self.DICTS[1]])
        else:
            self.assertEqual(random_ordered_0, [self.DICTS[3], self.DICTS[2]])
        random.seed(1)
        random_ordered_1 = self.dict_manager.order_by('?')[:2]
        if is_py3:
            self.assertEqual(random_ordered_1, [self.DICTS[1], self.DICTS[2]])
        else:
            self.assertEqual(random_ordered_1, [self.DICTS[0], self.DICTS[2]])

        self.assertNotEqual(random_ordered_0, random_ordered_1)

    def test_exists(self):
        self.assertFalse(self.manager.filter(TModel.a == 123).exists())
        self.assertTrue(self.manager.filter(TModel.a == 1).exists())

        self.assertFalse(self.dict_manager.filter(TModel.a == 123).exists())
        self.assertTrue(self.dict_manager.filter(TModel.a == 1).exists())

        # force reload from backend
        self.assertFalse(self.dict_manager.filter(TModel.a == 123).exists(from_backend=True))
        self.assertTrue(self.dict_manager.filter(TModel.a == 1).exists(from_backend=True))

    def test_get_raise_exception_on_multiple_objects_returned(self):
        with self.assertRaises(lifter.exceptions.MultipleObjectsReturned):
            self.manager.all().get(TModel.a == 1)

        with self.assertRaises(lifter.exceptions.MultipleObjectsReturned):
            self.dict_manager.all().get(TModel.a == 1)

    def test_get_raise_exception_on_does_not_exist(self):
        with self.assertRaises(lifter.exceptions.DoesNotExist):
            self.manager.all().get(TModel.a == 123)

        with self.assertRaises(lifter.exceptions.DoesNotExist):
            self.dict_manager.all().get(TModel.a == 123)

    def test_can_filter_using_callable(self):
        self.assertEqual(self.manager.filter(TModel.order.test(lambda v: v in [1, 3])), [self.OBJECTS[1], self.OBJECTS[2]])

        self.assertEqual(self.dict_manager.filter(TModel.order.test(lambda v: v in [1, 3])), [self.DICTS[1], self.DICTS[2]])

    def test_values(self):
        expected = [
            {'order': 2},
            {'order': 3},
        ]
        self.assertEqual(self.manager.filter(TModel.a == 1).values(TModel.order), expected)
        self.assertEqual(self.dict_manager.filter(TModel.a == 1).values(TModel.order), expected)

        expected = [
            {'order': 2, 'a': 1},
            {'order': 3, 'a': 1},
        ]
        self.assertEqual(self.manager.filter(TModel.a == 1).values(TModel.order, TModel.a), expected)
        self.assertEqual(self.dict_manager.filter(TModel.a == 1).values(TModel.order, TModel.a), expected)

    def test_values_list(self):
        expected = [2, 3]
        self.assertEqual(self.manager.filter(TModel.a == 1).values_list(TModel.order, flat=True), expected)
        self.assertEqual(self.dict_manager.filter(TModel.a == 1).values_list(TModel.order, flat=True), expected)

        expected = [
            (2, 1),
            (3, 1),
        ]
        self.assertEqual(self.manager.filter(TModel.a == 1).values_list(TModel.order, TModel.a), expected)
        self.assertEqual(self.dict_manager.filter(TModel.a == 1).values_list(TModel.order, TModel.a), expected)

    def test_distinct(self):
        self.assertEqual(self.manager.all().values_list(TModel.a, flat=True), [1, 1, 2, 2])
        self.assertEqual(self.manager.all().values_list(TModel.a, flat=True).distinct(), [1, 2])
        self.assertEqual(self.manager.all().values_list(TModel.parent, flat=True).distinct(), self.PARENTS)

    def test_run_filter_on_nested_iterables(self):
        data = [
            {
                'name': 'Kurt',
                'tags': [
                    {
                        'name': 'nice',
                    },
                    {
                        'name': 'friendly',
                    },
                ]
            },
            {
                'name': 'Bill',
                'tags': [
                    {
                        'name': 'friendly',
                    },
                ]
            },
        ]

        class User(lifter.models.Model):
            pass

        manager = IterableStore(data).query(User)
        users = list(manager.all())

        query = User.tags.name == 'nice'
        self.assertEqual(manager.filter(query), [users[0]])

        query = User.tags.name == 'friendly'
        self.assertEqual(manager.filter(query), users)

        query = (User.tags.name == 'friendly') & (User.name == 'Bill')
        self.assertEqual(manager.filter(query), [users[1]])

    def test_conditional_field_inside_nested_iterable(self):
        data = [
            {
                'id': 1,
                'members': [
                    {'name': 'son', 'dob': '2/24/2000'},
                    {'name': 'dad'},
                ]
            },
            {
                'id': 2,
                'members': [
                    {'name': 'forever_alone', 'cats': 12}
                ]
            }
        ]

        class Family(lifter.models.Model):
            pass

        manager = IterableStore(data).query(Family)
        families = list(manager.all())

        # get families with son/dob members
        son_dob_families = manager.filter(
            Family.members.name == 'son',
            Family.members.dob.exists())

        self.assertEqual(son_dob_families, [families[0]])

    def test_can_query_nested_nested_iterables(self):
        data = [
            {
                'name': 'Kurt',
                'tags': [
                    {
                        'name': 'nice',
                        'subtags': [
                            {'name': 'subtag_1'},
                            {'name': 'subtag_2'},
                        ]
                    },
                    {
                        'name': 'friendly',
                        'subtags': [
                            {'name': 'subtag_0'},
                        ]
                    },
                ]
            },
            {
                'name': 'Bill',
                'tags': [
                    {
                        'name': 'friendly',
                        'subtags': [
                            {'name': 'subtag_1'},
                            {'name': 'subtag_3'},
                        ]
                    },
                ]
            },
        ]

        class User(lifter.models.Model):
            pass

        manager = IterableStore(data).query(User)
        users = list(manager.all())

        self.assertEqual(manager.filter(User.tags.name == 'nice'), [users[0]])
        self.assertEqual(
            manager.filter(User.tags.subtags.name == 'subtag_0'), [users[0]])
        self.assertEqual(
            manager.filter(User.tags.subtags.name == 'subtag_1'),
            [users[0], users[1]])
        self.assertEqual(
            manager.filter(User.tags.subtags.name == 'subtag_3'),
            [users[1]])

    # def test_can_get_query_from_queryset(self):
    #
    #     qs = self.manager.filter(TModel.a == 1).order_by(~TModel.a)
    #     expected = {
    #         'filter': TModel.a == 1,
    #         'ordering': TModel.a,
    #     }
    #     self.assertEqual(qs.base_query, expected)
    #
    # def test_can_check_nested_iterables(self):
    #     users = [
    #         {
    #             'name': 'Kurt',
    #             'tags': [
    #                 {'name': 'nice'},
    #                 {'name': 'friendly'},
    #             ]
    #         },
    #         {
    #             'name': 'Bill',
    #             'tags': [
    #                 {'name': 'friendly'},
    #             ]
    #         },
    #     ]
    #     manager = lifter.load(users)
    #     self.assertNotIn(users[1], manager.filter(tags__name='nice'))
    #     self.assertRaises(ValueError, manager.filter, tags__x='y')
    #
    #     companies = [
    #         {
    #             'name': 'blackbooks',
    #             'employees': [
    #                 {
    #                     'name': 'Manny',
    #                     'tags': [
    #                         {'name': 'nice'},
    #                         {'name': 'friendly'},
    #                     ]
    #                 }
    #             ]
    #         },
    #         {
    #             'name': 'community',
    #             'employees': [
    #                 {
    #                     'name': 'Britta',
    #                     'tags': [
    #                         {'name': 'activist'},
    #                     ]
    #                 }
    #             ]
    #         }
    #     ]
    #     manager = lifter.load(companies)
    #     self.assertNotIn(companies[1], manager.filter(employees__tags__name='friendly'))

class TestLookups(TestBase):
    def test_gt(self):
        self.assertEqual(self.manager.filter(TModel.order > 3), [self.OBJECTS[3]])

    def test_gte(self):
        self.assertEqual(self.manager.filter(TModel.order >= 3), [self.OBJECTS[1], self.OBJECTS[3]])

    def test_lt(self):
        self.assertEqual(self.manager.filter(TModel.order < 3), [self.OBJECTS[0], self.OBJECTS[2]])

    def test_lte(self):
        self.assertEqual(self.manager.filter(TModel.order <= 3), [self.OBJECTS[0], self.OBJECTS[1], self.OBJECTS[2]])

    def test_startswith(self):
        self.assertEqual(self.manager.filter(TModel.label.test(lifter.lookups.startswith('a'))),
                        [self.OBJECTS[0], self.OBJECTS[1]])

    def test_endswith(self):
        self.assertEqual(self.manager.filter(TModel.label.test(lifter.lookups.endswith('s'))),
                        [self.OBJECTS[1], self.OBJECTS[2]])

    def test_value_in(self):
        self.assertEqual(self.manager.filter(TModel.label.test(lifter.lookups.value_in(['alabama', 'arkansas']))),
                        [self.OBJECTS[0], self.OBJECTS[1]])

    def test_range(self):
        self.assertEqual(self.manager.filter(TModel.order.test(lifter.lookups.value_range((2, 3)))),
                        [self.OBJECTS[0], self.OBJECTS[1]])

    def test_istartswith(self):
        self.assertEqual(self.manager.filter(TModel.surname.test(lifter.lookups.istartswith('c'))),
                        [self.OBJECTS[1], self.OBJECTS[3]])

    def test_iendswith(self):
        self.assertEqual(self.manager.filter(TModel.surname.test(lifter.lookups.iendswith('t'))),
                        [self.OBJECTS[0], self.OBJECTS[3]])

    def test_contains(self):
        self.assertEqual(self.manager.filter(TModel.surname.test(lifter.lookups.contains('Lin'))),
                        [self.OBJECTS[2]])

    def test_icontains(self):
        self.assertEqual(self.manager.filter(TModel.surname.test(lifter.lookups.icontains('lin'))),
                        [self.OBJECTS[2], self.OBJECTS[3]])

    def test_field_exists(self):

        families = [
            {
                'name': 'Community',
                'postal_adress': 'Greendale',
            },
            {
                'name': 'Misfits',
            }
        ]

        class Family(lifter.models.Model):
            pass

        manager = IterableStore(families).query(Family)

        self.assertEqual(manager.filter(Family.postal_adress.exists()), [families[0]])
        self.assertEqual(manager.filter(~Family.postal_adress.exists()), [families[1]])

def mean(values):
    return float(sum(values)) / len(values)

class TestAggregation(TestBase):
    def test_multiple_aggregates_at_once(self):
        expected = {
            'a__sum': 6,
            'a__avg': 1.5,
        }
        aggregates = (lifter.aggregates.Sum('a'), lifter.aggregates.Avg('a'))
        self.assertEqual(self.manager.aggregate(*aggregates), expected)

    def test_sum(self):
        self.assertEqual(self.manager.aggregate((TModel.a, sum)), {'a__sum': 6})
        self.assertEqual(self.manager.aggregate(total=(TModel.a, sum)), {'total': 6})

    def test_min(self):
        self.assertEqual(self.manager.aggregate((TModel.a, min)), {'a__min': 1})

    def test_max(self):
        self.assertEqual(self.manager.aggregate((TModel.a, max)), {'a__max': 2})

    def test_mean(self):
        self.assertEqual(self.manager.aggregate((TModel.a, mean)), {'a__mean': 1.5})

    def test_flat(self):
        self.assertEqual(self.manager.aggregate((TModel.a, mean), flat=True), [1.5])

if __name__ == '__main__':
    import sys
    sys.exit(unittest.main())
