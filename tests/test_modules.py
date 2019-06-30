# -*- coding: utf-8 -*-

from unittest import TestCase
#from indeed.main import Query, Element, Indeed
from indeed import construct_query, Element, Indeed
__all__ = [construct_query, Element, Indeed]

class TestQuery(TestCase):
    """
    Test proper query construction
    """
    def setUp(self):
        """
        Initialize Query instance
        """
        self.indeed = Indeed();
        self.q = self.indeed.con

    def test_only_one_word(self):
        """
        One word given - one word returned
        """
        word = "Python"
        self.q.construct_query(all_words=word)
        self.assertEqual(self.q.query, "Python")

    def test_multiple_words(self):
        """
        Multiple words given - concatenated by '+'
        """
        words = "Python Django Junior"
        self.q.construct_query(all_words=words)
        self.assertEqual(self.q.query, "Python+Django+Junior")

    def test_one_word_with_one_not(self):
        """
        One word include + one word exclude = two words with different signs
        """
        words = "Python"
        none = "Junior"
        self.q.construct_query(all_words=words, none=none)
        self.assertEqual(self.q.query, "Python+-Junior")
