#!/bin/bash/python

from search.monitor.monitor import Monitor

class Search:
    def __init__(self):
        self.monitor = Monitor()

    def get_search_data(self):
        return self.monitor.get_data()
