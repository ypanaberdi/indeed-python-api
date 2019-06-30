"""Singleton class to crawl and access data all data available in Indeed API for a particular query
It will run a first query and get a total count of results available and then get all data by running
queries with a starting position increased each time until is saved.
"""
from indeed import Indeed, construct_query
import math, time
from indeed.utils import *


class PageCrawler:
    crawler_limit = 1000  # indeed doesn't return jobs more than #1000
    job_title = ""
    publisher_id = ""
    limit = 25
    start = 0
    location = 98101  # Seattle by default
    sort = "date"
    fromage = 3  # days back by default
    filter_dups = 1
    results = []

    def __init__(self, publisher_id, job_title, location=98101, fromage=3, limit=25, filter_dups=1):
        self.publisher_id = publisher_id
        self.location = location
        self.job_title = job_title
        self.fromage = fromage
        self.limit = limit
        self.filter_dups = filter_dups


    def crawl(self):
        indeed = Indeed(self.publisher_id)
        query = construct_query(all_words=self.job_title)
        indeed.search_jobs(query=query, sort=self.sort, location=self.location, fromage=self.fromage, filter_dups=self.filter_dups, userip=getRandomIP(), useragent=getRandomUserAgent())
        print("total jobs found: " + indeed.totalResults.__str__())
        #for job in indeed.results:
        #    print(job.title)
        if indeed.totalResults<=25:
            pages_left = 0
        else:
            pages_left = math.ceil((indeed.totalResults-25)/self.limit)

        print("pages left: " + pages_left.__str__())
        self.results = self.results + indeed.results
        # generate request for each page
        for x in range(1, pages_left+1):
            # delay between each request
            time.sleep(getRandomSleepTime(1, 10))
            job_seq_number = x*self.limit

            # stop sending requests if we reached limit per query request
            if job_seq_number>self.crawler_limit:
                return

            indeed.search_jobs(query=query, sort=self.sort, location=self.location, fromage=self.fromage, start=job_seq_number, limit=self.limit, filter_dups=self.filter_dups)
            #append all results into one array
            self.results = self.results + indeed.results
            print("page: " + str(x))


