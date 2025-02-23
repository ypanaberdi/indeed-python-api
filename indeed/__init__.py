# -*- coding: utf-8 -*-
#
# Python Indeed API
# Main information here https://ads.indeed.com/jobroll/xmlfeed
#
import requests
from lxml import etree
from indeed.utils import *
import urllib.parse as urlparse


def construct_query( all_words="", exact_phraze="",
                    at_least_one="", none="", title="",
                    company=""):
    """
    Construct query from initialized words
    Sequence of parameters are given space-separated, e.g. "Python Django"
    :all_words - look for all words in vacancies
    :exact_phraze - look for exact phraze in vacancies
    :at_least_one - look for at least one word in vacancies
    :none - ignore vacancies with these words
    :title - look for this words in title
    :company - look for vacancies, posted by this company
    """
    query = ""
    query += "+".join(filter(None, all_words.split(" ")))
    if exact_phraze:
        query += "\"{}\"".format(exact_phraze)
    if at_least_one:
        temp = list(filter(None, at_least_one.split(" ")))
        if len(temp) > 1:
            query += "({})".format("+".join(temp))
        else:
            query += "+" + temp[0]
    if none:
        temp = list(filter(None, none.split(" ")))
        if len(temp) > 1:
            query += "+-".format(temp)
        else:
            query += "+-" + temp[0]
    if title:
        query += "title:({})".format(title)
    if company:
        query += "company:({})".format(company)
    return query


class Element():
    """
    Every element of Indeed's response
    """
    def __init__(self, title, company, city, state,
                 country, source, date, description, url,
                 expired, date_published):
        """
        Initialize element
        :title - title of vacancy
        :company - name of company
        :city - city, where company is located
        :state - state (bundesland, province, territory)
        :country - country, where company is located
        :source - site, where vacancy is posted (external or indeed itself)
        :date - date when vacancy was posted with GMT
        :description - short description of vacancy
        :url - link of vacancy
        :expired - True if vacancy is inactive, False otherwise
        :date_published - date of vacancy publication in days, e.g "2 days ago"
        """
        self.title = title
        self.company = company
        self.city = city
        self.state = state
        self.country = country
        self.source = source
        self.date = date
        self.description = description
        self.url = url
        self.expired = expired
        self.date_published = date_published
        self.job_id = (urlparse.parse_qs((urlparse.urlparse(url)).query))['jk'][0]

    def __repr__(self):
        """
        Give information when using print (ind.results)
        """
        return self.title + ' : ' + self.job_id

    def __str__(self):
        """
        Show info of element
        """
        row = \
        """
        Title: {title},
        Description: {description}
        Company: {company}
        Date: {date}, {date_published}
        Location: {country}, {city}, {state}
        Expired: {expired}
        Link: {url}
        """.format(**self.__dict__)
        return row

    def getTitle(self):
        return self.title


class Indeed():
    """
    Indeed client
    """
    TIMEOUT = 5
    FORMAT_RESULTS = ("xml", "json")
    SORT = ("relevance", "date")
    SITE_TYPE = ("jobsite", "employer")
    JOB_TYPE = ("fulltime", "parttime", "contract", "internship", "temporary")
    COUNTRIES = {"United states": "us",
                 "Argentina": "ar",
                 "Australia": "au",
                 "Austria": "at",
                 "Bahrain": "bh",
                 "Belgium": "be",
                 "Brazil": "br",
                 "Canada": "ca",
                 "Chile": "cl",
                 "China": "cn",
                 "Colombia": "co",
                 "Czhech Republic": "cz",
                 "Denmark": "dk",
                 "Finland": "fi",
                 "France": "fr",
                 "Germany": "de",
                 "Greece": "gr",
                 "Hong Kong": "hk",
                 "Hungary": "hu",
                 "India": "in",
                 "Indonesia": "id",
                 "Ireland": "ie",
                 "Israel": "il",
                 "Italy": "it",
                 "Japan": "jp",
                 "Korea": "kr",
                 "Kuwait": "kw",
                 "Luxemburg": "lu",
                 "Malaysia": "my",
                 "Mexico": "mx",
                 "Netherlands": "nl",
                 "New Zealand": "nz",
                 "Norway": "no",
                 "Oman": "om",
                 "Pakistan": "pk",
                 "Peru": "pe",
                 "Philippines": "ph",
                 "Poland": "pl",
                 "Portugal": "pt",
                 "Qatar": "qa",
                 "Romania": "ro",
                 "Russia": "ru",
                 "Saudi Arabia": "sa",
                 "Singapore": "sg",
                 "South Africa": "za",
                 "Spain": "es",
                 "Sweden": "se",
                 "Switzerland": "ch",
                 "Taiwan": "tw",
                 "Turkey": "tr",
                 "Ukraine": "ua",
                 "United Arab Emirates": "ae",
                 "United Kingdom": "gb",
                 "Venezuela": "ve"}

    def __init__(self, publisher, version=2):
        """
        Initialize Indeed with publisher ID and version of API
        :publisher - Publisher ID.
                     To get the Publisher ID you need to register
                     Publisher account - http://www.indeed.com/publisher
                     and go to page https://ads.indeed.com/jobroll/xmlfeed
                     to find there a string like:

                     Your publisher ID is "12345678901234567890".

                     12345678901234567890 is your Publisher ID
        :version - Version. Which version of the API you wish to use.
                   All publishers should be using version 2.
                   Currently available versions are 1 and 2.

        """
        self.publisher = publisher
        self.version = version
        self.totalResults = 0
        self.format_results = "json"
        self.url = "http://api.indeed.com/ads/apisearch?publisher={publisher}"\
                   "&v={version}"\
                   "&format={format_results}"\
                   "&callback={callback}"\
                   "&q={query}"\
                   "&l={location}"\
                   "&sort={sort}"\
                   "&radius={radius}"\
                   "&st={site_type}"\
                   "&jt={job_type}"\
                   "&start={start}"\
                   "&limit={limit}"\
                   "&fromage={fromage}"\
                   "&highlight={highlight}"\
                   "&filter={filter}"\
                   "&latlong={latlong}"\
                   "&co={country}"\
                   "&chnl={chnl}"\
                   "&userip={userip}"\
                   "&useragent={useragent}"
        self.results = []

    def search_jobs(self, query, format_results="json", callback="",
                    location="", state="", sort="", radius=25, site_type="",
                    job_type="", start=0, limit=25, fromage="",
                    highlight=False, filter_dups=1,
                    latlong="", country="", chnl="",
                    userip="",
                    useragent="Mozilla/5.0 " +
                              "(Macintosh; Intel Mac OS X 10_8_2)"):
        """
        Main search function
        :query - Query. By default terms are ANDed. To see what is possible,
                 use http://www.indeed.com/advanced_search
                 to perform a search and then
                 check the url for the q value
        :format_results - "xml" and "json". Default - "xml"
        :callback - Callback. The name of a javascript function to use as a
                    callback to which the results of the search are passed.
                    This only applies when format=json.
                    For security reasons,
                    the callback name is restricted letters,
                    numbers, and the underscore character.
        :location - Location. Use a postal code or a "city,
                 state/province/region" combination.
        :state - State code of some countries.
                 Austria -  state (bundesland)
                 http://de.wikipedia.org/wiki/Bundesland_(%C3%96sterreich)

                 Australia -  state (territory)
                 http://en.wikipedia.org/wiki/ISO_3166-2:AU

                 Belgium -  province
                 http://en.wikipedia.org/wiki/ISO_3166-2:BE

                 Brazil - state (estado)
                 http://en.wikipedia.org/wiki/ISO_3166-2:BR

                 Canada - province (territory)
                 http://en.wikipedia.org/wiki/ISO_3166-2:CA

                 France - region
                 http://en.wikipedia.org/wiki/List_of_FIPS_region_codes_(D-F)#FR:_France

                 Germany - state (bundesland)
                 http://en.wikipedia.org/wiki/ISO_3166-2:DE

                 India - state
                 http://en.wikipedia.org/wiki/ISO_3166-2:IN

                 Ireland - county
                 http://en.wikipedia.org/wiki/ISO_3166-2:IE

                 Italy - region
                 http://it.wikipedia.org/wiki/Regioni_italiane#Sigle.5Bsenza.C2.A0fonte.5D

                 Mexico - state (estado)
                 http://en.wikipedia.org/wiki/ISO_3166-2:MX

                 Netherlands - province
                 http://en.wikipedia.org/wiki/ISO_3166-2:NL

                 Spain - provice
                 http://en.wikipedia.org/wiki/ISO_3166-2:ES

                 Switzerland - canton
                 http://en.wikipedia.org/wiki/ISO_3166-2:CH

                 United Kingdom - country
                 http://en.wikipedia.org/wiki/ISO_3166-2:GB

                 United States - state
                 http://en.wikipedia.org/wiki/ISO_3166-2:US

        :sort - Sort by relevance or date. Default is relevance
        :radius - Distance from search location ("as the crow flies").
                  Default is 25.
        :site_type - Site type. To show only jobs from job board use "jobsite".
              For jobs from direct employer websites use "employer"
        :job_type - Job type. Allowed values: "fulltime",
              "parttime", "contract", "internship", "temporary"
        :start - Start results as this results returned per query.
                 Default is 10.
        :limit - Maximum number of results returned per query.
                 Default is 25.
        :fromage - Number of days back to search.
        :highlight - Setting this value to 1 will bold terms in the snippet
                     that are also present in q. Default is 0.
        :filter - Filter duplicate results. 0 turns off duplicate job filtering
                  Default is 0.
        :latlong - If latlong=1, returns latitude and longtitude information
                   for each job result. Default is 0.
        :country - Search within country specified. Default is US.
                   Available coutries:
                   United States, Argentina, Australia, Austria, Bahrain,
                   Belgium, Brazil, Canada, Chile, China, Colombia,
                   Czech Republic, Denmark, Finland, France, Germany,
                   Greece, Hong Kong, Hungary, India, Indonesia, Ireland,
                   Israel, Italy, Japan, Korea, Kuwait, Luxembourg, Malaysia,
                   Mexico, Netherlands, New Zealand, Norway, Oman,
                   Pakistan, Peru, Philippines, Poland, Portugal,
                   Qatar, Romania, Russia, Saudi Arabia, Singapore,
                   South Africa, Spain, Sweden, Switzerland, Taiwan, Turkey,
                   United Arab Emirates, United Kingdom, Venezuela
        :chnl - Channel name: Group API requests to a specific channel
        :userip - The IP number of the end-user to whom the job results will be
                  displayed. This field is required
        :useragent - The User-Agent (browser) of the end-user to whom the job
                     results will be displayed. This can be obtained from the
                     "User-Agent" HTTP request header from the end-user.
                     This field is required.
        """
        format_results = "xml" if format_results not in self.FORMAT_RESULTS\
            else format_results
        location = "{0}, {1}".format(location, state) if state else location
        sort = "" if sort not in self.SORT else sort
        site_type = "" if site_type not in self.SITE_TYPE else site_type
        job_type = "" if job_type not in self.JOB_TYPE else job_type
        highlight = +highlight
        filter_dups = +filter_dups
        country = self.COUNTRIES.get(country, "us")
        if self.results:
            self.results = []
        url = self.url.format(version=self.version, publisher=self.publisher,
                              format_results=format_results,
                              callback=callback, query=query,
                              location=location, sort=sort,
                              radius=radius,
                              site_type=site_type, job_type=job_type,
                              start=start, limit=limit, fromage=fromage,
                              highlight=highlight,
                              filter=filter_dups,
                              latlong=latlong, country=country, chnl=chnl,
                              userip=userip, useragent=useragent)
        print(url)
        response = requests.get(url, timeout=self.TIMEOUT)
        if format_results == "json":
            json_resp = response.json()['results']
            self.totalResults = response.json()['totalResults']
            #print(response.json())
            for i in json_resp:
                title = i['jobtitle']
                company = i['company']
                city = i['city']
                state = i['state']
                country = i['country']
                source = i['source']
                date = i['date']
                description = i['snippet']
                url = i['url']
                expired = i['expired']
                date_published = i['formattedRelativeTime']
                self.results.append(Element(title, company, city, state,
                                            country, source, date,
                                            description, url, expired,
                                            date_published))
        else:
            xml_resp = etree.fromstring(response.content)
            count_of_results = len(xml_resp.findall(".//date"))
            for i in range(count_of_results):
                title = xml_resp.findall(".//jobtitle")[i].text
                company = xml_resp.findall(".//company")[i].text
                city = xml_resp.findall(".//city")[i].text
                state = xml_resp.findall(".//state")[i].text
                country = xml_resp.findall(".//country")[i].text
                source = xml_resp.findall(".//source")[i].text
                date = xml_resp.findall(".//date")[i].text
                description = xml_resp.findall(".//snippet")[i].text
                url = xml_resp.findall(".//url")[i].text
                expired = False if xml_resp.findall(".//expired")[i].text == 'false' else\
                    True
                date_published = xml_resp.findall(".//formattedRelativeTime")[i].text

                self.results.append(Element(title, company, city, state, country,
                                            source, date, description, url, expired,
                                            date_published))
        return response


def main():
    """
    Main module
    """
    pass


if __name__ == '__main__':
    main()
