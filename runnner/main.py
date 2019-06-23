from indeed.SingletonPageCrawler import SingletonPageCrawler


def main():
    """
    Main module
    """
    singleton_instance = SingletonPageCrawler(publisher_id="", job_title="data engineer")
    singleton_instance.crawl()


if __name__ == '__main__':
    main()
