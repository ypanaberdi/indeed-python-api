import datetime
import os

from indeed.PageCrawler import PageCrawler
from awslib import dynamodb
import boto3, sys, json, time, yaml


def main():
    """
    Main module
    """

    def purge_queues(config):
        print("queues purge process started")
        sqs = boto3.client('sqs')
        queue_dict = config['queues']

        for k, q in queue_dict.items():
            print("purging for queue {0} in process".format(q))
            try:
                queue_response = sqs.get_queue_url(QueueName=q)
                if 'ErrorResponse' in queue_response.keys():
                    print(response)
                else:
                    queue_url = queue_response['QueueUrl']
                    response = sqs.purge_queue(
                        QueueUrl=queue_url
                    )
            except Exception as e:
                print("Error: " + str(e))

        # let it purge the queue and wait according to the documentation
        print("sleeping 60 sec")
        time.sleep(60)
        print("done")
        return 0

    def kicker(config):
        print("Kicker started, queue name: %s" % config['queues']["crawler_tasks"])
        sqs = boto3.client('sqs')
        queue_url = (sqs.get_queue_url(QueueName=config['queues']["crawler_tasks"]))['QueueUrl']
        # use first entry in PYTHONPATH, there are better ways to do that.
        user_paths = os.environ['PYTHONPATH'].split(os.pathsep)

        zip_codes_filename = user_paths[0] + '/' + config["dicts"]["zip_codes"]
        job_titles_filename = user_paths[0] + '/' + config["dicts"]["titles_data"]

        counter = 0
        with open(zip_codes_filename) as filepointer_zip:
            zip_codes = json.load(filepointer_zip)
        with open(job_titles_filename) as filepointer_title:
            job_titles = json.load(filepointer_title)

        # todo: find the instance name to keep track of it
        """r = requests.get("http://169.254.169.254/latest/dynamic/instance-identity/document")
        response_json = r.json()
        region = response_json.get('region')
        instance_id = response_json.get('instanceId')
        """
        try:
            for title_x in job_titles:
                for zip_x in range(0, len(zip_codes)):
                    zip_code = zip_codes[zip_x]["zip"]
                    time_var = time.time().__str__()
                    datetime_var = datetime.datetime.utcnow().__str__()
                    response = sqs.send_message(
                        QueueUrl=queue_url,
                        DelaySeconds=1,
                        MessageAttributes={
                            'Origin': {
                                'DataType': 'String',
                                'StringValue': 'Kicker'
                            },
                            'Timestamp': {
                                'DataType': 'String',
                                'StringValue': time_var
                            }
                        },
                        MessageBody=json.dumps({
                            'title': title_x,
                            'zip_code': zip_code,
                            'datetime': datetime_var
                        })
                    )
                    if not response.keys().__contains__('MessageId'):
                        print("Error occured, no MessageId in response %s:" %response)
                        exit(1)
                    else:
                        counter += 1
                print("{0} messages sent with title {1} to sqs queue".format(counter, title_x))
                counter = 0
        except  Exception as e:
                print("Error: " + str(e))
        return 0

    def crawler(config):
        print("Crawler started, queue name for tasks: %s" % config['queues']["crawler_tasks"])
        sqs = boto3.resource('sqs')
        queue = sqs.get_queue_by_name(QueueName=config['queues']["crawler_tasks"])

        try:
            for message in queue.receive_messages(VisibilityTimeout=600, MaxNumberOfMessages=10, MessageAttributeNames=['All']):
                json_obj = json.loads(message.body)
                crawl_instance = PageCrawler(publisher_id=config["publisher_id"],
                                             job_title=json_obj["title"],
                                             location=json_obj["zip_code"],
                                             fromage=1)
                crawl_instance.crawl()
                # save all job_ids to DynamoDB in bulk
                print("fetched: " +len(crawl_instance.results).__str__()+ " jobs")
                dynamodb.updateJobs(crawl_instance.results)

                # Let the queue know that the message is processed
                print("Deleting message Timestamp '%s' from sqs" % message.message_attributes.get('Timestamp').get('StringValue'))
                message.delete()
        except Exception as e:
                print("Error: " + str(e))
                exit(1)
        wait_seconds = 5
        # sqs doesn't return all messages since it is decentralized, now we wait and re-run in recursion
        print("Crawler finished processing current batch, waiting for %s seconds" %wait_seconds)
        time.sleep(wait_seconds) # here we don't want to spam API.
        return crawler(config)

    def crawler_desc(configParams):
        print("Crawler for descriptions started")
        # check if job id and description already exists in DynamoDB, then don't bother
        a = dynamodb.jobExists()
        exit(1)
        return 0

    command = sys.argv[1]

    print("command passed to the main script: %s" % command)
    config_params = yaml.safe_load(open("ConfigParameters.yaml"))
    print(config_params)
    # map the inputs to the function blocks
    options = {'kicker': kicker,
               'crawler': crawler,
               'crawler_desc': crawler_desc,
               'purge_queues': purge_queues,
               }

    if not options.keys().__contains__(command):
        print("error, no options found for the command: %s" % command)

    options[command](config_params)


if __name__ == '__main__':
    main()
