import datetime

from indeed.PageCrawler import PageCrawler
import boto3, sys, json, time, yaml


def main():
    """
    Main module
    """

    def purge_queues(config):
        print("queues purge process started")
        sqs = boto3.client('sqs')
        queue_list = [config["queue_kicker"], config["queue_crawler"], config["queue_crawler_descr"]]

        for q in queue_list:
            print("purging for queue '%s' in process" % q)
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
        print("Kicker started, queue name: %s" % config["queue_kicker"])
        sqs = boto3.client('sqs')
        queue_url = (sqs.get_queue_url(QueueName=config["queue_kicker"]))['QueueUrl']
        # zip_codes_filename = "/home/yerbol/PycharmProjects/Dtindeed/indeed-python-api/runner/zip_codes_top700.json"
        zip_codes_filename = config["dicts"]["zip_codes"]
        job_title = "data engineer"
        with open(zip_codes_filename) as filepointer:
            zip_codes = json.load(filepointer)
        # todo: find the instance name to keep track of it
        """r = requests.get("http://169.254.169.254/latest/dynamic/instance-identity/document")
        response_json = r.json()
        region = response_json.get('region')
        instance_id = response_json.get('instanceId')
        """
        try:
            for x in range(0, len(zip_codes)):
                zip_code = zip_codes[x]["zip"]
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
                        'title': job_title,
                        'zip_code': zip_code,
                        'datetime': datetime_var
                    })
                )
                print(response)
        except  Exception as e:
                print("Error: " + str(e))
        return 0

    def crawler(config):
        print("Crawler started, queue name for tasks: %s" % config["queue_kicker"])
        sqs = boto3.resource('sqs')
        queue = sqs.get_queue_by_name(QueueName=config["queue_kicker"])
        try:
            for message in queue.receive_messages(VisibilityTimeout=600, MaxNumberOfMessages=10, MessageAttributeNames=['All']):

                # check if job id already exists in DynamoDb


                # generate random sleep time not to spam API
                json_obj = json.loads(message.body)
                crawl_instance = PageCrawler(publisher_id=config["publisher_id"],
                                             job_title=json_obj["title"],
                                             location=json_obj["zip_code"],
                                             fromage=1)
                crawl_instance.crawl()
                for job in crawl_instance.results:
                    print(job.title)
                # save all job_ids to Dynamo in bulk

                # Let the queue know that the message is processed
                print("Deleting message Timestamp '%s' from sqs" % message.message_attributes.get('Timestamp').get('StringValue'))
                message.delete()
        except Exception as e:
               print("Error: " + str(e))
        wait_seconds = 5
        # sqs doesn't return all messages since it is decentralized, now we wait and re-run in recursion
        print("Crawler finished processing current batch, waiting for %s seconds" %wait_seconds)
        time.sleep(wait_seconds)
        return crawler(config)

    def crawler_desc(configParams):
        print("Crawler for descriptions started")
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
