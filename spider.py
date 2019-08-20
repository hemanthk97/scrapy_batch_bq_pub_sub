import scrapy
from google.cloud import pubsub_v1
import json
import os
from google.cloud import bigquery
import csv
import random
import time
import string
from google.cloud import datastore


class BlogSpider(scrapy.Spider):
    name = 'blogspider'
    start_urls = ['https://www.ulta.com/miracle-hair-mask?productId=xlsImpprod6481226']
    custom_settings = {
        'CONCURRENT_REQUESTS': 1
    }

    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path('linux-249818', 'prod_sub')
    a = True
    b = True
    count = 0
    data = []

    datastore_client = datastore.Client('linux-249818')
    task_key = datastore_client.key('Cas', 'Casper')
    task = datastore.Entity(key=task_key)

    ack = []
    def __init__(self):
        super(BlogSpider, self).__init__()
        self.url = None

    def counter(self):
        res = self.datastore_client.get(self.task_key)
        print(res['count_spider'])
        self.task['count_crawl'] = res['count_crawl']
        self.task['count_spider'] = res['count_spider'] + 1
        self.task['failed'] = res['failed']
        self.datastore_client.put(self.task)

    def counter_failed(self):
        res = self.datastore_client.get(self.task_key)
        print(res['count_spider'])
        self.task['count_crawl'] = res['count_crawl']
        self.task['count_spider'] = res['count_spider']
        self.task['failed'] = res['failed'] + 1
        self.datastore_client.put(self.task)


    def start_requests(self):
        while self.b:
            res_url = self.get_messages()
            if res_url:
                print(res_url,"Response URLS!!!!!!!!!!")
                yield scrapy.Request(res_url,callback=self.parse, dont_filter=True)
            if self.a == False:
                self.b = False

        # yield self.subscriber.subscribe(self.subscription_path, self.callback).result()


    def get_messages(self):
        NUM_MESSAGES = 1
        response = self.subscriber.pull(self.subscription_path, max_messages=NUM_MESSAGES)
        print(len(response.received_messages),"Len on message")
        if len(response.received_messages) >= 1:
            print("Recevied messages!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
            for res in response.received_messages:
                self.subscriber.acknowledge(self.subscription_path, [res.ack_id])
                if res.message.data.decode('utf-8'):
                    return res.message.data.decode('utf-8')
                else:
                    return None
        else:
            print("No message")
            self.count = self.count + 1
            if self.count == 2:
                self.a = False
                return None
            else:
                return None




    def parse(self, response):
        try:
            item =dict()
            data = json.loads(response.css('script[type="application/ld+json"]::text').get())
            item['name'] = data['name']
            item['price'] = data['offers']['price']
            item['size'] = response.css('.ProductMainSection__colorPanel *::text').get()
            item['sku'] = data['productID']
            item['desc'] = data['description']
            item['image'] = [data['image']]
            item['url'] = response.url
            item['rating'] = data['aggregateRating']['ratingValue']
            item['review'] = data['aggregateRating']['reviewCount']
            self.stream_pub(item)
        except:
            self.counter_failed()


        # item['name'] = response.css('.ProductMainSection__productName > span::text').get()
        # item['price'] = response.css('meta[property="product:price:amount"]::attr(content)').get()
        # item['size'] = response.css('.ProductMainSection__colorPanel *::text').get()
        # item['sku'] = response.css('.ProductMainSection__itemNumber *::text').get()
        # item['desc'] = response.css('meta[property="og:description"]::attr(content)').get()
        # item['image'] = [response.css('meta[property="og:image"]::attr(content)').get()]

        # print(item)


    def stream_pub(self,data):
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path('linux-249818', 'casper_pub')
        mess = json.dumps(data).encode('utf8')
        future = publisher.publish(
            topic_path, mess, origin='python-sample', username='gcp'
        ).result()
        self.counter()



    def ingest(self, data):
        self.data.append(data)
        if len(self.data) == 100:
            self.push_bq(self.data)
            self.data = []
    def random_char(self,y):
       return ''.join(random.choice(string.ascii_letters) for x in range(y))
    def push_bq(self,data):
        client = bigquery.Client()
        # filename = r'C:\Users\hemi8\Desktop\caspers_4.csv'
        dataset_id = 'test'
        table_id = 'my_data'
        filename = self.random_char(5)+".json"
        f = open(filename,"w",newline='',encoding="utf-8")
        for i in self.data:
            f.write(json.dumps(i)+'\n')
        # dataset_ref = client.dataset(dataset_id)
        f.close()
        table_ref = client.dataset(dataset_id).table(table_id)
        job_config = bigquery.LoadJobConfig()
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
        job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
        with open(filename, "rb") as source_file:
            job = client.load_table_from_file(source_file, table_ref, job_config=job_config)
        print("Started......")
        job.result()
        os.remove(filename)
