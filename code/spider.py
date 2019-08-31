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
    pub_sub_ack = ""
    dataset_id = 'test'
    table_id = 'my_data'
    dup_urls = []
    first_row = True

    datastore_client = datastore.Client('linux-249818')
    task_key = datastore_client.key('Cas', 'Casper')
    task = datastore.Entity(key=task_key)


    client = bigquery.Client()
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    table = client.get_table(table_ref)


    def insert_bq(self,data):
        self.counter()
        row = tuple([str(data[field.name]) for field in self.table.schema ])
        if self.first_row:
            row_id = str(str(data['sku'])+'|'+str(data['size'])+'|'+str(data['price']))
            errors = self.client.insert_rows(self.table,[row],row_ids=[row_id])
            assert errors == []
            self.first_row = False
        row_id = str(str(data['sku'])+'|'+str(data['size'])+'|'+str(data['price']))
        errors = self.client.insert_rows(self.table,[row],row_ids=[row_id])
        assert errors == []
        self.subscriber.acknowledge(self.subscription_path, [self.pub_sub_ack])



    ack = []
    def __init__(self):
        super(BlogSpider, self).__init__()
        self.url = None

    def counter(self):
        res = self.datastore_client.get(self.task_key)
        self.task['count_crawl'] = res['count_crawl']
        self.task['count_spider'] = res['count_spider'] + 1
        self.task['failed'] = res['failed']
        self.datastore_client.put(self.task)

    def counter_failed(self):
        res = self.datastore_client.get(self.task_key)
        self.task['count_crawl'] = res['count_crawl']
        self.task['count_spider'] = res['count_spider']
        self.task['failed'] = res['failed'] + 1
        self.datastore_client.put(self.task)
        self.subscriber.acknowledge(self.subscription_path, [self.pub_sub_ack])


    def start_requests(self):
        while self.b:
            res_url = self.get_messages()
            if res_url:
                yield scrapy.Request(res_url,callback=self.parse,dont_filter=False)
            if self.a == False:
                self.b = False

        # yield self.subscriber.subscribe(self.subscription_path, self.callback).result()


    def get_messages(self):
        NUM_MESSAGES = 1
        response = self.subscriber.pull(self.subscription_path, max_messages=NUM_MESSAGES)
        if len(response.received_messages) >= 1:
            for res in response.received_messages:
                self.pub_sub_ack = res.ack_id
                if res.message.data.decode('utf-8'):
                    if res.message.data.decode('utf-8') in self.dup_urls:
                        self.subscriber.acknowledge(self.subscription_path, [res.ack_id])
                        return None
                    else:
                        self.dup_urls.append(res.message.data.decode('utf-8'))
                        return res.message.data.decode('utf-8')
                else:
                    return None
        else:
            print("No message")
            print(len(self.dup_urls))
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
            item['image'] = [data['image']]
            item['desc'] = data['description']
            item['url'] = response.url
            item['rating'] = data['aggregateRating']['ratingValue'] if 'aggregateRating' in data else ""
            item['review'] = data['aggregateRating']['reviewCount'] if 'aggregateRating' in data else ""
            self.insert_bq(item)
        except Exception as e:
            print(str(e))
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
