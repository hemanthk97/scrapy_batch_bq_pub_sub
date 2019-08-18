import scrapy
from google.cloud import pubsub_v1
import json
import os
from google.cloud import bigquery
import csv
import random
import string

# def test():
#     data = [{'name':'Hello','price':'12'},{'name':'Hello','price':'12'}]
#     result = [json.dumps(record) for record in data]
#
# test()
    # os.remove("dict.json")

class BlogSpider(scrapy.Spider):
    name = 'blogspider'
    start_urls = ['https://www.ulta.com/miracle-hair-mask?productId=xlsImpprod6481226']
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path('linux-249818', 'prod_sub')
    a = True
    count = 0
    data = []

    def start_requests(self):
        NUM_MESSAGES = 1
        while self.a:
            response = self.subscriber.pull(self.subscription_path, max_messages=NUM_MESSAGES)
            print(len(response.received_messages),"Len on message")
            if len(response.received_messages) >= 1:
                self.subscriber.acknowledge(self.subscription_path, [response.received_messages[0].ack_id])
                yield scrapy.FormRequest(response.received_messages[0].message.data.decode('utf-8'),
                                       callback=self.parse)
            else:
                print("No message")
                self.count = self.count + 1
                if self.count == 2:
                    self.a = False
                    if len(self.data) > 0:
                        self.push_bq(self.data)




    def parse(self, response):
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
        print(item)
        # item['name'] = response.css('.ProductMainSection__productName > span::text').get()
        # item['price'] = response.css('meta[property="product:price:amount"]::attr(content)').get()
        # item['size'] = response.css('.ProductMainSection__colorPanel *::text').get()
        # item['sku'] = response.css('.ProductMainSection__itemNumber *::text').get()
        # item['desc'] = response.css('meta[property="og:description"]::attr(content)').get()
        # item['image'] = [response.css('meta[property="og:image"]::attr(content)').get()]

        # print(item)
        self.stream_pub(item)

    def stream_pub(self,data):
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path('linux-249818', 'casper_pub')
        mess = json.dumps(data).encode('utf8')
        future = publisher.publish(
            topic_path, mess, origin='python-sample', username='gcp'
        )
        print(future.result())

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
