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
    start_urls = ['https://www.ulta.com/hair-treatment?N=26xy']
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path('linux-249818', 'prod_sub')
    a = True
    count = 0
    data = []

    def start_requests(self):
        NUM_MESSAGES = 1
        while self.a:
            count = 0
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
        item['name'] = response.css('.ProductMainSection__productName > span::text').get()
        item['price'] = response.css('.ProductPricingPanel > span::text').get()
        self.ingest(item)

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
