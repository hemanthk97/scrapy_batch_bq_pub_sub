import scrapy
from google.cloud import pubsub_v1
from google.cloud import datastore

class BlogSpider(scrapy.Spider):
    name = 'blogspider'
    start_urls = ['https://www.ulta.com/makeup-face-foundation?N=26y5',"https://www.ulta.com/makeup-face-blush?N=277v","https://www.ulta.com/makeup-eyes-eyeliner?N=26yh","https://www.ulta.com/makeup-face-setting-spray?N=10wj5jk","https://www.ulta.com/makeup-eye-primer-base?N=26yl","https://www.ulta.com/makeup-lips-lipstick?N=26ys","https://www.ulta.com/makeup-lip-liner?N=26yv","https://www.ulta.com/tools-brushes-makeup-brushes-tools-brush-sets?N=27ho","https://www.ulta.com/makeup-eyes-mascara?N=26yg"]
    datastore_client = datastore.Client('linux-249818')
    task_key = datastore_client.key('Cas', 'Casper')
    task = datastore.Entity(key=task_key)
    batch_settings = pubsub_v1.types.BatchSettings(
    max_bytes=1024,  # One kilobyte
    max_latency=1,   # One second
)
    publisher = pubsub_v1.PublisherClient(batch_settings)
    topic_path = publisher.topic_path('linux-249818', 'prod_pub')



    def count(self):
        res = self.datastore_client.get(self.task_key)
        print(res['count_crawl'])
        self.task['count_crawl'] = res['count_crawl'] + 1
        self.task['count_spider'] = res['count_spider']
        self.task['failed'] = res['failed']
        self.datastore_client.put(self.task)


    def parse(self, response):
        temp = ["https://www.ulta.com"+i  for i in response.css('.prod-desc > a::attr(href)').getall() if i]
        print(len(temp))
        for n in temp:
            data = n
            # Data must be a bytestring
            data = data.encode('utf-8')
            # Add two attributes, origin and username, to the message
            self.count()
            self.publisher.publish(
                self.topic_path, data, origin='python-sample', username='gcp'
            ).result()
