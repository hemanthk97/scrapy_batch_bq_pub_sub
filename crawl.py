import scrapy
from google.cloud import pubsub_v1
from google.cloud import datastore


class BlogSpider(scrapy.Spider):
    name = 'blogspider'
    start_urls = ['https://www.ulta.com/makeup-face-foundation?N=26y5',"https://www.ulta.com/makeup-face-blush?N=277v","https://www.ulta.com/makeup-eyes-eyeliner?N=26yh","https://www.ulta.com/makeup-face-setting-spray?N=10wj5jk","https://www.ulta.com/makeup-eye-primer-base?N=26yl","https://www.ulta.com/makeup-lips-lipstick?N=26ys","https://www.ulta.com/makeup-lip-liner?N=26yv","https://www.ulta.com/tools-brushes-makeup-brushes-tools-brush-sets?N=27ho","https://www.ulta.com/makeup-eyes-mascara?N=26yg","https://www.ulta.com/men-skin-care-face-wash?N=27cl","https://www.ulta.com/men-skin-care-moisturizers-treatments?N=27cm","https://www.ulta.com/men-shaving-cream-razors?N=rcphjk","https://www.ulta.com/men-shaving-aftershave?N=3hcaqj","https://www.ulta.com/men-shaving-beard-care?N=u5rnk6","https://www.ulta.com/tools-brushes-makeup-brushes-tools-brush-sets?N=27ho","https://www.ulta.com/tools-brushes-makeup-brushes-tools-makeup-brushes?N=27hp","https://www.ulta.com/tools-brushes-makeup-brushes-tools-sponges-applicators?N=27hq","https://www.ulta.com/tools-brushes-makeup-brushes-tools-brush-cleaner?N=27hr"]
    datastore_client = datastore.Client('linux-249818')
    task_key = datastore_client.key('Cas', 'Casper')
    task = datastore.Entity(key=task_key)
    batch_settings = pubsub_v1.types.BatchSettings(
    max_latency=5,   # One second
)
    publisher = pubsub_v1.PublisherClient(batch_settings)
    topic_path = publisher.topic_path('linux-249818', 'prod_pub')
    leng = 0
    urlss = []








    def count(self,num):
        res = self.datastore_client.get(self.task_key)
        print(res['count_crawl'])
        self.task['count_crawl'] = num
        self.task['count_spider'] = res['count_spider']
        self.task['failed'] = res['failed']
        self.datastore_client.put(self.task)


    def parse(self, response):
        temp = ["https://www.ulta.com"+i  for i in response.css('.prod-desc > a::attr(href)').getall() if i]
        print(len(temp))
        self.leng = self.leng + len(temp)

        # self.publisher.publish(
        #     self.topic_path, temp, origin='python-sample', username='gcp'
        # ).result()
        # self.count(len(temp))
        for n in temp:
            data = n
            # Data must be a bytestring
            data = data.encode('utf-8')
            # Add two attributes, origin and username, to the message
            self.urlss.append(data)
            future = self.publisher.publish(
                self.topic_path, data, origin='python-sample', username='gcp'
            )
        published = future.result()
        print(published, "Messages !!!")
        dup_len = [self.urlss[i] for i in range(len(self.urlss)) if i == self.urlss.index(self.urlss[i])]
        self.count(len(dup_len))
        print("Duplicates urls", len(dup_len))
