import scrapy
from google.cloud import pubsub_v1

class BlogSpider(scrapy.Spider):
    name = 'blogspider'
    start_urls = ['https://www.ulta.com/makeup-face-foundation?N=26y5',"https://www.ulta.com/makeup-face-blush?N=277v","https://www.ulta.com/makeup-eyes-eyeliner?N=26yh","https://www.ulta.com/makeup-face-setting-spray?N=10wj5jk","https://www.ulta.com/makeup-eye-primer-base?N=26yl","https://www.ulta.com/makeup-lips-lipstick?N=26ys","https://www.ulta.com/makeup-lip-liner?N=26yv","https://www.ulta.com/tools-brushes-makeup-brushes-tools-brush-sets?N=27ho","https://www.ulta.com/makeup-eyes-mascara?N=26yg"]
    count = 0

    def parse(self, response):
        temp = ["https://www.ulta.com"+i  for i in response.css('.prod-desc > a::attr(href)').getall()]
        print(len(temp))
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path('linux-249818', 'prod_pub')

        for n in temp:
            data = n
            # Data must be a bytestring
            data = data.encode('utf-8')
            # Add two attributes, origin and username, to the message
            self.count = self.count + 1
            future = publisher.publish(
                topic_path, data, origin='python-sample', username='gcp'
            )
            print(future.result())

        print('Published messages with custom attributes.')
        print(self.count,"Total URLS")
