import scrapy
from google.cloud import pubsub_v1

class BlogSpider(scrapy.Spider):
    name = 'blogspider'
    start_urls = ['https://www.ulta.com/hair-treatment?N=26xy',"https://www.ulta.com/makeup-face-powder?N=26y8","https://www.ulta.com/skin-care-cleansers-makeup-remover?N=27gx","https://www.ulta.com/womens-fragrance-perfume?N=26wq","https://www.ulta.com/bath-body-body-moisturizers-body-lotion-creams?N=26v4"]

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
            future = publisher.publish(
                topic_path, data, origin='python-sample', username='gcp'
            )
            print(future.result())

        print('Published messages with custom attributes.')
