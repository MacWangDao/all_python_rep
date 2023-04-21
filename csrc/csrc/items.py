# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class CsrcItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    original = scrapy.Field()
    original_url = scrapy.Field()
    original_id = scrapy.Field()
    update_id = scrapy.Field()
    title = scrapy.Field()
    summary = scrapy.Field()
    content = scrapy.Field()
    href = scrapy.Field()
    publisher_date = scrapy.Field()
    to_oracle_date = scrapy.Field()
    total_count = scrapy.Field()
    real_date = scrapy.Field()
