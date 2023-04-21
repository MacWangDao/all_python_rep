# Define here the models for your spider middleware
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/spider-middleware.html
import random
import time
import scrapy
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from scrapy import signals, Request

# useful for handling different item types with a single interface
from itemadapter import is_item, ItemAdapter
from scrapy.downloadermiddlewares.useragent import UserAgentMiddleware
from scrapy.http import HtmlResponse
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from logging import getLogger
import platform
import sys
import os

current_path = os.path.dirname(os.path.abspath(__file__))
father_path = os.path.dirname(current_path)


class CsrcSpiderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the spider middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_spider_input(self, response, spider):
        # Called for each response that goes through the spider
        # middleware and into the spider.

        # Should return None or raise an exception.
        return None

    def process_spider_output(self, response, result, spider):
        # Called with the results returned from the Spider, after
        # it has processed the response.

        # Must return an iterable of Request, or item objects.
        for i in result:
            yield i

    def process_spider_exception(self, response, exception, spider):
        # Called when a spider or process_spider_input() method
        # (from other spider middleware) raises an exception.

        # Should return either None or an iterable of Request or item objects.
        pass

    def process_start_requests(self, start_requests, spider):
        # Called with the start requests of the spider, and works
        # similarly to the process_spider_output() method, except
        # that it doesn’t have a response associated.

        # Must return only requests (not items).
        for r in start_requests:
            yield r

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)


class CsrcDownloaderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the downloader middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_request(self, request, spider):
        # Called for each request that goes through the downloader
        # middleware.

        # Must either:
        # - return None: continue processing this request
        # - or return a Response object
        # - or return a Request object
        # - or raise IgnoreRequest: process_exception() methods of
        #   installed downloader middleware will be called
        return None

    def process_response(self, request, response, spider):
        # Called with the response returned from the downloader.

        # Must either;
        # - return a Response object
        # - return a Request object
        # - or raise IgnoreRequest
        return response

    def process_exception(self, request, exception, spider):
        # Called when a download handler or a process_request()
        # (from other downloader middleware) raises an exception.

        # Must either:
        # - return None: continue processing this exception
        # - return a Response object: stops process_exception() chain
        # - return a Request object: stops process_exception() chain
        pass

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)


class IpProxyDownloadMiddleware(object):
    '''
    定义代理ip的类,这是开放代理的应用
    '''
    PROXIES = [
        '182.111.64.8:53364'
    ]

    def process_request(self, request, spider):
        proxy = random.choice(self.PROXIES)
        request.meta['proxy'] = proxy


class MyUserAgentMiddleware(UserAgentMiddleware):
    def __init__(self, user_agent=''):
        self.user_agent = user_agent

    def process_request(self, request, spider):
        ua = UserAgent()


class RandomUserAgentMiddlware(object):
    # 随机更换user-agent
    def __init__(self, crawler):
        super(RandomUserAgentMiddlware, self).__init__()
        self.ua = UserAgent()
        self.ua_type = crawler.settings.get("RANDOM_UA_TYPE", "random")  # random是默认值

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler)

    def process_request(self, request, spider):
        def get_ua():
            return getattr(self.ua, self.ua_type)  # 获取ua的ua_type

        request.headers.setdefault('User-Agent', get_ua())


class ImgDownloaderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the downloader middleware does not modify the
    # passed objects.

    user_agent_list = [
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 "
        "(KHTML, like Gecko) Chrome/22.0.1207.1 Safari/537.1",
        "Mozilla/5.0 (X11; CrOS i686 2268.111.0) AppleWebKit/536.11 "
        "(KHTML, like Gecko) Chrome/20.0.1132.57 Safari/536.11",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.6 "
        "(KHTML, like Gecko) Chrome/20.0.1092.0 Safari/536.6",
        "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.6 "
        "(KHTML, like Gecko) Chrome/20.0.1090.0 Safari/536.6",
        "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.1 "
        "(KHTML, like Gecko) Chrome/19.77.34.5 Safari/537.1",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/536.5 "
        "(KHTML, like Gecko) Chrome/19.0.1084.9 Safari/536.5",
        "Mozilla/5.0 (Windows NT 6.0) AppleWebKit/536.5 "
        "(KHTML, like Gecko) Chrome/19.0.1084.36 Safari/536.5",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.3 "
        "(KHTML, like Gecko) Chrome/19.0.1063.0 Safari/536.3",
        "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/536.3 "
        "(KHTML, like Gecko) Chrome/19.0.1063.0 Safari/536.3",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_0) AppleWebKit/536.3 "
        "(KHTML, like Gecko) Chrome/19.0.1063.0 Safari/536.3",
        "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.3 "
        "(KHTML, like Gecko) Chrome/19.0.1062.0 Safari/536.3",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.3 "
        "(KHTML, like Gecko) Chrome/19.0.1062.0 Safari/536.3",
        "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.3 "
        "(KHTML, like Gecko) Chrome/19.0.1061.1 Safari/536.3",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.3 "
        "(KHTML, like Gecko) Chrome/19.0.1061.1 Safari/536.3",
        "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/536.3 "
        "(KHTML, like Gecko) Chrome/19.0.1061.1 Safari/536.3",
        "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.3 "
        "(KHTML, like Gecko) Chrome/19.0.1061.0 Safari/536.3",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/535.24 "
        "(KHTML, like Gecko) Chrome/19.0.1055.1 Safari/535.24",
        "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/535.24 "
        "(KHTML, like Gecko) Chrome/19.0.1055.1 Safari/535.24"
    ]
    proxy_http = [
        '60.188.2.46:3000',
        '110.243.16.20:9999'
    ]
    proxy_https = [
        '60.179.201.207:3000',
        '60.179.200.202:3000'
    ]

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_request(self, request, spider):
        request.headers['User-Agent'] = random.choice(self.user_agent_list)
        request.meta['proxy'] = 'http://60.188.2.46:3000'
        # if request.url.split(':')[0] == 'http':
        #     request.meta['proxy'] = 'http://' + random.choice(self.proxy_http)
        # if request.url.split(':')[0] == 'https':
        #     request.meta['proxy'] = 'https://' + random.choice(self.proxy_https)
        return None

    def process_response(self, request, response, spider):
        # Called with the response returned from the downloader.

        # Must either;
        # - return a Response object
        # - return a Request object
        # - or raise IgnoreRequest
        return response

    def process_exception(self, request, exception, spider):
        if request.url.split(':')[0] == 'http':
            request.meta['proxy'] = 'http://' + random.choice(self.proxy_http)
        if request.url.split(':')[0] == 'https':
            request.meta['proxy'] = 'https://' + random.choice(self.proxy_https)
        return request  # 将修正后的请求对象重新进行请求发送

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)


class BytedanceDownloaderMiddleware:

    def __init__(self):
        self.driver = webdriver.Chrome()
        self.driver.maximize_window()
        self.wait = WebDriverWait(self.driver, 10)

    def __del__(self):
        self.driver.close()

    def process_request(self, request, spider):
        offset = request.meta.get('offset', 1)
        self.driver.get(request.url)
        time.sleep(1)
        if offset > 1:
            self.driver.find_element_by_xpath('.//*[@class="anticon"]').click()
        # html = self.driver.page_source
        # self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, '.m-itemlist .items .item')))
        return scrapy.http.HtmlResponse(url=request.url, body=self.driver.page_source.encode('utf-8'), encoding='utf-8',
                                        request=request,
                                        status=200)  # Called for each request that goes through the downloader


class SeleniumMiddleware_gpu:
    def __init__(self, timeout=None, service_args=[]):
        self.logger = getLogger(__name__)
        self.timeout = timeout
        self.browser = webdriver.PhantomJS(service_args=service_args)
        self.browser.set_window_size(1400, 700)
        self.browser.set_page_load_timeout(self.timeout)
        self.wait = WebDriverWait(self.browser, self.timeout)

    def __del__(self):
        self.browser.close()

    def process_request(self, request, spider):
        """
        用PhantomJS抓取页面
        :param request: Request对象
        :param spider: Spider对象
        :return: HtmlResponse
        """
        self.logger.debug('PhantomJS is Starting')
        page = request.meta.get('page', 1)
        try:
            self.browser.get(request.url)
            if page > 1:
                input = self.wait.until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, '#mainsrp-pager div.form > input')))
                submit = self.wait.until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, '#mainsrp-pager div.form > span.btn.J_Submit')))
                input.clear()
                input.send_keys(page)
                submit.click()
            self.wait.until(
                EC.text_to_be_present_in_element((By.CSS_SELECTOR, '#mainsrp-pager li.item.active > span'), str(page)))
            self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, '.m-itemlist .items .item')))
            return HtmlResponse(url=request.url, body=self.browser.page_source, request=request, encoding='utf-8',
                                status=200)
        except TimeoutException:
            return HtmlResponse(url=request.url, status=500, request=request)

    @classmethod
    def from_crawler(cls, crawler):
        return cls(timeout=crawler.settings.get('SELENIUM_TIMEOUT'),
                   service_args=crawler.settings.get('PHANTOMJS_SERVICE_ARGS'))


class ChromeSeleniumDownloaderMiddleware:

    def __init__(self):
        pass

    def __del__(self):
        pass

    def process_request(self, request, spider):

        try:
            self.webdriver = spider.webdriver
            self.webdriver.get(request.url)
            # doc = BeautifulSoup(self.webdriver.page_source, features='html.parser')
            if request.meta.get("method") == "json":
                return None
            return HtmlResponse(url=request.url, body=self.webdriver.page_source, request=request, encoding='utf-8',
                                status=200)
        except TimeoutException:

            return HtmlResponse(url=request.url, encoding='utf-8', status=500)

    def spider_closed(self):
        """Shutdown the driver when spider is closed"""
        print("spider_closed")
        pass

        # self.driver.quit()

    def process_response(self, request, response, spider):
        """
        :param request: 调度出来被Downloader Middleware处理的request对象
        :param response: Downloader Middleware处理request对象返回后的response对象
        :param spider: response返回来的spider对象
        """
        return response

    # 当process_request和process_response发生异常时调用
    def process_exception(self, request, exception, spider):
        # self.webdriver.quit()
        """
        :param request:  产生异常的request对象
        :param exception:  抛出的异常对象
        :param spider: 产生异常的request对象的spider对象
        """
        pass


class ChromeSeleniumDownloaderMiddlewareOne:

    def __init__(self):
        pass

    def __del__(self):
        pass

    def process_request(self, request, spider):

        try:

            # doc = BeautifulSoup(self.webdriver.page_source, features='html.parser')
            if request.meta.get("method") == "json":
                return None
            if request.meta.get("_id") not in [22, 23]:
                return None
            self.webdriver = spider.webdriver
            self.webdriver.get(request.url)
            return HtmlResponse(url=request.url, body=self.webdriver.page_source, request=request, encoding='utf-8',
                                status=200)
        except TimeoutException:

            return HtmlResponse(url=request.url, encoding='utf-8', status=500)

    def spider_closed(self):
        """Shutdown the driver when spider is closed"""
        print("spider_closed")
        pass

        # self.driver.quit()

    def process_response(self, request, response, spider):
        """
        :param request: 调度出来被Downloader Middleware处理的request对象
        :param response: Downloader Middleware处理request对象返回后的response对象
        :param spider: response返回来的spider对象
        """
        return response

    # 当process_request和process_response发生异常时调用
    def process_exception(self, request, exception, spider):
        # self.webdriver.quit()
        """
        :param request:  产生异常的request对象
        :param exception:  抛出的异常对象
        :param spider: 产生异常的request对象的spider对象
        """
        pass


class ChromeDownloaderMiddleware:

    def __init__(self):
        print("#####################__init__")
        options = webdriver.ChromeOptions()
        # options = Options()  # 实例化Option对象
        options.add_argument('--headless')  # 设置无界面
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-gpu')
        # options.add_argument('--disable-dev-shm-usage')
        """
        sudo mount -t tmpfs -o rw,nosuid,nodev,noexec,relatime,size=512M tmpfs /dev/shm
        """
        # options.add_experimental_option('excludeSwitches', ['enable-automation'])
        # options.add_experimental_option("prefs", {"profile.managed_default_content_settings.images": 2,
        #                                           "profile.managed_default_content_settings.flash": 1})
        if platform.system().lower() == "linux":
            self.webdriver = webdriver.Chrome(
                executable_path=os.path.join(father_path, "chromedriver_linux64/chromedriver"),
                options=options)
        else:
            self.webdriver = webdriver.Chrome(
                executable_path=os.path.join(father_path, "chromedriver_win32/chromedriver.exe"),
                options=options)
        self.webdriver.implicitly_wait(10)  # 等待元素最多10s
        self.webdriver.set_page_load_timeout(10)  # 页面10秒后强制中断加载
        '''
        prefs = {'profile.default_content_setting_values': {

        'images': 2,

        'javascript':2,

        "stylesheet": 2
         }}
        options.add_experimental_option('prefs',prefs)
        browser = webdriver.Chrome(chrome_options=options)
        '''

    def __del__(self):
        print("#####################   webdriver.quit")

        self.webdriver.quit()
        # self.webdriver.close()

    def process_request(self, request, spider):

        print("process_request")
        try:
            self.webdriver.get(request.url)
            # doc = BeautifulSoup(self.webdriver.page_source, features='html.parser')
            if request.meta.get("method") == "json":
                return None
            return HtmlResponse(url=request.url, body=self.webdriver.page_source, request=request, encoding='utf-8',
                                status=200)
        except TimeoutException:
            self.webdriver.close()
            return HtmlResponse(url=request.url, encoding='utf-8', status=500)

    def spider_closed(self):
        """Shutdown the driver when spider is closed"""
        print("spider_closed")
        pass

        # self.driver.quit()

    def process_response(self, request, response, spider):
        """
        :param request: 调度出来被Downloader Middleware处理的request对象
        :param response: Downloader Middleware处理request对象返回后的response对象
        :param spider: response返回来的spider对象
        """
        return response

    # 当process_request和process_response发生异常时调用
    def process_exception(self, request, exception, spider):
        # self.webdriver.quit()
        """
        :param request:  产生异常的request对象
        :param exception:  抛出的异常对象
        :param spider: 产生异常的request对象的spider对象
        """
        pass

    # 这个处理响应到底在程序运行过程中的哪一步？
    # def process_response(self, response, requset, spider):
    #     print(response.url)
    #     try:
    #         pure_text = BeautifulSoup(response.body).get_text()
    #         if pure_text.len < 200:
    #             # self.webdriver.get(url=response.url)
    #             # wait = WebDriverWait(self.webdriver, timeout=20)
    #             return HtmlResponse(url=response.url, body=self.webdriver.page_source, encoding='utf-8',
    #                                 )  # 返回selenium渲染之后的HTML数据
    #         else:
    #             return response
    #     except TimeoutException:
    #         return HtmlResponse(url=response.url, encoding='utf-8', status=500)
    #     finally:
    #         print('Chrome driver end...')


from importlib import import_module

from scrapy import signals
from scrapy.exceptions import NotConfigured
from scrapy.http import HtmlResponse
from selenium.webdriver.support.ui import WebDriverWait


class SeleniumRequest(Request):
    """Scrapy ``Request`` subclass providing additional arguments"""

    def __init__(self, wait_time=None, wait_until=None, screenshot=False, script=None, *args, **kwargs):
        """Initialize a new selenium request
        Parameters
        ----------
        wait_time: int
            The number of seconds to wait.
        wait_until: method
            One of the "selenium.webdriver.support.expected_conditions". The response
            will be returned until the given condition is fulfilled.
        screenshot: bool
            If True, a screenshot of the page will be taken and the data of the screenshot
            will be returned in the response "meta" attribute.
        script: str
            JavaScript code to execute.
        """

        self.wait_time = wait_time
        self.wait_until = wait_until
        self.screenshot = screenshot
        self.script = script

        super().__init__(*args, **kwargs)


class SeleniumMiddleware:
    """Scrapy middleware handling the requests using selenium"""

    def __init__(self, driver_name, driver_executable_path,
                 browser_executable_path, command_executor, driver_arguments):
        """Initialize the selenium webdriver
        Parameters
        ----------
        driver_name: str
            The selenium ``WebDriver`` to use
        driver_executable_path: str
            The path of the executable binary of the driver
        driver_arguments: list
            A list of arguments to initialize the driver
        browser_executable_path: str
            The path of the executable binary of the browser
        command_executor: str
            Selenium remote server endpoint
        """

        webdriver_base_path = f'selenium.webdriver.{driver_name}'

        driver_klass_module = import_module(f'{webdriver_base_path}.webdriver')
        driver_klass = getattr(driver_klass_module, 'WebDriver')

        driver_options_module = import_module(f'{webdriver_base_path}.options')
        driver_options_klass = getattr(driver_options_module, 'Options')

        driver_options = driver_options_klass()

        if browser_executable_path:
            driver_options.binary_location = browser_executable_path
        for argument in driver_arguments:
            driver_options.add_argument(argument)

        driver_kwargs = {
            'executable_path': driver_executable_path,
            f'{driver_name}_options': driver_options
        }

        # locally installed driver
        if driver_executable_path is not None:
            driver_kwargs = {
                'executable_path': driver_executable_path,
                f'{driver_name}_options': driver_options
            }
            self.driver = driver_klass(**driver_kwargs)
        # remote driver
        elif command_executor is not None:
            from selenium import webdriver
            capabilities = driver_options.to_capabilities()
            self.driver = webdriver.Remote(command_executor=command_executor,
                                           desired_capabilities=capabilities)

    @classmethod
    def from_crawler(cls, crawler):
        """Initialize the middleware with the crawler settings"""

        driver_name = crawler.settings.get('SELENIUM_DRIVER_NAME')
        driver_executable_path = crawler.settings.get('SELENIUM_DRIVER_EXECUTABLE_PATH')
        browser_executable_path = crawler.settings.get('SELENIUM_BROWSER_EXECUTABLE_PATH')
        command_executor = crawler.settings.get('SELENIUM_COMMAND_EXECUTOR')
        driver_arguments = crawler.settings.get('SELENIUM_DRIVER_ARGUMENTS')

        if driver_name is None:
            raise NotConfigured('SELENIUM_DRIVER_NAME must be set')

        if driver_executable_path is None and command_executor is None:
            raise NotConfigured('Either SELENIUM_DRIVER_EXECUTABLE_PATH '
                                'or SELENIUM_COMMAND_EXECUTOR must be set')

        middleware = cls(
            driver_name=driver_name,
            driver_executable_path=driver_executable_path,
            browser_executable_path=browser_executable_path,
            command_executor=command_executor,
            driver_arguments=driver_arguments
        )

        crawler.signals.connect(middleware.spider_closed, signals.spider_closed)

        return middleware

    def process_request(self, request, spider):
        """Process a request using the selenium driver if applicable"""

        if not isinstance(request, SeleniumRequest):
            return None

        self.driver.get(request.url)

        for cookie_name, cookie_value in request.cookies.items():
            self.driver.add_cookie(
                {
                    'name': cookie_name,
                    'value': cookie_value
                }
            )

        if request.wait_until:
            WebDriverWait(self.driver, request.wait_time).until(
                request.wait_until
            )

        if request.screenshot:
            request.meta['screenshot'] = self.driver.get_screenshot_as_png()

        if request.script:
            self.driver.execute_script(request.script)

        body = str.encode(self.driver.page_source)

        # Expose the driver via the "meta" attribute
        request.meta.update({'driver': self.driver})

        return HtmlResponse(
            self.driver.current_url,
            body=body,
            encoding='utf-8',
            request=request
        )

    def spider_closed(self):
        """Shutdown the driver when spider is closed"""

        self.driver.quit()
