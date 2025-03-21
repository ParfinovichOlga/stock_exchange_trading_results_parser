from dateutil import parser
from selenium import webdriver
from selenium.webdriver.common.by import By
import time


class LinkCollection:
    """Class for collecting links."""

    def __init__(self, start_from, url):
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_experimental_option('excludeSwitches', ['disable-popup-blocking'])
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)

        self.driver = webdriver.Chrome(options=chrome_options)
        self.start_date = start_from
        self.url = url
        self.links = []
        self._next_css_sel = 'li.bx-pag-next a'
        self._link_css_sel = '#comp_d609bce6ada86eff0b6f7e49e6bae904 div.accordeon-inner__wrap-item a'

    @staticmethod
    def get_file_date(file_url: str):
        """Retrieve date from file name."""
        date = parser.parse(file_url.split('_')[-1][:8]).date()
        return date

    def _get_links(self):
        """Get download links from the page."""
        link_obj = self.driver.find_elements(By.CSS_SELECTOR, self._link_css_sel)
        return [
            t.get_attribute('href') for t in link_obj
            if self.get_file_date(t.get_attribute('href')) >= self.start_date
        ]

    def grab_links(self):
        """Collect links from url"""
        self.driver.get(self.url)
        time.sleep(1)
        str_obj = self._get_links()
        if str_obj:
            self.links.extend(str_obj)
            next_page = self.driver.find_element(By.CSS_SELECTOR, self._next_css_sel)
            self.url = next_page.get_attribute('href')
            self.grab_links()
        return self.links
