# Automatically created by: shub deploy

from setuptools import setup, find_packages

setup(
    name         = 'scraper_build',
    version      = '1.0',
    packages     = find_packages(),
    package_data= {
        'scrape_n_geo': ['scrape_n_geo/scrape_n_geo/lookup.csv']
    },
    include_package_data=True,
    zip_safe=False,
    entry_points = {'scrapy': ['settings = scrape_n_geo.settings']},
)
