

from setuptools import setup, find_packages


setup(name="tap-ms-graph",
      version="0.0.1",
      description="Singer.io tap for extracting data from MS_Graph API",
      author="Stitch",
      url="http://singer.io",
      classifiers=["Programming Language :: Python :: 3 :: Only"],
      py_modules=["tap_ms_graph"],
      install_requires=[
        "singer-python==6.1.1",
        "requests==2.32.4",
        "backoff==2.2.1"
      ],
      entry_points="""
          [console_scripts]
          tap-ms-graph=tap_ms_graph:main
      """,
      packages=find_packages(),
      package_data = {
          "tap_ms_graph": ["schemas/*.json"],
      },
      include_package_data=True,
)