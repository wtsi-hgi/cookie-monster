from setuptools import setup, find_packages

setup(
    name="cookiemonster",

    version="0.5.0",

    author="HGI",
    author_email="hgi@sanger.ac.uk",

    packages=find_packages(exclude=["test"]),

    url="https://github.com/wtsi-hgi/cookie-monster",

    license="LICENSE",

    description="Cookie Monster.",
    long_description=open("README.md").read(),

    install_requires=[x for x in open("requirements.txt").read().splitlines() if "://" not in x],
    dependency_links=[x for x in open("requirements.txt").read().splitlines() if "://" in x]
)
