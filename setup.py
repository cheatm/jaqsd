from setuptools import setup, find_packages


REQUIRES = open("requirements.txt").readlines()


def list_packages():
    packages = find_packages("jaqsd")
    return ["jaqsd.%s" % name for name in packages]


setup(
    name="jaqsd",
    version="0.0.1",
    packages=find_packages(),
    install_requires=REQUIRES,
    entry_points={"console_scripts": ["jaqsd = jaqsd.entry_point:group"]}
)

