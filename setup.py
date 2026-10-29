from setuptools import find_packages, setup

# Used by Dataflow workers (--setup_file) so the lease_demo package is available remotely.
setup(
    name="lease-demo",
    version="0.1.0",
    package_dir={"": "src"},
    packages=find_packages("src"),
)
