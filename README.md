### Basic setup

To build the project you will need ``docker`` and ``docker-compose``.
To build the image, first navigate to the root of the cloned repository and run:

- ``docker build . -f Dockerfile --tag my-image:0.0.1``
- ``docker-compose up airflow-init``
- ``docker-compose up -d``
