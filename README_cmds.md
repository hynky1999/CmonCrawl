### How to run the docker image

First of all make sure you have docker installed.
Then you simply need to cd into the root folder and run the following command:
`docker-compose up`

The application will start up and by default it will run all tests that are definied. You can see output at command line(altough quite messy).

If you want to abort it you can at any time by pressing Ctrl+C.

### How to run specific tests

You can run just single test_file by running the following command:
`docker-compose run unittests test_file_name.py`
