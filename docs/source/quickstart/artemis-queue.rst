Artemis Queue
=============

`Artemis <https://activemq.apache.org/components/artemis/>`_ is a message queue that is designed to be fast, scalable, and easy to use

We use artemis for communication between Aggregator and Processor.
If you use docker approach you don't need to install artemis manually.
However it's good to know that artemis provides a web console for monitoring and management.
You can connect to this console while running the docker containers by opening the following URL in your browser: `Console <http://localhost:8161/console/auth/login>`_.
The password is `admin` and the username is `admin`. There you can see the status of the queues.



