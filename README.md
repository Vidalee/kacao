# Kacao [![Go Tests](https://github.com/Vidalee/kacao/actions/workflows/test.yaml/badge.svg)](https://github.com/Vidalee/kacao/actions/workflows/test.yaml) [![codecov](https://codecov.io/gh/Vidalee/kacao/branch/master/graph/badge.svg)](https://codecov.io/gh/Vidalee/kacao)

Kacao is a kubectl-style Kafka CLI for your daily needs.

Features:
- Fully covered by unit tests
- Start offset specifiable consume command
- Retrieve messages from a topic with filtering

  Example:
  `kacao get messages <topic_name> --limit 10 --key my-key --header key1=value1 --header key2=*`

  Will retrieve 10 messages from each partition of the topic <topic_name> and filter for messages that have for key "my-key", and headers with key1=value1 and key2 having any value.
- Produce messages with specified key and headers
- Retrieve number of messages of a topic in total and per partition



```
Available Commands:
  completion  Generate the autocompletion script for the specified shell

  config      Manage Kacao configuration
    delete-cluster Delete the specified cluster from the Kacao configuration
    delete-context Delete the specified context from the Kacao configuration
    get-clusters   Display clusters defined in the Kacao configuration
    get-contexts   Display contexts defined in the Kacao configuration
    set-cluster    Setup a cluster configuration
    set-context    Setup a context configuration
    use-context    Set the current context

  consume     Consume messages from a topic

  create      Create a resource
    partition   Add partitions to a topic
    topic       Create a topic

  delete      Delete one or many resources
    topic       Delete a topic

  describe    Describe one or many resources
    partition   Describe a topic's partition
    topic       Describe a topic of the current cluster

  get         Display one or many resources
    brokers     Display brokers of the current cluster
    messages    Get messages from a topic
    partitions  Display partitions of a topic
    topics      Display topics of the current cluster

  help        Help about any command

  produce     Produce messages to a topic

```