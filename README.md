# Learn kafka streams by making the tests pass

The goal of this project is to provide a bunch of tests with some challenges to be completed for anyone who wants to improve his/her Kafka streams skills.

The tests will start up a whole Kafka ecosystem infrastructure for you (Apache Kafka, Apache Zookeeper and Schema Registry) so, this way, you only need to focus on the fun part: play around with the Kafka Streams API!

I will be adding more challenges so please stay tuned. Collaborations, ideas on new challenges & PRs are more than welcome.


# The challenges

[1. Filtering events](#1-filtering-vip-clients)

[2. Enriching sale events](#2-enriching-sale-events)

## 1. Filtering VIP clients

In this challenge you will need to filter a stream of client events. These events will follow the following structure:

```
    {"name": "name", "type": "string"},
    {"name": "age",  "type": "int"},
    {"name": "vip",  "type": "boolean"}
```

To make the test pass you must get rid of all the clients who are not VIPs (vip field equals to false).

### Instructions

1. Start by executing the test (FilterTopologySpec) to make sure that the test runs smoothly. You can execute the test in your IDE or by executing the following console command:

```
mvn test
```

The test should execute properly but it also should fail (do not worry you need to make it pass).

2. Now it is time to make the test pass! To do so goes to the FilterTopologyBuilder class (within the filtering package)
3. Add your code to the filterVIPClients method
4. Execute the test again until you get the green flag ;)

## 2. Enriching sale events

In this challenge you will be facing a pretty common use case when working with streaming systems: enrich the events with information from a third-party system. In this scenario
for the sake of simplicity we will be using an in-memory hashmap to enrich events that simulate sales. The challenge is quite simple, the events will follow the following structure:

```
    {"name": "amount", "type": "float"},
    {"name": "product",  "type": "string"},
    {"name": "storeid",  "type": "int"}
```

On receiving an event you should check if the storeid exists within the storesInformation hashmap, if so, you should pick up the values storeAddress and storeCity from the hashmap, create a  
new event with the following structure:

```
    {"name": "amount", "type": "float"},
    {"name": "product",  "type": "string"},
    {"name": "storeaddress",  "type": "string"},
    {"name": "storecity", "type": "string"}
```

and send this event to the outputtopic. If the hashmap does not include the storeid of the event you should redirect this event to a error output topic.

If you have any further questions we strongly recommend to have a look at the EnrichmentTopologySpec test.

### Instructions

1. Start by executing the EnrichmentTopologySpec and check out that the tests run properly and fail
2. Go to the EnrichmentTopologyBuilder class (the one within the enrichment package)
3. Add your code within the createTopology method
4. Execute the test again until it passes

# Solutions

There is a solutions package where you will be able to find my proposed solution for the challenges. Try not to cheat and make the test pass on your own.

Do please let me know if you find a better solution by submitting your PRs.