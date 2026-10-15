# QUESTIONS
Set up a 3-node kafka cluster as you see in the class using docker-compose.
## 1.
Create a topic named `atscale`, 2 partitions and replication factor 1.

## 2. 
List all topics.

## 3. 
Describe `atscale` topic.

## 4. 
Use data-generator and send `https://raw.githubusercontent.com/erkansirin78/datasets/master/Churn_Modelling.csv` to  3 partitioned `churn` topic.

- Message key should be CustomerId.

- Consume under `churn_group` and this group must have 3 consumer. 
    - Use different terminal for each consumer. 
    - Use `kafka-console-consumer.sh` as a consumer.

## 5. 
Delete `atscale` and `churn` topics.