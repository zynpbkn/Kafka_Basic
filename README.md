# Kafka 3-Node Cluster Ödevi

Bu ödevde `docker-compose` ile 3-node (KRaft modunda) bir Kafka cluster'ı kurulmuş, topic işlemleri yapılmış ve `data-generator` ile `churn` topic'ine key'li mesajlar gönderilmiştir.

## Ortam

- Kafka imajı: `apache/kafka:4.1.0` (kafka1, kafka2, kafka3)
- Schema Registry: `confluentinc/cp-schema-registry:8.3.2`
- Host'tan erişim (EXTERNAL listener): `localhost:19092`, `localhost:29092`, `localhost:39092`
- Container içinden erişim: `kafka1:9092`, `kafka2:9092`, `kafka3:9092`

## 0. Cluster Kurulması

```bash
docker-compose up -d
```

Çıktı:

```
[+] Running 8/8
 ✔ Network kafka_cluster_knet        Created
 ✔ Volume kafka_cluster_kafka3-data  Created
 ✔ Volume kafka_cluster_kafka1-data  Created
 ✔ Volume kafka_cluster_kafka2-data  Created
 ✔ Container kafka3                  Started
 ✔ Container kafka1                  Started
 ✔ Container kafka2                  Started
 ✔ Container schema-registry         Started
```

## 1. `atscale` topic'ini oluşturma (2 partition, replication factor 1)

```bash
docker exec -it kafka1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka1:9092 \
  --create --topic atscale \
  --partitions 2 --replication-factor 1
```

Çıktı:

```
Created topic atscale.
```

## 2. Tüm topic'leri listeleme

```bash
docker exec -it kafka1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka1:9092 --list
```

Çıktı:

```
__consumer_offsets
_schemas
atscale
```

Kümede iki sistem topic'i (`__consumer_offsets`, `_schemas`) ve ödevde oluşturulan `atscale` topic'i bulunmaktadır.

## 3. `atscale` topic'ini describe etme

```bash
docker exec -it kafka1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka1:9092 \
  --describe --topic atscale
```

Çıktı:

```
Topic: atscale  TopicId: 9SYrDOJRQjW7BCkvWAkYDA PartitionCount: 2       ReplicationFactor: 1    Configs: min.insync.replicas=1,segment.bytes=1073741824
        Topic: atscale  Partition: 0    Leader: 2       Replicas: 2     Isr: 2  Elr:    LastKnownElr:
        Topic: atscale  Partition: 1    Leader: 3       Replicas: 3     Isr: 3  Elr:    LastKnownElr:
```

`atscale` topic'i 2 partition'a sahiptir ve partition'lar farklı broker'lara (2 ve 3) dağılmıştır.

## 4. Data-generator ile `churn` topic'ine veri gönderme

### 4.1 `churn` topic'ini oluşturma (3 partition)

```bash
docker exec -it kafka1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka1:9092 \
  --create --topic churn \
  --partitions 3 --replication-factor 1
```

Çıktı:

```
Created topic churn.
```

### 4.2 Data-generator kurulumu

```bash
git clone https://github.com/erkansirin78/data-generator.git
uv init .
uv add -r data-generator/requirements.txt
source .venv/bin/activate
cd data-generator/
```

### 4.3 Consumer'ların başlatılması

Üç ayrı terminal açılmış ve her birinde aşağıdaki komut çalıştırılmıştır. Üç consumer da `churn_group` grubundadır ve `kafka-console-consumer.sh` kullanılmıştır.

```bash
docker exec -it kafka1 /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka1:9092 \
  --topic churn \
  --property print.key=true \
  --property print.partition=true \
  --group churn_group
```

### 4.4 Verinin gönderilmesi (key = CustomerId)

```bash
curl https://raw.githubusercontent.com/erkansirin78/datasets/master/Churn_Modelling.csv -O

python dataframe_to_kafka.py \
  -i Churn_Modelling.csv \
  -t churn \
  -b localhost:19092 \
  -k 1 \
  -rst 0.01
```

Parametreler:

| Parametre | Açıklama |
|-----------|----------|
| `-i` | Kaynak CSV dosyası |
| `-t` | Hedef topic (`churn`) |
| `-b` | Bootstrap server (host'tan EXTERNAL listener) |
| `-k 1` | Key olarak gönderilecek kolonun indeksi. Kolon sırası `RowNumber(0), CustomerId(1), ...` olduğu için `CustomerId` key olarak gönderilir |
| `-rst 0.01` | Satır başına bekleme süresi (saniye), gönderimi hızlandırmak için |

Gönderim, dosya tamamlanmadan Ctrl+C ile durdurulmuştur. `churn` topic'indeki toplam kayıt sayısı 1124'tür (aşağıdaki offset toplamı: 385 + 376 + 363). Son consumer oturumunda üç consumer toplam 719 mesaj işlemiştir (232 + 270 + 217).

### 4.5 Consumer çıktıları

Her satırda `Partition:N`, ardından key (`CustomerId`), ardından mesaj değeri görünür. Üç consumer üç ayrı partition'ı okumuştur: Consumer-1 Partition 2, Consumer-2 Partition 0, Consumer-3 Partition 1. Mesaj değerindeki ikinci kolon (`CustomerId`) key ile aynıdır.

**Consumer-1 (Partition 2):**

```
Partition:2     15747867        685,15747867,Trevisani,583,France,Male,24,9,135125.28,1,0,0,89801.9,0
Partition:2     15600337        686,15600337,Dobie,661,Spain,Male,42,2,178820.91,1,0,0,29358.57,1
Partition:2     15748625        696,15748625,Napolitano,664,France,Male,57,6,0.0,2,1,1,15304.08,0
Partition:2     15620204        698,15620204,Walker,543,Germany,Female,57,1,106138.33,2,1,1,120657.32,1
Partition:2     15669516        699,15669516,Steele,746,Spain,Male,36,2,0.0,2,1,1,16436.56,0
^CProcessed a total of 232 messages
```

**Consumer-2 (Partition 0):**

```
Partition:0     15773456        624,15773456,Lazareva,678,Germany,Male,36,3,145747.67,2,0,1,89566.74,0
Partition:0     15712403        631,15712403,McMillan,589,France,Female,61,1,0.0,1,1,0,61108.56,1
Partition:0     15738181        635,15738181,Douglas,850,France,Male,31,6,67996.23,2,0,0,50129.87,1
Partition:0     15633648        636,15633648,Jideofor,696,Spain,Female,51,5,0.0,2,1,0,55022.43,0
Partition:0     15603323        637,15603323,Bell,660,Spain,Female,33,1,0.0,2,0,0,117834.91,0
^CProcessed a total of 270 messages
```

**Consumer-3 (Partition 1):**

```
Partition:1     15699637        662,15699637,Anenechi,694,Spain,Male,57,8,116326.07,1,1,1,117704.65,0
Partition:1     15667215        663,15667215,Chandler,678,France,Male,31,2,0.0,2,1,1,58803.28,0
Partition:1     15575024        668,15575024,Uwaezuoke,503,France,Male,29,3,0.0,2,1,1,143954.99,0
Partition:1     15674991        672,15674991,Kao,667,France,Male,42,9,0.0,2,0,1,58137.42,0
Partition:1     15780804        681,15780804,Nucci,482,France,Male,55,5,97318.25,1,0,1,78416.14,0
^CProcessed a total of 217 messages
```

### 4.6 Consumer group dağılımı

```bash
docker exec -it kafka1 /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka1:9092 \
  --describe --group churn_group
```

Çıktı:

```
GROUP           TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID                                           HOST            CLIENT-ID
churn_group     churn           0          385             385             0               console-consumer-432ccf43-5897-46b3-83ec-349a9a6aa1d4 /172.18.0.2     console-consumer
churn_group     churn           1          376             376             0               console-consumer-4c0d30a2-c207-4e66-a4fc-c5725a3e3ba9 /172.18.0.2     console-consumer
churn_group     churn           2          363             363             0               console-consumer-96f74d2f-9b00-41ca-8320-f9aceba73816 /172.18.0.2     console-consumer
```

3 partition, 3 farklı consumer'a atanmıştır. Tüm partition'larda `LAG` 0'dır, yani mesajların tamamı tüketilmiştir.

## 5. `atscale` ve `churn` topic'lerini silme

Consumer'lar ve generator Ctrl+C ile durdurulduktan sonra:

```bash
docker exec -it kafka1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka1:9092 \
  --delete --topic atscale

docker exec -it kafka1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka1:9092 \
  --delete --topic churn
```

Doğrulama:

```bash
docker exec -it kafka1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka1:9092 --list
```

Çıktı:

```
__consumer_offsets
_schemas
```

`atscale` ve `churn` topic'leri listede görünmemektedir.

### Ek: Consumer group'un silinmesi

Temizlik amacıyla `churn_group` da silinmiştir.

```bash
docker exec -it kafka1 /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka1:9092 \
  --delete --group churn_group
```

Çıktı:

```
Deletion of requested consumer groups ('churn_group') was successful.
```