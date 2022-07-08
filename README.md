# SparkML_Homework.OTUS

Набор данных в формате CSV: https://www.kaggle.com/arshid/iris-flower-dataset

Набор данных в формате LIBSVM: https://github.com/apache/spark/blob/master/data/mllib/iris_libsvm.txt

Описание набора данных: https://ru.wikipedia.org/wiki/%D0%98%D1%80%D0%B8%D1%81%D1%8B_%D0%A4%D0%B8%D1%88%D0%B5%D1%80%D0%B0

Задание 1:
Построить модель классификации Ирисов Фишера и сохранить её Описание набора данных: https://ru.wikipedia.org/wiki/%D0%98%D1%80%D0%B8%D1%81%D1%8B_%D0%A4%D0%B8%D1%88%D0%B5%D1%80%D0%B0 Набор данных в формате CSV: https://www.kaggle.com/arshid/iris-flower-dataset Набор данных в формате LIBSVM: https://github.com/apache/spark/blob/master/data/mllib/iris_libsvm.txt Должен быть предоставлен код построения модели (ноутбук или программа)
Задание 2:
Разработать приложение, которое читает из одной темы Kafka (например, "input") CSV-записи с четырми признаками ирисов, и возвращает в другую тему (например, "predictition") CSV-записи с теми же признаками и классом ириса Должен быть предоставлен код программы

Запускаем kafka в docker
```
docker-compose up -d
```

Создаем топики
```
docker exec sparkml_broker_1 kafka-topics --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 1 --topic input
docker exec sparkml_broker_1 kafka-topics --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 1 --topic prediction
```

Включаем трансляцию из топиков
```
docker exec sparkml_broker_1 kafka-console-consumer --bootstrap-server localhost:9092 --topic input
docker exec sparkml_broker_1 kafka-console-consumer --bootstrap-server localhost:9092 --topic prediction
```

Запускаем запись в топик input
```
docker exec sparkml_broker_1 bash -c "./scripts/output.sh | kafka-console-producer --bootstrap-server localhost:9092 --topic input"
```

Запускаем артефакт через Intelij Idea/cmd
