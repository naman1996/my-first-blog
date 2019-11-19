{
  "paragraphs": [
    {
      "text": "%md",
      "user": "namanu@qubole.com",
      "dateUpdated": "Nov 19, 2019 6:52:13 AM",
      "config": {
        "colWidth": 12.0,
        "graph": {
          "mode": "table",
          "height": 300.0,
          "optionOpen": false,
          "keys": [],
          "values": [],
          "groups": [],
          "scatter": {}
        },
        "enabled": true
      },
      "settings": {
        "params": {},
        "forms": {}
      },
      "version": "v0",
      "jobName": "paragraph_1574142435281_1378078064",
      "id": "20191119-054715_1926594556_q_1ANMCNVVGX1574142406",
      "dateCreated": "Nov 19, 2019 5:46:46 AM",
      "status": "READY",
      "progressUpdateIntervalMs": 1000
    },
    {
      "text": "import sys.process._\nimport org.apache.spark.sql.SparkSession\nimport org.apache.spark.sql.functions._\nimport org.apache.spark.sql.streaming._\nimport org.apache.spark.sql.types._\n\nimport org.apache.kafka.clients.producer.{ProducerConfig, KafkaProducer, ProducerRecord}\nimport org.apache.log4j.{Level, Logger}\n\nimport org.apache.kafka.clients.producer.{ProducerConfig, KafkaProducer, ProducerRecord}\nimport java.util.HashMap\n\nobject QuboleKafka extends Serializable {\n    \n    val zkhost \u003d (\"/usr/lib/spark/bin/qcmd master-public-dns\".!!).trim\n    val kafkahost \u003d zkhost\n    val zkhostport \u003d s\"$zkhost:2181\"\n    val topics \u003d \"qubole\"\n    val topicMap \u003d topics.split(\",\").map((_, 4)).toMap\n    val topicToSend \u003d topics.split(\",\")(0)\n    val group \u003d \"group\"\n    val brokers \u003d s\"$kafkahost:9092\"\n\n    val props \u003d new HashMap[String, Object]()\n    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)\n    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,\n      \"org.apache.kafka.common.serialization.StringSerializer\")\n    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,\n      \"org.apache.kafka.common.serialization.StringSerializer\")\n      \n    @transient var producer : KafkaProducer[String, String] \u003d null\n    var messageId : Long \u003d 1\n    @transient var query : StreamingQuery \u003d null\n    val checkpointPathQuery \u003d \"/tmp/testdata/checkpoint\"\n    val outputPathQuery \u003d \"/tmp/testdata/output\"\n    \n    // Start of get Spark Session. This is starting pointing of streaming job\n    val spark \u003d SparkSession.builder.appName(\"Azure Kafka Streaming Example\").getOrCreate()\n\n    import spark.implicits._    \n\n    def start() \u003d {\n        //Start producer for kafka\n        producer \u003d new KafkaProducer[String, String](props)\n        \n        //Creare a datastream from Kafka topics. StartingOffsets is earliest. So we always read from beginning of the kafka queue\n        val df \u003d spark\n            .readStream\n            .format(\"kafka\")\n            .option(\"kafka.bootstrap.servers\", brokers)\n            .option(\"subscribe\", topics)\n            .option(\"startingOffsets\", \"earliest\")\n            .load()\n        \n        df.printSchema()  \n        \n        // covert datastream into a datasets and convert the stream into multiple rows by applying appropriate schema\n        val ds \u003d df.selectExpr(\"CAST(key AS STRING)\", \"CAST(value AS STRING)\").as[(String, String)]\n\n        // write your streaming job here. We can have multple streaming query on same datasets. \n        // Here we are aggregating the rows by the message and saving it as a table in memory\n        query \u003d ds\n            .select(\"value\").groupBy(\"value\").count()\n            .writeStream\n            .queryName(\"aggregates\")\n            .format(\"memory\")\n            .outputMode(\"complete\")\n            .option(\"checkpointLocation\",checkpointPathQuery)\n            .start()\n    }\n    \n    // Any sql query on output table\n    def print10Rows() \u003d {\n        spark.sql(\"select * from aggregates limit 10\").show()\n    }\n    \n    def stop() \u003d {\n        query.stop()\n    }\n\n    def stopSpark() \u003d {\n        spark.stop()\n    }\n    \n    def printExplain() \u003d {\n        query.explain(true)\n    }\n    \n    def printStatus() \u003d {\n        query.recentProgress\n    }\n\n    // Send message to kafka\n    def send(msg: String) \u003d {\n        val message \u003d new ProducerRecord[String, String](topicToSend, null, s\"$msg\")\n        messageId \u003d messageId + 1\n        producer.send(message)\n    }\n\n}\n\n",
      "user": "namanu@qubole.com",
      "dateUpdated": "Nov 19, 2019 6:59:58 AM",
      "config": {
        "colWidth": 12.0,
        "graph": {
          "mode": "table",
          "height": 300.0,
          "optionOpen": false,
          "keys": [],
          "values": [],
          "groups": [],
          "scatter": {}
        },
        "enabled": true,
        "editorMode": "ace/mode/scala"
      },
      "settings": {
        "params": {},
        "forms": {}
      },
      "paragraphProgress": {
        "jobs": [],
        "numCompletedTasks": 0,
        "numTasks": 0,
        "truncated": false
      },
      "version": "v1",
      "jobName": "paragraph_1574145107599_-1902309147",
      "id": "20191119-063147_1728381593_q_1ANMCNVVGX1574142406",
      "result": {
        "code": "SUCCESS",
        "type": "TEXT",
        "msg": "\nimport sys.process._\n\nimport org.apache.spark.sql.SparkSession\n\nimport org.apache.spark.sql.functions._\n\nimport org.apache.spark.sql.streaming._\n\nimport org.apache.spark.sql.types._\n\nimport org.apache.kafka.clients.producer.{ProducerConfig, KafkaProducer, ProducerRecord}\n\nimport org.apache.log4j.{Level, Logger}\n\nimport org.apache.kafka.clients.producer.{ProducerConfig, KafkaProducer, ProducerRecord}\n\n\nimport java.util.HashMap\ndefined object QuboleKafka\n"
      },
      "dateCreated": "Nov 19, 2019 6:31:47 AM",
      "dateSubmitted": "Nov 19, 2019 6:59:58 AM",
      "dateStarted": "Nov 19, 2019 6:59:58 AM",
      "dateFinished": "Nov 19, 2019 7:00:21 AM",
      "status": "FINISHED",
      "progressUpdateIntervalMs": 1000
    },
    {
      "text": "",
      "config": {},
      "settings": {
        "params": {},
        "forms": {}
      },
      "version": "v0",
      "jobName": "paragraph_1574146124292_786592525",
      "id": "20191119-064844_1190339494_q_1ANMCNVVGX1574142406",
      "dateCreated": "Nov 19, 2019 6:48:44 AM",
      "status": "READY",
      "progressUpdateIntervalMs": 1000
    }
  ],
  "name": "naman",
  "id": "1ANMCNVVGX1574142406",
  "angularObjects": {
    "2EVB7TESB907771574143125480:shared_process": [],
    "2EVE87CDT907771574143125480:shared_process": [],
    "2EVGC1QAY907771574143125481:shared_process": [],
    "2EVPKUY7Y907771574143125374:shared_process": []
  },
  "config": {
    "isDashboard": false,
    "defaultLang": "spark"
  },
  "info": {},
  "source": "FCN"
}