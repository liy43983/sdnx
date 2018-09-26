package cn.trustfar.spark;

import cn.trustfar.db.DBUtil;
import cn.trustfar.db.SQLUtil;
import cn.trustfar.db.po.ViewData;
import cn.trustfar.utils.DataValidate;
import cn.trustfar.utils.TimeUtil;
import kafka.serializer.StringDecoder;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class WJCS {
    private static final Logger LOGGER = LoggerFactory.getLogger(WJCS.class);

    public static void submit(JavaStreamingContext jssc, Map<String, String> kafkaParams) throws InterruptedException {
        final String defaultString = "-99";
        final String separator = "\\|";
        try {
            Map<String, Integer> topicsAndPartitions1 = new HashMap<>();
            topicsAndPartitions1.put("MQ_WJCS_STATS_MONITOR", 8);
            Map<String, Integer> topicsAndPartitions2 = new HashMap<>();
            topicsAndPartitions2.put("MQ_WJCS_EXEC_MONITOR", 8);
            Map<String, Integer> topicsAndPartitions3 = new HashMap<>();
            topicsAndPartitions3.put("MQ_WJCS_STATE_MONITOR", 8);
            Map<String, Integer> topicsAndPartitions4 = new HashMap<>();
            topicsAndPartitions4.put("MQ_WJCS_DELAY_MONITOR", 8);
            JavaPairReceiverInputDStream<String, String> stats = KafkaUtils.createStream(
                    jssc, String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topicsAndPartitions1, StorageLevel.MEMORY_AND_DISK_SER());
            JavaPairReceiverInputDStream<String, String> ecec = KafkaUtils.createStream(
                    jssc, String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topicsAndPartitions2, StorageLevel.MEMORY_AND_DISK_SER());
            JavaPairReceiverInputDStream<String, String> state = KafkaUtils.createStream(
                    jssc, String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topicsAndPartitions3, StorageLevel.MEMORY_AND_DISK_SER());
            JavaPairReceiverInputDStream<String, String> delay = KafkaUtils.createStream(
                    jssc, String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topicsAndPartitions4, StorageLevel.MEMORY_AND_DISK_SER());

            //排队任务数 正在执行任务数 完成任务数
            state.flatMap(message -> {
                String[] fields = message._2.split(separator);
                int queueTaskCount = DataValidate.isInteger(fields[2]) ? Integer.valueOf(fields[2]) : 0;
                int transferringTaskCount = DataValidate.isInteger(fields[3]) ? Integer.valueOf(fields[3]) : 0;
                int completedTaskCount = DataValidate.isInteger(fields[5]) ? Integer.valueOf(fields[5]) : 0;
                List<Tuple2> tuple2s = new ArrayList<>();
                tuple2s.add(new Tuple2<>("queueTaskCount", queueTaskCount));
                tuple2s.add(new Tuple2<>("transferringTaskCount", transferringTaskCount));
                tuple2s.add(new Tuple2<>("completedTaskCount", completedTaskCount));
                return tuple2s.iterator();
            }).mapToPair(tuple2 -> new Tuple2<>((String) tuple2._1, (int) tuple2._2))
                    .reduceByKey((x, y) -> x + y)
                    .foreachRDD(rdd -> {
                        rdd.foreachPartition(partition -> {
                            partition.forEachRemaining(record -> {
                                LOGGER.info(record._1 + ":" + record._2);
                                int indexId = -1;
                                if ("queueTaskCount".equals(record._1)) {
                                    indexId = 14;
                                } else if ("transferringTaskCount".equals(record._1)) {
                                    indexId = 13;
                                } else if ("completedTaskCount".equals(record._1)) {
                                    indexId = 11;
                                }
                                if (indexId > 0) {
                                    String currentTimeMillisStr = TimeUtil.getDMCTimeStamp();
                                    DBUtil.insert(SQLUtil.INSERT_VIEW_DATA, new ViewData(5, indexId, currentTimeMillisStr, record._1, String.valueOf(record._2)));
                                }
                            });
                        });
                    });
            //控制器异常状态
            stats.map(message -> {
                String[] fields = message._2.split(separator);
                return DataValidate.isInteger(fields[3]) ? Integer.valueOf(fields[3]) : 0;
            }).mapToPair(field -> new Tuple2<>("errorNodeCount",field))
                    .reduceByKey((x, y) -> x + y)
                    .foreachRDD(rdd -> {
                        rdd.foreachPartition(partition -> {
                            partition.forEachRemaining(record -> {
                                LOGGER.info(record._1 + ":" + record._2);
                                String currentTimeMillisStr = TimeUtil.getDMCTimeStamp();
                                DBUtil.insert(SQLUtil.INSERT_VIEW_DATA, new ViewData(5, 16, currentTimeMillisStr, record._1, String.valueOf(record._2)));
                            });
                        });
                    });


            //执行器异常状态
            ecec.map(message -> {
                String[] fields = message._2.split(separator);
                return DataValidate.isInteger(fields[3]) ? Integer.valueOf(fields[3]) : 0;
            }).mapToPair(field -> new Tuple2<>("errorNodeCount",field))
                    .reduceByKey((x, y) -> x + y)
                    .foreachRDD(rdd -> {
                        rdd.foreachPartition(partition -> {
                            partition.forEachRemaining(record -> {
                                LOGGER.info(record._1 + ":" + record._2);
                                    String currentTimeMillisStr = TimeUtil.getDMCTimeStamp();
                                    DBUtil.insert(SQLUtil.INSERT_VIEW_DATA, new ViewData(5, 17, currentTimeMillisStr, record._1, String.valueOf(record._2)));
                            });
                        });
                    });


        } catch (Exception e) {
            LOGGER.error("", e);
        }
    }
}
