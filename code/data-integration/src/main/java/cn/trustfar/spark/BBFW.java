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

import java.math.BigDecimal;
import java.util.*;

/**
 * 报表服务
 */
public class BBFW {
    private static final Logger LOGGER = LoggerFactory.getLogger(BBFW.class);

    public static void submit(JavaStreamingContext jssc, Map<String, String> kafkaParams) throws InterruptedException {

        final String separator = "\\|";
        try {
            Map<String, Integer> topicsAndPartitions = new HashMap<>();
            topicsAndPartitions.put("MQ_BBFW_MONITOR", 8);
            JavaPairReceiverInputDStream<String, String> bbfw = KafkaUtils.createStream(
                    jssc, String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topicsAndPartitions, StorageLevel.MEMORY_AND_DISK_SER());

            //总任务数
            bbfw.flatMap(message -> {
                String line = message._2;
                String[] fields = line.split(separator);
//                int completedCount = "3".equals(fields[4]) ? 1 : 0;//执行完成 fields[4] == 3
//                int totalCount = "3".equals(fields[4]) || "4".equals(fields[4]) ? 1 : 0;//累计任务数 fields[4] == 3 | 4
//                int processingCount = "2".equals(fields[4]) ? 1 : 0; //正在执行任务数 fields[4] == 2
//                int readyCount = "1".equals(fields[4]) ? 1 : 0; // 准备执行 fields[4] == 1
//                int errorCount = "1".equals(fields[3]) ? 1 : 0; // 异常任务数 fields[3] == 1
                //每个粒度发全量的数据
                List<String> count = new ArrayList<>();
                if ("3".equals(fields[4]) || "4".equals(fields[4])) {
                    count.add("completed");
                }
                count.add("total");
                if ("2".equals(fields[4])) {
                    count.add("processing");
                }
                if ("1".equals(fields[4])) {
                    count.add("ready");
                }
                if ("1".equals(fields[3])) {
                    count.add("error");
                }
                return count.iterator();
            }).mapToPair(field -> {
                return new Tuple2<>(field, 1);
            }).reduceByKey((x, y) -> x + y).foreachRDD(rdd -> {
                rdd.foreachPartition(partition -> {
                    partition.forEachRemaining(record -> {
                        LOGGER.info(record._1 + ":" + record._2);
                        int indexId = 0;
                        switch (record._1) {
                            case "completed":
                                indexId = 11;
                                break;
                            case "total":
                                indexId = 12;
                                break;
                            case "processing":
                                indexId = 13;
                                break;
                            case "ready":
                                indexId = 14;
                                break;
                            case "error":
                                indexId = 15;
                                break;
                        }
                        if (indexId > 0) {
                            String currentTimeMillisStr = TimeUtil.getDMCTimeStamp();
                            DBUtil.insert(SQLUtil.INSERT_VIEW_DATA, new ViewData(6, indexId, currentTimeMillisStr, record._1, String.valueOf(record._2)));
                        }
                    });
                });
            });


        } catch (Exception e) {
            LOGGER.error("", e);
        }

    }
}
