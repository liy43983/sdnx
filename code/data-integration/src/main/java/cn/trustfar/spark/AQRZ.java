package cn.trustfar.spark;

import cn.trustfar.db.DBUtil;
import cn.trustfar.db.SQLUtil;
import cn.trustfar.db.po.ViewData;
import cn.trustfar.utils.DataValidate;
import cn.trustfar.utils.TimeUtil;
import com.alibaba.fastjson.JSONObject;
import kafka.serializer.StringDecoder;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

/**
 * 安全认证
 */
public class AQRZ {
    private static final Logger LOGGER = LoggerFactory.getLogger(AQRZ.class);

    public static void submit(JavaStreamingContext jssc, Map<String, String> kafkaParams) throws InterruptedException {

        final String separator = "\\|";
        try {
            Map<String, Integer> topicsAndPartitions = new HashMap<>();
            topicsAndPartitions.put("MQ_AQRZ_MONITOR", 8);
            JavaPairReceiverInputDStream<String, String> aqrz = KafkaUtils.createStream(
                    jssc, String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topicsAndPartitions, StorageLevel.MEMORY_AND_DISK_SER());

            //总交易量
            aqrz.mapToPair(message -> {
                String line = message._2;
                String[] fields = line.split(separator);
                int successCount = DataValidate.isInteger(fields[2]) ? Integer.valueOf(fields[2]) : 0;
                int failCount = DataValidate.isInteger(fields[3]) ? Integer.valueOf(fields[3]) : 0;
                return new Tuple2<>("totalCount", successCount + failCount);
            }).reduceByKey((x, y) -> x + y).foreachRDD(rdd -> {
                rdd.foreachPartition(partition -> {
                    partition.forEachRemaining(record -> {
                        LOGGER.error("totalCount" + ":" + record._2);
                        String currentTimeMillisStr = TimeUtil.getDMCTimeStamp();
                        DBUtil.insert(SQLUtil.INSERT_VIEW_DATA, new ViewData(8, 1, currentTimeMillisStr, record._1, String.valueOf(record._2)));
                    });
                });
            });

            //交易成功率
            aqrz.mapToPair(message -> {
                String line = message._2;
                String[] fields = line.split(separator);
                int successCount = DataValidate.isInteger(fields[2]) ? Integer.valueOf(fields[2]) : 0;
                int failCount = DataValidate.isInteger(fields[3]) ? Integer.valueOf(fields[3]) : 0;
                long totalCount = successCount + failCount;
                if (totalCount == 0) {
                    return new Tuple2<>("successRate", null);
                }
                return new Tuple2<>("successRate", new Tuple2<>(1,(double) successCount / totalCount));
            }).filter(tuple -> tuple._2 != null)
                    .reduceByKey((x, y) -> new Tuple2<>(x._1 + y._1, x._2 + y._2))
                    .foreachRDD(rdd -> {
                        rdd.foreachPartition(partition -> {
                            partition.forEachRemaining(record -> {
                                String value = new BigDecimal(record._2._2 / record._2._1).setScale(2, BigDecimal.ROUND_HALF_UP).toString();
                                LOGGER.error("successRate" + ":" + value);
                                String currentTimeMillisStr = TimeUtil.getDMCTimeStamp();
                                DBUtil.insert(SQLUtil.INSERT_VIEW_DATA, new ViewData(8, 2, currentTimeMillisStr, "successRate", value));
                            });
                        });
                    });
        } catch (Exception e) {
            LOGGER.error("", e);
        }

    }
}
