package cn.trustfar.spark;

import cn.trustfar.db.DBUtil;
import cn.trustfar.db.SQLUtil;
import cn.trustfar.db.po.ViewData;
import cn.trustfar.utils.DataValidate;
import cn.trustfar.utils.DateTimeUtil;
import cn.trustfar.utils.TimeUtil;
import kafka.serializer.StringDecoder;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 智慧厅堂
 */
public class ZHTT {
    private static final Logger LOGGER = LoggerFactory.getLogger(ZHTT.class);

    public static void submit(JavaStreamingContext jssc, Map<String, String> kafkaParams) throws InterruptedException {

        final String separator = "\\|";
        try {
            Map<String, Integer> topicsAndPartitions = new HashMap<>();
            topicsAndPartitions.put("MQ_ZHTT_MONITOR", 8);
            JavaPairReceiverInputDStream<String, String> zhtt = KafkaUtils.createStream(
                    jssc, String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topicsAndPartitions, StorageLevel.MEMORY_AND_DISK_SER());

            //总交易量 交易成功率 响应时间
            zhtt.flatMap(message -> {
                String line = message._2;
                String[] fields = line.split(separator);

                long successCount = "success".equals(fields[6]) ? 1 : 0;
                long totalCount = 1;
                String startTimeStr = fields[5];
                String endTimeStr = fields[4];
                long respTime = 0;
                DateTime startTime = DateTimeUtil.parseDateTime(startTimeStr, null);
                DateTime endTime = DateTimeUtil.parseDateTime(endTimeStr, null);
                if (startTime != null && endTime != null) {
                    respTime = endTime.getMillis() - startTime.getMillis();
                }

                List<Tuple2> tuple2s = new ArrayList<>();
                tuple2s.add(new Tuple2<String, Tuple2>("responseTime", new Tuple2<Long, Long>(totalCount, respTime)));
                tuple2s.add(new Tuple2<String, Tuple2>("successRate", new Tuple2<Long, Long>(totalCount, successCount)));
                return tuple2s.iterator();
            }).filter(tuple2 -> {
                if ("responseTime".equals((String) tuple2._1)) {
                    return (long) (((Tuple2) tuple2._2)._1) != 0;
                } else return true;
            }).mapToPair(tuple2 -> new Tuple2<>((String) tuple2._1, new Tuple2<>((long) tuple2._2, (long) tuple2._1)))
                    .reduceByKey((x, y) -> new Tuple2<>(x._1 + y._1, x._2 + y._2))
                    .foreachRDD(rdd -> {
                        rdd.foreachPartition(partition -> {
                            partition.forEachRemaining(record -> {
                                String value = new BigDecimal(record._2._2 / record._2._1).setScale(2, BigDecimal.ROUND_HALF_UP).toString();
                                LOGGER.error(record._1 + ":" + value);
                                int indexId = -1;
                                String currentTimeMillisStr = TimeUtil.getDMCTimeStamp();
                                if ("responseTime".equals(record._1)) {
                                    indexId = 3;
                                    DBUtil.insert(SQLUtil.INSERT_VIEW_DATA, new ViewData(10, 1, currentTimeMillisStr, record._1, value));
                                } else if ("successRate".equals(record._1)) {
                                    indexId = 2;
                                }
                                DBUtil.insert(SQLUtil.INSERT_VIEW_DATA, new ViewData(10, indexId, currentTimeMillisStr, record._1, value));
                            });
                        });
                    });

            //机构交易量
            zhtt.mapToPair(message -> {
                String line = message._2;
                String[] fields = line.split(separator);
                return new Tuple2<>(fields[2], 1);
            }).filter(tuple -> tuple._2 != null)
                    .reduceByKey((x, y) -> x + y)
                    .foreachRDD(rdd -> {
                        rdd.foreachPartition(partition -> {
                            partition.forEachRemaining(record -> {
                                String currentTimeMillisStr = TimeUtil.getDMCTimeStamp();
                                DBUtil.insert(SQLUtil.INSERT_VIEW_DATA, new ViewData(10, 18, currentTimeMillisStr, record._1, String.valueOf(record._2)));
                            });
                        });
                    });
        } catch (Exception e) {
            LOGGER.error("", e);
        }

    }
}
