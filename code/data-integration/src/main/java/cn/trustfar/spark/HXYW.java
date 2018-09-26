package cn.trustfar.spark;

import cn.trustfar.db.ConnectionPool;
import cn.trustfar.db.DBUtil;
import cn.trustfar.db.SQLUtil;
import cn.trustfar.db.po.ViewData;
import cn.trustfar.utils.DataValidate;
import cn.trustfar.utils.TimeUtil;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairDStream;
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
 * 核心业务
 */
public class HXYW {
    private static final Logger LOGGER = LoggerFactory.getLogger(HXYW.class);

    public static void submit(JavaStreamingContext jssc, Map<String, String> kafkaParams) throws InterruptedException {

        final String separator = " ";

        try {

            Map<String, Integer> topicsAndPartitions = new HashMap<>();
            topicsAndPartitions.put("MQ_HXFW_BEGIN_MONITOR", 3);
            Map<String, Integer> topicsAndPartitions2 = new HashMap<>();
            topicsAndPartitions2.put("MQ_HXFW_END_MONITOR", 3);

            JavaPairReceiverInputDStream<String, String> begin = KafkaUtils.createStream(
                    jssc, String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topicsAndPartitions, StorageLevel.MEMORY_AND_DISK_SER());
            JavaPairReceiverInputDStream<String, String> end = KafkaUtils.createStream(
                    jssc, String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topicsAndPartitions2, StorageLevel.MEMORY_AND_DISK_SER());


            // 总交易量
            begin.union(end).mapToPair(message -> {
                int beginTradeCount = 0;
                int endTradeCount = 0;
                if (message._2.startsWith("B")) {
                    beginTradeCount = 1;
                } else if (message._2.startsWith("E")) {
                    endTradeCount = 1;
                }
                return new Tuple2<>("totalCount", new Tuple2<>(beginTradeCount, endTradeCount));
            }).reduceByKey((x, y) -> new Tuple2<>(x._1 + y._1, x._2 + y._2))
                    .foreachRDD(rdd -> {
                        rdd.foreachPartition(partition -> {
                            partition.forEachRemaining(record -> {
                                long totalBeginTradeCount = record._2._1;
                                long totalEndTradeCount = record._2._2;
                                long totalTradeCount = totalBeginTradeCount > totalEndTradeCount ? totalBeginTradeCount : totalEndTradeCount;
                                LOGGER.info("totalCount" + ":" + totalTradeCount);
                                String currentTimeMillisStr = TimeUtil.getDMCTimeStamp();
                                DBUtil.insert(SQLUtil.INSERT_VIEW_DATA, new ViewData(1, 1, currentTimeMillisStr, "", String.valueOf(totalTradeCount)));
                            });
                        });
                    });

            // 渠道交易量
            begin.mapToPair(line -> {
                String[] fields = line._2.split(separator);
                return new Tuple2<>(fields[1], 1);
            }).reduceByKey((x, y) -> x + y).foreachRDD(rdd -> {
                rdd.foreachPartition(partition -> {
                    partition.forEachRemaining(record -> {
                        LOGGER.info("systemCount" + record._1 + ":" + String.valueOf(record._2.doubleValue()));
                        String currentTimeMillisStr = TimeUtil.getDMCTimeStamp();
                        DBUtil.insert(SQLUtil.INSERT_VIEW_DATA, new ViewData(1, 4, currentTimeMillisStr, record._1, String.valueOf(record._2)));
                    });
                });
            });

            // 服务交易量
            begin.mapToPair(line -> {
                String[] fields = line._2.split(separator);
                return new Tuple2<>(fields[2], 1);
            }).reduceByKey((x, y) -> x + y).foreachRDD(rdd -> {
                rdd.foreachPartition(partition -> {
                    partition.forEachRemaining(record -> {
                        LOGGER.info("systemCount" + record._1 + ":" + String.valueOf(record._2.doubleValue()));
                        String currentTimeMillisStr = TimeUtil.getDMCTimeStamp();
                        DBUtil.insert(SQLUtil.INSERT_VIEW_DATA, new ViewData(1, 5, currentTimeMillisStr, record._1, String.valueOf(record._2)));
                    });
                });
            });

            // 成功率
            end.mapToPair(line -> {
                        int totalCount = 1;
                        int successCount = 0;
                        String[] fields = line._2.split(separator);
                        if ("0".equals(fields[1])) {
                            successCount = 1;
                        }
                        return new Tuple2<>("successRate", new Tuple2<>(totalCount, successCount));
                    }
            ).reduceByKey((x, y) -> {
                return new Tuple2<>(x._1 + y._1, x._2 + y._2);
            }).foreachRDD(rdd -> {
                rdd.foreachPartition(partitionOfRecords -> {
                    partitionOfRecords.forEachRemaining(
                            record -> {
                                String currentTimeMillisStr = TimeUtil.getDMCTimeStamp();
                                String value = new BigDecimal(record._2._2 / record._2._1).setScale(2, BigDecimal.ROUND_HALF_UP).toString();
                                LOGGER.info("avgSuccessRate" + ":" + value);
                                DBUtil.insert(SQLUtil.INSERT_VIEW_DATA, new ViewData(1, 2, currentTimeMillisStr, "", value));
                            }
                    );
                });
            });

            // 响应时间
            end.map(message -> {
                String responseTime = null;
                String[] fields = message._2.split(separator);
                if (DataValidate.isDouble(fields[2])) {
                    responseTime = fields[2];
                }
                return responseTime;
            }).filter(line -> !(line == null))
                    .mapToPair(line -> new Tuple2<>("responseTime", new Tuple2<>(1, Double.valueOf(line))))
                    .reduceByKey((x, y) -> new Tuple2<>(x._1 + y._1, x._2 + y._2))
                    .foreachRDD(rdd -> {
                        rdd.foreachPartition(partitionOfRecords -> {
                            partitionOfRecords.forEachRemaining(
                                    record -> {
                                        String currentTimeMillisStr = TimeUtil.getDMCTimeStamp();
                                        String value = new BigDecimal(record._2._2 / record._2._1).setScale(2, BigDecimal.ROUND_HALF_UP).toString();
                                        LOGGER.info("avgResponseTime" + ":" + value);
                                        DBUtil.insert(SQLUtil.INSERT_VIEW_DATA, new ViewData(1, 3, currentTimeMillisStr, "", value));
                                    }
                            );
                        });
                    });

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
