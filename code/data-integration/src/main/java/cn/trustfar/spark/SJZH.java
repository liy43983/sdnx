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
import java.util.HashMap;
import java.util.Map;

/**
 * 核心业务
 */
public class SJZH {
    private static final Logger LOGGER = LoggerFactory.getLogger(SJZH.class);

    public static void submit(JavaStreamingContext jssc, Map<String, String> kafkaParams) throws InterruptedException {

        final String separator = ",";

        try {

            Map<String, Integer> topicsAndPartitions = new HashMap<>();
            topicsAndPartitions.put("MQ_SJZH_LJ_MONITOR", 8);
            Map<String, Integer> topicsAndPartitions2 = new HashMap<>();
            topicsAndPartitions2.put("MQ_SJZH_PL_MONITOR", 8);

            JavaPairReceiverInputDStream<String, String> lj = KafkaUtils.createStream(
                    jssc, String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topicsAndPartitions, StorageLevel.MEMORY_AND_DISK_SER());
            JavaPairReceiverInputDStream<String, String> pl = KafkaUtils.createStream(
                    jssc, String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topicsAndPartitions2, StorageLevel.MEMORY_AND_DISK_SER());


            // 联机总交易量
            lj.count().foreachRDD(rdd -> {
                rdd.foreachPartition(partition -> {
                    partition.forEachRemaining(record -> {
                        int totalCount = record.intValue();
                        LOGGER.info("totalCount" + ":" + totalCount);
                        String currentTimeMillisStr = TimeUtil.getDMCTimeStamp();
                        DBUtil.insert(SQLUtil.INSERT_VIEW_DATA, new ViewData(4, 1, currentTimeMillisStr, "", String.valueOf(totalCount)));
                    });
                });
            });

            // 渠道交易量
            lj.mapToPair(line -> {
                String[] fields = line._2.split(separator);
                return new Tuple2<>(fields[1], 1);
            }).reduceByKey((x, y) -> x + y).foreachRDD(rdd -> {
                rdd.foreachPartition(partition -> {
                    partition.forEachRemaining(record -> {
                        LOGGER.info("channelCount" + record._1 + ":" + String.valueOf(record._2.doubleValue()));
                        String currentTimeMillisStr = TimeUtil.getDMCTimeStamp();
                        DBUtil.insert(SQLUtil.INSERT_VIEW_DATA, new ViewData(4, 4, currentTimeMillisStr, record._1, String.valueOf(record._2)));
                    });
                });
            });

            // 交易类型交易量
            lj.mapToPair(line -> {
                String[] fields = line._2.split(separator);
                return new Tuple2<>(fields[2], 1);
            }).reduceByKey((x, y) -> x + y)
                    .foreachRDD(rdd -> {
                        rdd.foreachPartition(partition -> {
                            partition.forEachRemaining(record -> {
                                LOGGER.info("tradeTypeCount" + record._1 + ":" + String.valueOf(record._2.doubleValue()));
                                String currentTimeMillisStr = TimeUtil.getDMCTimeStamp();
                                DBUtil.insert(SQLUtil.INSERT_VIEW_DATA, new ViewData(4, 5, currentTimeMillisStr, record._1, String.valueOf(record._2)));
                            });
                        });
                    });

            // 成功率
            lj.mapToPair(line -> {
                String[] fields = line._2.split(separator);
                int failTradeCount = 0;
                if (fields[5] != null && fields[5].contains("ERROR")) {

                    failTradeCount = 1;
                }
                return new Tuple2<>("successRate", new Tuple2<>(1, failTradeCount));
            }).reduceByKey((x, y) -> new Tuple2<>(x._1 + y._1, x._2 + y._2))
                    .foreachRDD(rdd -> {
                        rdd.foreachPartition(partition -> {
                            partition.forEachRemaining(record -> {
                                String value = new BigDecimal(1 - (record._2._2 / record._2._1)).setScale(2, BigDecimal.ROUND_HALF_UP).toString();
                                LOGGER.error("successRate" + ":" + value);
                                String currentTimeMillisStr = TimeUtil.getDMCTimeStamp();
                                DBUtil.insert(SQLUtil.INSERT_VIEW_DATA, new ViewData(4, 2, currentTimeMillisStr, record._1, value));
                            });
                        });
                    });

            // 响应时间
            lj.mapToPair(line -> {
                String[] fields = line._2.split(separator);
                long startTime = DataValidate.isInteger(fields[3]) ? Long.valueOf(fields[3]) : 0;
                long endTime = DataValidate.isInteger(fields[4]) ? Long.valueOf(fields[4]) : 0;
                long responseTime = endTime - startTime;
                if (startTime == 0 || endTime == 0) {
                    responseTime = 0;
                }
                return new Tuple2<>("responseTime", new Tuple2<>(1, responseTime));
            }).filter(tuple -> tuple._2._2 != 0)
                    .reduceByKey((x, y) -> new Tuple2<>(x._1 + y._1, x._2 + y._2))
                    .foreachRDD(rdd -> {
                        rdd.foreachPartition(partitionOfRecords -> {
                            partitionOfRecords.forEachRemaining(
                                    record -> {
                                        String currentTimeMillisStr = TimeUtil.getDMCTimeStamp();
                                        String value = new BigDecimal(record._2._2 / record._2._1).setScale(2, BigDecimal.ROUND_HALF_UP).toString();
                                        LOGGER.info("avgResponseTime" + ":" + value);
                                        DBUtil.insert(SQLUtil.INSERT_VIEW_DATA, new ViewData(4, 2, currentTimeMillisStr, "", value));
                                    }
                            );
                        });
                    });
            pl.mapToPair(message -> {
                String[] fields = message._2.split(separator);
                int result = DataValidate.isInteger(fields[4]) ? Integer.valueOf(fields[4]) : -1;
                if (result == 1) {
                    return new Tuple2<>("executionFailed", 1);
                } else if (result == 2) {
                    return new Tuple2<>("executionSucceed", 1);
                } else {
                    return new Tuple2<>("noExecutionStatus", 0);
                }
            }).filter(tuple -> tuple._2 != 0)
                    .reduceByKey((x, y) -> x + y)
                    .foreachRDD(rdd -> {
                        rdd.foreachPartition(partition -> {
                            partition.forEachRemaining(record -> {
                                LOGGER.info(record._1 + ":" + record._2);
                                String currentTimeMillisStr = TimeUtil.getDMCTimeStamp();
                                int indexId = -1;
                                if ("executionFailed".equals(record._1)) {
                                    indexId = 9;
                                } else if ("executionSucceed".equals(record._1)) {
                                    indexId = 10;
                                } else {
                                    return;
                                }
                                DBUtil.insert(SQLUtil.INSERT_VIEW_DATA, new ViewData(4, indexId, currentTimeMillisStr, record._1, String.valueOf(record._2)));
                            });
                        });
                    });

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
