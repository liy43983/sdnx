package cn.trustfar.spark;

import cn.trustfar.db.DBUtil;
import cn.trustfar.db.SQLUtil;
import cn.trustfar.db.po.ViewData;
import cn.trustfar.utils.DateTimeUtil;
import cn.trustfar.utils.StringUtil;
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
import java.util.HashMap;
import java.util.Map;

/**
 * 信息交互
 */
public class XXJH {
    private static final Logger LOGGER = LoggerFactory.getLogger(XXJH.class);

    public static void submit(JavaStreamingContext jssc, Map<String, String> kafkaParams) throws InterruptedException {

        final String defaultString = "-99";

        try {
            Map<String, Integer> topicsAndPartitions = new HashMap<>();
            topicsAndPartitions.put("MQ_XXJH_MONITOR", 8);
            JavaPairReceiverInputDStream<String, String> xxjh = KafkaUtils.createStream(
                    jssc, String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topicsAndPartitions, StorageLevel.MEMORY_AND_DISK_SER());


            //总交易量
            xxjh.mapToPair(message -> {
                String line = message._2;
                int tradeCount = 0;
                if ((line.contains("<Root>") && line.contains("</Root>")) || (line.contains("<Service>") && line.contains("</Service>"))) {
                    tradeCount = 1;
                }
                return new Tuple2<>("totalCount", tradeCount);
            }).reduceByKey((x, y) -> x + y)
                    .foreachRDD(rdd -> {
                        rdd.foreachPartition(partition -> {
                            partition.forEachRemaining(record -> {
                                String value = String.valueOf(record._2);
                                LOGGER.error("totalCount" + ":" + value);
                                String currentTimeMillisStr = TimeUtil.getDMCTimeStamp();
                                DBUtil.insert(SQLUtil.INSERT_VIEW_DATA, new ViewData(7, 1, currentTimeMillisStr, "totalCount", value));
                            });
                        });
                    });

            //渠道交易量
            xxjh.mapToPair(message -> {
                String line = message._2;
                int tradeCount = 0;
                String channel = defaultString;
                if (line.contains("<Root>") && line.contains("</Root>")) {
                    tradeCount = 1;
                    channel = StringUtil.getFiledValueFromXML(line, "REQ-CHANEL-FLG-LCH");
                } else if (line.contains("<Service>") && line.contains("</Service>")) {
                    tradeCount = 1;
                    channel = StringUtil.getFiledValueFromXML(line, "requester_id");
                }
                return new Tuple2<>(channel, tradeCount);
            }).filter(field -> !defaultString.equals(field._1))
                    .reduceByKey((x, y) -> x + y)
                    .foreachRDD(rdd -> {
                        rdd.foreachPartition(partition -> {
                            partition.forEachRemaining(record -> {
                                String value = String.valueOf(record._2);
                                LOGGER.error("channelTradeCount" + record._1 + ":" + value);
                                String currentTimeMillisStr = TimeUtil.getDMCTimeStamp();
                                DBUtil.insert(SQLUtil.INSERT_VIEW_DATA, new ViewData(7, 4, currentTimeMillisStr, record._1, value));
                            });
                        });
                    });

            //交易类型交易量
            xxjh.mapToPair(message -> {
                String line = message._2;
                int tradeCount = 0;
                String tradeType = defaultString;
                if (line.contains("<Root>") && line.contains("</Root>")) {
                    tradeCount = 1;
                    tradeType = StringUtil.getFiledValueFromXML(line, "REQ-SERVICE-ID");
                } else if (line.contains("<Service>") && line.contains("</Service>")) {
                    tradeCount = 1;
                    tradeType = StringUtil.getFiledValueFromXML(line, "service_id");
                }
                return new Tuple2<>(tradeType, tradeCount);
            }).filter(field -> !defaultString.equals(field._1))
                    .reduceByKey((x, y) -> x + y)
                    .foreachRDD(rdd -> {
                        rdd.foreachPartition(partition -> {
                            partition.forEachRemaining(record -> {
                                String value = String.valueOf(record._2);
                                LOGGER.error("tradeTypeCount" + record._1 + ":" + value);
                                String currentTimeMillisStr = TimeUtil.getDMCTimeStamp();
                                DBUtil.insert(SQLUtil.INSERT_VIEW_DATA, new ViewData(7, 5, currentTimeMillisStr, record._1, value));
                            });
                        });
                    });

            //响应时间
            xxjh.mapToPair(message -> {
                String line = message._2;
                long responseTime = 0;
                String startTimeStr = null;
                String endTimeStr = null;
                int tradeCount = 0;
                if (line.contains("<Root>") && line.contains("</Root>")) {
                    tradeCount = 1;
                    startTimeStr = StringUtil.getFiledValueFromXML(line, "MH-RECV-TIME_1");
                    endTimeStr = StringUtil.getFiledValueFromXML(line, "MH-SEND-TIME_3");
                } else if (line.contains("<Service>") && line.contains("</Service>")) {
                    tradeCount = 1;
                    startTimeStr = StringUtil.getFiledValueFromXML(line, "start_timestamp");
                    endTimeStr = StringUtil.getFiledValueFromXML(line, "process_timestamp");
                }
                DateTime startDateTime = DateTimeUtil.parseDateTime(startTimeStr, null);
                DateTime endDateTime = DateTimeUtil.parseDateTime(endTimeStr, null);
                if (tradeCount == 0 || startDateTime == null || endDateTime == null) {
                    return new Tuple2<>("responseTime", null);
                }
                responseTime = endDateTime.getMillis() - startDateTime.getMillis();
                return new Tuple2<>("responseTime", new Tuple2<>(tradeCount, responseTime));
            }).filter(tuple -> tuple._2 != null)
                    .reduceByKey((x, y) -> new Tuple2<>(x._1 + y._1, x._2 + y._2))
                    .foreachRDD(rdd -> {
                        rdd.foreachPartition(partition -> {
                            partition.forEachRemaining(record -> {
                                String value = new BigDecimal(record._2._2 / record._2._1).setScale(2, BigDecimal.ROUND_HALF_UP).toString();
                                LOGGER.error("responseTime" + ":" + value);
                                String currentTimeMillisStr = TimeUtil.getDMCTimeStamp();
                                DBUtil.insert(SQLUtil.INSERT_VIEW_DATA, new ViewData(7, 3, currentTimeMillisStr, record._1, value));
                            });
                        });
                    });

            //成功率
            xxjh.mapToPair(message -> {
                String line = message._2;
                int successCount = 0;
                int tradeCount = 0;
                if (line.contains("<Service>") && line.contains("</Service>")) {
                    tradeCount = 1;
                    if ("S000A000".equals(StringUtil.getFiledValueFromXML(line, "code"))) {
                        successCount = 1;
                    }
                } else {
                    return new Tuple2<>("successRate", null);
                }
                return new Tuple2<>("successRate", new Tuple2<>(tradeCount, successCount));
            }).filter(tuple2 -> tuple2._2 != null)
                    .reduceByKey((x, y) -> new Tuple2<>(x._1 + y._1, x._2 + y._2))
                    .foreachRDD(rdd -> {
                        rdd.foreachPartition(partition -> {
                            partition.forEachRemaining(record -> {
                                String value = new BigDecimal(record._2._2 / record._2._1).setScale(2, BigDecimal.ROUND_HALF_UP).toString();
                                LOGGER.error("responseTime" + ":" + value);
                                String currentTimeMillisStr = TimeUtil.getDMCTimeStamp();
                                DBUtil.insert(SQLUtil.INSERT_VIEW_DATA, new ViewData(7, 2, currentTimeMillisStr, record._1, value));
                            });
                        });
                    });


        } catch (Exception e) {
            LOGGER.error("", e);
        }

    }
}
