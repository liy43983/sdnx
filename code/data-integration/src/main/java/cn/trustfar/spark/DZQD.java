package cn.trustfar.spark;

import cn.trustfar.db.ConnectionPool;
import cn.trustfar.db.DBUtil;
import cn.trustfar.db.SQLUtil;
import cn.trustfar.db.po.ViewData;
import cn.trustfar.utils.TimeUtil;
import com.alibaba.fastjson.JSONObject;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.StreamSupport;

/**
 * 电子渠道
 */
public class DZQD {
    private static final Logger LOGGER = LoggerFactory.getLogger(DZQD.class);

    public static void submit(JavaStreamingContext jssc, Map<String, String> kafkaParams) throws InterruptedException {

        final String defaultChannelNum = "-99";
        final String responseTimeFieldName = "trdTakeTime";
        final String resFieldName1 = "res";
        final String resFieldName2 = "response";
        final String channelNumFieldName1 = "chnNum";
        final String channelNumFieldName2 = "launch_channel_id";

        try {
            Map<String, Integer> topicsAndPartitions = new HashMap<>();
            topicsAndPartitions.put("MQ_DZQD_MONITOR", 8);
            JavaPairReceiverInputDStream<String, String> dzqd = KafkaUtils.createStream(
                    jssc, String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topicsAndPartitions, StorageLevel.MEMORY_AND_DISK_SER());


            JavaDStream<JSONObject> filter = dzqd.repartition(1000).mapPartitions(
                    partition -> {
                        List<JSONObject> jsonObjects = new ArrayList<>();
                        while (partition.hasNext()) {
                            Tuple2<String, String> message = partition.next();
                            String line = message._2;
                            int index = line.indexOf("{");
                            if (index < 0) {
                                continue;
                            }
                            String jsonStr = line.substring(index);
                            try {
                                JSONObject header = JSONObject.parseObject(jsonStr).getJSONObject("header");
                                JSONObject jsonObject = new JSONObject();

                                if (header.containsKey(channelNumFieldName1)) {
                                    jsonObject.put(channelNumFieldName1, header.getString(channelNumFieldName1));
                                } else if (header.containsKey(channelNumFieldName2)) {
                                    jsonObject.put(channelNumFieldName2, header.getString(channelNumFieldName2));
                                }

                                if (header.containsKey(resFieldName1)) {
                                    JSONObject res = header.getJSONObject(resFieldName1);
                                    jsonObject.put(resFieldName1, res);
                                } else if (header.containsKey(resFieldName2)) {
                                    JSONObject response = header.getJSONObject(resFieldName2);
                                    jsonObject.put(resFieldName2, response);
                                }

                                Double trdTakeTime = jsonObject.getDouble(responseTimeFieldName);
                                jsonObject.put(responseTimeFieldName, trdTakeTime);
                                jsonObjects.add(jsonObject);
                            } catch (Exception e) {
                                LOGGER.warn("json parse error:" + jsonStr);
                            }
                        }
                        return jsonObjects.iterator();
                    }).filter(line -> !(line == null)).persist(StorageLevel.MEMORY_AND_DISK_SER());

            /*JavaDStream<JSONObject> filter = dzqd.repartition(1000)
                    .map(
                            message -> {
                                String line = message._2;
                                int index = line.indexOf("{");
                                if (index < 0) {
                                    return null;
                                }
                                String jsonStr = line.substring(index);
                                try {
                                    JSONObject header = JSONObject.parseObject(jsonStr).getJSONObject("header");
                                    JSONObject jsonObject = new JSONObject();

                                    if (header.containsKey(channelNumFieldName1)) {
                                        jsonObject.put(channelNumFieldName1, header.getString(channelNumFieldName1));
                                    } else if (header.containsKey(channelNumFieldName2)) {
                                        jsonObject.put(channelNumFieldName2, header.getString(channelNumFieldName2));
                                    }

                                    if (header.containsKey(resFieldName1)) {
                                        JSONObject res = header.getJSONObject(resFieldName1);
                                        jsonObject.put(resFieldName1, res);
                                    } else if (header.containsKey(resFieldName2)) {
                                        JSONObject response = header.getJSONObject(resFieldName2);
                                        jsonObject.put(resFieldName2, response);
                                    }

                                    Double trdTakeTime = jsonObject.getDouble(responseTimeFieldName);
                                    jsonObject.put(responseTimeFieldName, trdTakeTime);
                                    return jsonObject;
                                } catch (Exception e) {
                                    LOGGER.warn("json parse error:" + jsonStr);
                                    return null;
                                }
                            }).filter(line -> !(line == null));*/


            //总交易量
            filter.mapToPair(jsonObject -> new Tuple2<>("totalCount", 1))
                    .reduceByKey((x, y) -> x + y)
                    .foreachRDD(rdd -> {
                        rdd.foreachPartition(partition -> {
                            partition.forEachRemaining(record -> {
                                String value = String.valueOf(record._2);
                                LOGGER.error("totalCount" + ":" + value);
                                String currentTimeMillisStr = TimeUtil.getDMCTimeStamp();
                                DBUtil.insert(SQLUtil.INSERT_VIEW_DATA, new ViewData(3, 1, currentTimeMillisStr, "totalCount", value));
                            });
                        });
                    });


            //渠道交易量
            filter.map(line -> {
                String chnNum = null;
                if (line.containsKey("chnNum")) {
                    chnNum = line.getString("chnNum");
                } else if (line.containsKey("launch_channel_id")) {
                    chnNum = line.getString("launch_channel_id");
                } else {
                    chnNum = defaultChannelNum;
                }
                return chnNum;
            }).filter(field -> !defaultChannelNum.equals(field))
                    .mapToPair(field -> new Tuple2<>(field, 1))
                    .reduceByKey((x, y) -> x + y)
                    .foreachRDD(rdd -> {
                        rdd.foreachPartition(partition -> {
                            partition.forEachRemaining(record -> {
                                String value = String.valueOf(record._2.longValue());
                                LOGGER.error("channelTradeCount" + record._1 + ":" + value);
                                String currentTimeMillisStr = TimeUtil.getDMCTimeStamp();
                                DBUtil.insert(SQLUtil.INSERT_VIEW_DATA, new ViewData(3, 8, currentTimeMillisStr, record._1, value));
                            });
                        });
                    });

            //交易成功率
            filter.mapToPair(line -> {
                int totalCount = 1;
                int successCount = 1;
                if (line.containsKey("res")) {
                    JSONObject res = line.getJSONObject("res");
                    if (res.containsKey("resStatus")) {
                        successCount = "fail".equals(res.getString("resStatus")) ? 0 : 1;
                    }
                } else if (line.containsKey("response")) {
                    JSONObject response = line.getJSONObject("response");
                    successCount = "FAIL".equals(response.getString("status")) ? 0 : 1;
                }
                return new Tuple2<>("successRate", new Tuple2<>(totalCount, successCount));
            }).reduceByKey((x, y) -> {
                return new Tuple2<>(x._1 + y._1, x._2 + y._2);
            }).foreachRDD(rdd -> {
                rdd.foreachPartition(partitionOfRecords -> {
                    partitionOfRecords.forEachRemaining(
                            record -> {
                                String currentTimeMillisStr = TimeUtil.getDMCTimeStamp();
                                String value = new BigDecimal(record._2._2 / record._2._1).setScale(2, BigDecimal.ROUND_HALF_UP).toString();
                                LOGGER.info("avgSuccessRate" + ":" + value);
                                DBUtil.insert(SQLUtil.INSERT_VIEW_DATA, new ViewData(3, 2, currentTimeMillisStr, "", value));
                            }
                    );
                });
            });

            //响应时间 纳秒->毫秒
            filter.mapToPair(jsonObject -> new Tuple2<>("", new Tuple2<>(1, jsonObject.getDouble("trdTakeTime") / 1000000)))
                    .reduceByKey((x, y) -> {
                        return new Tuple2<>(x._1 + y._1, x._2 + y._2);
                    }).foreachRDD(rdd -> {
                rdd.foreachPartition(partitionOfRecords -> {
                    partitionOfRecords.forEachRemaining(
                            record -> {
                                String currentTimeMillisStr = TimeUtil.getDMCTimeStamp();
                                String value = new BigDecimal(record._2._2 / record._2._1).setScale(2, BigDecimal.ROUND_HALF_UP).toString();
                                LOGGER.info("avgResponseTime" + ":" + value);
                                DBUtil.insert(SQLUtil.INSERT_VIEW_DATA, new ViewData(3, 3, currentTimeMillisStr, "", value));
                            }
                    );
                });
            });
        } catch (Exception e) {
            LOGGER.error("", e);
        }

    }
}
