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
import java.util.concurrent.ConcurrentHashMap;

/**
 * 服务总线
 */
public class FWZX {
    private static final Logger LOGGER = LoggerFactory.getLogger(FWZX.class);

    public static void submit(JavaStreamingContext jssc,Map<String, String> kafkaParams) throws InterruptedException {

        String sysFirst = "RECORD_ID|PROCESS_DATE|SYSID|REQ_APPS_FLOW_TPS|PROV_APPS_FLOW_TPS|RATE|RPS_MAX|RPS_AVG|RPS_MIN|REQ_APPS_FLOW_TPS_SUM|PROV_APPS_FLOW_TPS_SUM";
        String svrFirst = "RECORD_ID|PROCESS_DATE|SVR_TYPE|TPS|RATE|RPS_MAX|RPS_AVG|RPS_MIN|TPS_SUM";
        final String zero = "0";

        try {

            Map<String, Integer> topicsAndPartitions = new HashMap<>();
            topicsAndPartitions.put("MQ_FWZX_SYSTEM_MONITOR", 3);
            Map<String, Integer> topicsAndPartitions2 = new HashMap<>();
            topicsAndPartitions2.put("MQ_FWZX_SERVICE_MONITOR", 3);

            JavaPairReceiverInputDStream<String, String> sys = KafkaUtils.createStream(
                    jssc, String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topicsAndPartitions, StorageLevel.MEMORY_AND_DISK_SER());
            JavaPairReceiverInputDStream<String, String> svr = KafkaUtils.createStream(
                    jssc, String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topicsAndPartitions2, StorageLevel.MEMORY_AND_DISK_SER());

            JavaPairDStream<String, String> sysFiltered = sys.filter(line -> {
                // (message_key,message_value)
                return !sysFirst.equals(line._2);
            });
            JavaPairDStream<String, String> svrFiltered = svr.filter(line -> {
                // (message_key,message_value)
                return !svrFirst.equals(line._2);
            });
            // 总交易量
            sysFiltered.union(svrFiltered).mapToPair(line -> {
                return new Tuple2<>("totalCount", 1);
            }).reduceByKey((x, y) -> x + y)
                    .foreachRDD(rdd -> {
                        rdd.foreachPartition(partition -> {
                            partition.forEachRemaining(record -> {
                                LOGGER.info("totalCount" + ":" + String.valueOf(record._2));
                                String currentTimeMillisStr = TimeUtil.getDMCTimeStamp();
                                DBUtil.insert(SQLUtil.INSERT_VIEW_DATA, new ViewData(2, 1, currentTimeMillisStr, "", String.valueOf(record._2)));
                            });
                        });
                    });

            // 系统交易量
            sysFiltered.mapToPair(line -> {
                String[] fields = line._2.split("\\|");
                if (DataValidate.isDouble(fields[5])) {
                    return new Tuple2<>(fields[2], Double.valueOf(fields[3]));
                } else {
                    return new Tuple2<>(fields[2], Double.valueOf(zero));
                }
            }).reduceByKey((x,y) -> x + y).foreachRDD(rdd -> {
                rdd.foreachPartition(partition -> {
                    partition.forEachRemaining(record -> {
                        LOGGER.info("systemCount" + record._1 + ":" + String.valueOf(record._2.doubleValue()));
                        String currentTimeMillisStr = TimeUtil.getDMCTimeStamp();
                        DBUtil.insert(SQLUtil.INSERT_VIEW_DATA, new ViewData(2, 6, currentTimeMillisStr, record._1, String.valueOf(record._2)));
                    });
                });
            });

            // 服务交易量
            svrFiltered.mapToPair(line -> {
                String[] fields = line._2.split("\\|");
                if (DataValidate.isDouble(fields[5])) {
                    return new Tuple2<>(fields[2], Double.valueOf(fields[8]));
                } else {
                    return new Tuple2<>(fields[2], Double.valueOf(zero));
                }
            }).reduceByKey((x,y) -> x + y).foreachRDD(rdd -> {
                rdd.foreachPartition(partition -> {
                    partition.forEachRemaining(record -> {
                        LOGGER.info("serviceCount-" + record._1 + ":" + String.valueOf(record._2.doubleValue()));
                        String currentTimeMillisStr = TimeUtil.getDMCTimeStamp();
                        DBUtil.insert(SQLUtil.INSERT_VIEW_DATA, new ViewData(2, 7, currentTimeMillisStr, record._1, String.valueOf(record._2)));
                    });
                });
            });

            // 成功率
            sysFiltered
                    .union(svrFiltered)
                    .mapToPair(line -> {
                                String successRate = null;
                                String[] fields = line._2.split("\\|");
                                if (fields.length == 11 && DataValidate.isDouble(fields[5])) {
                                    successRate = fields[5];
                                } else if (fields.length == 9 && DataValidate.isDouble(fields[4])) {
                                    successRate = fields[4];
                                } else {
                                    successRate = zero;
                                }
                                return new Tuple2<>("totalSuccessRate", new Tuple2<>(Double.valueOf(successRate), 1));
                            }
                    ).reduceByKey((x, y) ->
                    new Tuple2<>(x._1 + y._1, x._2 + y._2)
            ).foreachRDD(rdd -> {
                final long count = rdd.count();
                rdd.foreachPartition(partitionOfRecords -> {
                    partitionOfRecords.forEachRemaining(
                            record -> {
                                String currentTimeMillisStr = TimeUtil.getDMCTimeStamp();
                                String value = new BigDecimal(record._2._1 / record._2._2).setScale(2, BigDecimal.ROUND_HALF_UP).toString();
                                LOGGER.info("avgSuccessRate" + ":" + value);
                                DBUtil.insert(SQLUtil.INSERT_VIEW_DATA, new ViewData(2, 2, currentTimeMillisStr, "", value));
                            }
                    );
                });
            });

            // 响应时间
            sysFiltered.union(svrFiltered)
                    .mapToPair(line -> {
                                String responseTime = null;
                                String[] fields = line._2.split("\\|");
                                if (fields.length == 11 && DataValidate.isDouble(fields[7])) {
                                    responseTime = fields[7];
                                } else if (fields.length == 9 && DataValidate.isDouble(fields[6])) {
                                    responseTime = fields[6];
                                } else {
                                    responseTime = zero;
                                }
                                return new Tuple2<>("totalResponseTime", new Tuple2<>(Double.valueOf(responseTime), 1));
                            }
                    ).reduceByKey((x, y) ->
                    new Tuple2<>(x._1 + y._1, x._2 + y._2)
            ).foreachRDD(rdd -> {
                rdd.foreachPartition(partitionOfRecords -> {
                    partitionOfRecords.forEachRemaining(
                            record -> {
                                String currentTimeMillisStr = TimeUtil.getDMCTimeStamp();
                                String value = new BigDecimal(record._2._1 / record._2._2).setScale(2, BigDecimal.ROUND_HALF_UP).toString();
                                LOGGER.info("avgResponseTime" + ":" + value);
                                DBUtil.insert(SQLUtil.INSERT_VIEW_DATA, new ViewData(2, 3, currentTimeMillisStr, "", value));
                            }
                    );
                });
            });

        } catch (Exception e) {
            LOGGER.error("",e);
        }
    }
}
