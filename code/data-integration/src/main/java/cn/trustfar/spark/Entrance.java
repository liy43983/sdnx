package cn.trustfar.spark;

import cn.trustfar.db.ConnectionPool;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.*;

/**
 * 提交入口
 */
public class Entrance {
    private static final Logger LOGGER = LoggerFactory.getLogger(Entrance.class);

    public static void main(String[] args) throws InterruptedException {
        // args[] appname/平台系统 zookeeper:ip 间隔时间(分钟)

        ConnectionPool.getInstance();

//        args = new String[]{"fwzx","192.168.188.4:2181"};

        SparkConf sparkConf = new SparkConf().setAppName(args[0]);
        //设置序列化器，并注册要序列化的类型
        sparkConf.set("spark.streaming.backpressure.enabled", "true");
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        Class[] kyroClasses = new Class[]{String.class,Tuple2.class};
        sparkConf.registerKryoClasses(kyroClasses);
//        sparkConf.setMaster("local[6]");

        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(1000 * 60 * Integer.valueOf(args[2])));

        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("zookeeper.connect", args[1]);
        kafkaParams.put("group.id", "sparkSteaming02");
        kafkaParams.put("zookeeper.connection.timeout.ms", "20000");
        kafkaParams.put("zookeeper.sync.time.ms", "200");
        kafkaParams.put("auto.commit.interval.ms", "1000");
        kafkaParams.put("auto.offset.reset", "largest");
        kafkaParams.put("serializer.class", "kafka.serializer.StringEncoder");
        kafkaParams.put("compression.codec", "snappy");

        try {

            if ("fwzx".equals(args[0])) {
                FWZX.submit(jssc, kafkaParams);
            } else if ("dzqd".equals(args[0])) {
                DZQD.submit(jssc, kafkaParams);
            } else if ("hxyw".equals(args[0])) {
                HXYW.submit(jssc, kafkaParams);
            } else if ("aqrz".equals(args[0])) {
                AQRZ.submit(jssc, kafkaParams);
            } else if ("sjzh".equals(args[0])) {
                SJZH.submit(jssc, kafkaParams);
            } else if ("bbfw".equals(args[0])) {
                BBFW.submit(jssc, kafkaParams);
            } else if ("xxjh".equals(args[0])) {
                XXJH.submit(jssc, kafkaParams);
            } else if ("wjcs".equals(args[0])) {
                WJCS.submit(jssc, kafkaParams);
            } else if ("zhtt".equals(args[0])) {
                ZHTT.submit(jssc, kafkaParams);
            }

        } catch (Exception e) {
            LOGGER.error("", e);
        }
        jssc.start(); // Start the computation
        jssc.awaitTermination(); // Wait for the computation to terminate
    }
}
