package integration;

import com.hazelcast.jet.hadoop.HdfsSinks;
import com.hazelcast.jet.hadoop.HdfsSources;
import com.hazelcast.jet.kafka.KafkaSinks;
import com.hazelcast.jet.kafka.KafkaSources;
import com.hazelcast.jet.pipeline.Pipeline;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

import static com.hazelcast.jet.Traversers.traverseArray;
import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.function.Functions.wholeItem;

public class HdfsAndKafka {
    static void s1() {
        //tag::s1[]
        JobConf jobConfig = new JobConf();
        jobConfig.setInputFormat(TextInputFormat.class);
        jobConfig.setOutputFormat(TextOutputFormat.class);
        TextInputFormat.addInputPath(jobConfig, new Path("input-path"));
        TextOutputFormat.setOutputPath(jobConfig, new Path("output-path"));
        //end::s1[]
    }

    static void s2() {
        JobConf jobConfig = new JobConf();
        //tag::s2[]
        Pipeline p = Pipeline.create();
        p.drawFrom(HdfsSources.hdfs(jobConfig, (k, v) -> v.toString()))
         .flatMap(line -> traverseArray(line.toLowerCase().split("\\W+"))
                               .filter(w -> !w.isEmpty()))
         .groupingKey(wholeItem())
         .aggregate(counting())
         .drainTo(HdfsSinks.hdfs(jobConfig));
        //end::s2[]
    }

    static void s3() {
        //tag::s3[]
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("key.serializer", StringSerializer.class.getCanonicalName());
        props.setProperty("key.deserializer", StringDeserializer.class.getCanonicalName());
        props.setProperty("value.serializer", IntegerSerializer.class.getCanonicalName());
        props.setProperty("value.deserializer", IntegerDeserializer.class.getCanonicalName());
        props.setProperty("auto.offset.reset", "earliest");

        Pipeline p = Pipeline.create();
        p.drawFrom(KafkaSources.kafka(props, "t1", "t2"))
         .withoutTimestamps()
         .drainTo(KafkaSinks.kafka(props, "t3"));
        //end::s3[]
    }

    static void s4() {
        //tag::s4[]
        //end::s4[]
    }

    static void s5() {
        //tag::s5[]
        //end::s5[]
    }

    static void s6() {
        //tag::s6[]
        //end::s6[]
    }

    static void s7() {
        //tag::s7[]
        //end::s7[]
    }

    static void s8() {
        //tag::s8[]
        //end::s8[]
    }

    static void s9() {
        //tag::s9[]
        //end::s9[]
    }

    static void s10() {
        //tag::s10[]
        //end::s10[]
    }

    static void s11() {
        //tag::s11[]
        //end::s11[]
    }

}
