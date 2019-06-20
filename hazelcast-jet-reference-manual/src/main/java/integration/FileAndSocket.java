package integration;

import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;

import java.nio.charset.StandardCharsets;

public class FileAndSocket {
    static void s1() {
        //tag::s1[]
        Pipeline p = Pipeline.create();
        p.drawFrom(Sources.files("/home/jet/input"))
         .drainTo(Sinks.logger());
        //end::s1[]
    }

    static void s2() {
        //tag::s2[]
        Pipeline p = Pipeline.create();
        p.drawFrom(Sources.fileWatcher("/home/jet/input"))
         .withoutTimestamps()
         .drainTo(Sinks.logger());
        //end::s2[]
    }

    static void s3() {
        //tag::s3[]
        Pipeline p = Pipeline.create();
        p.drawFrom(Sources.list("inputList"))
         .drainTo(Sinks.files("/home/jet/output"));
        //end::s3[]
    }

    static void s4() {
        //tag::s4[]
        Pipeline p = Pipeline.create();
        p.drawFrom(Sources.socket("localhost", 8080, StandardCharsets.UTF_8))
         .withoutTimestamps()
         .drainTo(Sinks.logger());
        //end::s4[]
    }

    static void s5() {
        //tag::s5[]
        Pipeline p = Pipeline.create();
        p.drawFrom(Sources.list("inputList"))
         .drainTo(Sinks.socket("localhost", 8080));
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
