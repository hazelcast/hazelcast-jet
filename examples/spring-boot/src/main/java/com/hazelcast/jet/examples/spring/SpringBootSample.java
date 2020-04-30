/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.examples.spring;

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.examples.spring.config.AppConfig;
import com.hazelcast.jet.examples.spring.source.CustomSourceP;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Example of integrating Hazelcast Jet with Spring Boot. {@link AppConfig}
 * class is used as a configuration class and {@link SpringBootSample#submitJob()}
 * is mapped to url 'http://host:port/submitJob' using {@link RestController} and
 * {@link RequestMapping} annotations
 * <p>
 * Job uses a custom source implementation which has {@link com.hazelcast.spring.context.SpringAware}
 * annotation. This enables spring to auto-wire beans to created processors.
 *
 * Submit the job via `http://localhost:8080/submitJob` and observe the output.
 */
@RestController
@SpringBootApplication
public class SpringBootSample {

    @Autowired
    JetInstance instance;

    public static void main(String[] args) {
        SpringApplication.run(AppConfig.class, args);
    }

    @RequestMapping("/submitJob")
    public void submitJob() {
        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(CustomSourceP.customSource())
                .writeTo(Sinks.logger());

        JobConfig jobConfig = new JobConfig()
                .addClass(SpringBootSample.class)
                .addClass(CustomSourceP.class);
        instance.newJob(pipeline, jobConfig).join();
    }

}
