package com.hazelcast.jet.config;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class XmlYamlJetConfigBuilderEqualsTest {

    @Test
    public void testDefaultConfig() {
        //Given
        JetConfig xmlConfig = new ClasspathXmlJetConfig("hazelcast-jet-default.xml");
        JetConfig yamlConfig = new ClasspathYamlJetConfig("hazelcast-jet-default.yaml");

        //When
        String xmlConfigFromXml = JetConfigXmlGenerator.generate(xmlConfig);
        String xmlConfigFromYaml = JetConfigXmlGenerator.generate(yamlConfig);

        //Then
        assertEquals(xmlConfigFromXml, xmlConfigFromYaml);
    }

    @Test
    public void testFullConfig() {
        //Given
        JetConfig xmlConfig = new ClasspathXmlJetConfig("hazelcast-jet-full-example.xml");
        JetConfig yamlConfig = new ClasspathYamlJetConfig("hazelcast-jet-full-example.yaml");

        //When
        String xmlConfigFromXml = JetConfigXmlGenerator.generate(xmlConfig);
        String xmlConfigFromYaml = JetConfigXmlGenerator.generate(yamlConfig);

        //Then
        assertEquals(xmlConfigFromXml, xmlConfigFromYaml);
    }
}
