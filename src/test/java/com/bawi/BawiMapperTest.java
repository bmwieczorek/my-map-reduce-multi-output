package com.bawi;

import com.bawi.mrunit.MultipleOutputsMapDriver;
import com.bawi.mrunit.parquet.ParquetSerialization;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.serializer.JavaSerialization;
import org.apache.hadoop.io.serializer.WritableSerialization;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mrunit.internal.output.MockMultipleOutputs;
import org.apache.hadoop.mrunit.types.Pair;
import org.apache.parquet.example.data.Group;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.List;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"javax.xml.*", "org.xml.*", "org.w3c.*", "junit"})
@PrepareForTest({MultipleOutputs.class, BawiMapper.class})
public class BawiMapperTest {
    private MultipleOutputsMapDriver<LongWritable, Text, Void, Group> mapDriver;

    @Before
    public void setUp() {
        // given
        mapDriver = MultipleOutputsMapDriver
                .newMultipleOutputsMapDriver(new BawiMapper());

        mapDriver.getConfiguration().setStrings("io.serializations",
                ParquetSerialization.class.getName(), JavaSerialization.class.getName(),
                WritableSerialization.class.getName());
    }

    @Test
    public void should_return_OTA_request_items_test() throws Exception {

        // when
        mapDriver.withInput(new LongWritable(0), new Text("<FWD >null,ab</FWD>"));
        mapDriver.withInput(new LongWritable(1), new Text("<FWD >11,cd</FWD>"));
        mapDriver.withInput(new LongWritable(3), new Text("<FWD >22,</FWD>"));
        mapDriver.withInput(new LongWritable(4), new Text("<FWD >33, </FWD>"));
        mapDriver.withInput(new LongWritable(5), new Text("<FWD >44,null</FWD>"));
        mapDriver.run();

        // then
        MockMultipleOutputs<Void, Group> mos = mapDriver.getMOS();
        List<Pair<Void, Group>> bawiList = mos.getMultipleOutputs("bawi");
        Group bawiGroup = bawiList.get(0).getSecond();
    }

    }
