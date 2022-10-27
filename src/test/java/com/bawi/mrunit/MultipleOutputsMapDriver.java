package com.bawi.mrunit;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mrunit.internal.mapreduce.MockMapContextWrapper;
import org.apache.hadoop.mrunit.internal.output.MockMultipleOutputs;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Ignore;
import org.powermock.api.mockito.PowerMockito;

import java.io.IOException;
import java.util.List;

@Ignore // ignoring since is not a test class to be run by IDEA (extending external MapDriver with @RunWith), otherwise IDEA fails the test
public class MultipleOutputsMapDriver<K1, V1, K2, V2> extends MapDriver<K1, V1, K2, V2> {

	private MockMapContextWrapper<K1, V1, K2, V2> wrapper;

	public MultipleOutputsMapDriver() {
		super();
	}

	public MultipleOutputsMapDriver(Mapper<K1, V1, K2, V2> mapper) {
		super(mapper);
	}

	public static <K1, V1, K2, V2> MultipleOutputsMapDriver<K1, V1, K2, V2> newMultipleOutputsMapDriver() {
		return new MultipleOutputsMapDriver<>();
	}

	public static <K1, V1, K2, V2> MultipleOutputsMapDriver<K1, V1, K2, V2> newMultipleOutputsMapDriver(
			final Mapper<K1, V1, K2, V2> mapper) {
		return new MultipleOutputsMapDriver<>(mapper);
	}

	@SuppressWarnings("unchecked")
	public MockMultipleOutputs<K2, V2> getMOS() {
		return mos;
	}

	@Override
	public List<Pair<K2, V2>> run() throws IOException {
		try {
			preRunChecks(getMapper());
			initDistributedCache();
			MockMapContextWrapper<K1, V1, K2, V2> wrapper = getContextWrapper();
			mos = new MockVoidMultipleOutputs<K2, V2>(wrapper.getMockContext());
			try {
				PowerMockito.whenNew(MultipleOutputs.class).withArguments(wrapper.getMockContext()).thenReturn(mos);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
			getMapper().run(wrapper.getMockContext());
			return wrapper.getOutputs();
		} catch (final InterruptedException ie) {
			throw new IOException(ie);
		} finally {
			cleanupDistributedCache();
		}
	}

	private MockMapContextWrapper<K1, V1, K2, V2> getContextWrapper() {
		if (wrapper == null) {
			wrapper = new MockMapContextWrapper<>(getConfiguration(), inputs, mockOutputCreator, this);
		}
		return wrapper;
	}
}
