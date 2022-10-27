package com.bawi;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;

public class XmlInputStream extends InputStream {

	byte[] xmlStartTag;

	byte[] xmlEndTag;

	private InputStream is;

	private static int MAX_BYTES_READ = 1024 * 1024;

	public XmlInputStream(InputStream is, String xmlStartTag, String xmlEndTag) {
		this.is = is;
		this.xmlStartTag = xmlStartTag.getBytes(StandardCharsets.UTF_8);
		this.xmlEndTag = xmlEndTag.getBytes(StandardCharsets.UTF_8);
	}

	@Override
	public int read() throws IOException {
		return is.read();
	}

	public String readFwdAsString() throws IOException {

		// StringBuilder outputXml = new StringBuilder();
		ByteArrayOutputStream outputXml = new ByteArrayOutputStream(1024 * 128);

		if (readUntilMatch(xmlStartTag, outputXml, true)) {
			outputXml.write(xmlStartTag);
			if (readUntilMatch(xmlEndTag, outputXml, false)) {
				return new String(outputXml.toByteArray(), StandardCharsets.UTF_8);
			}
		}

		return null;
	}

	private boolean readUntilMatch(byte[] needle, ByteArrayOutputStream outputXml, boolean skipData) throws IOException {
		int i = 0;
		int pos = -1;
		while (true) {
			pos++;
			int b = is.read();
			// end of file:
			if (b == -1) {
				return false;
			}

			// skip or save to buffer:
			if (!skipData) {
				outputXml.write(b);
			}

			// check if we're matching:
			if (b == needle[i]) {
				i++;
				if (i >= needle.length) {
					return true;
				}
			} else {
				i = 0;
			}

			// TODO: verify if condition: pos >= MAX_BYTES_READ is necessary
			// see if we've passed the stop point:
			if (skipData && i == 0 && pos >= MAX_BYTES_READ) {
				return false;
			}
		}
	}

}
