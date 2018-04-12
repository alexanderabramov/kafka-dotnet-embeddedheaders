// Copyright 2018 the original author or authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Refer to LICENSE for more information.


using System;
using System.Collections.Generic;
using System.Text;
using Xunit;

namespace AA.KafkaEmbeddedHeaders.UnitTests
{
	public class EmbedAndExtractHeaders
	{
		[Fact]
		public void TestHeaderEmbedding()
		{
			byte[] typeHeaderName = Encoding.UTF8.GetBytes("type"); // 4 bytes
			byte[] typeHeaderValue = Encoding.UTF8.GetBytes("GreetingsMessage"); // 16 bytes
			byte[] idHeaderName = Encoding.UTF8.GetBytes("id"); // 2 bytes
			byte[] idHeaderValue = Encoding.UTF8.GetBytes("{1234-5678}"); // 11 bytes
			byte[] body = Encoding.UTF8.GetBytes("Hello Kafka-さん"); // 12 + 2*3 = 18 bytes
			var typeHeader = new Header(typeHeaderName, typeHeaderValue);
			var idHeader = new Header(idHeaderName, idHeaderValue);
			var originalHeaders = new List<Header> { typeHeader, idHeader };

			byte[] payload = EmbeddedHeaderUtils.EmbedHeaders(originalHeaders, body);
			Assert.Equal(0xff, payload[0]);
			Assert.Equal(2, payload[1]);
			Assert.Equal(4, payload[2]);
			Assert.Equal(typeHeaderName, Slice(payload, 3, 4));
			Assert.Equal(new byte[] { 0, 0, 0, 16 }, Slice(payload, 7, 4));
			Assert.Equal(typeHeaderValue, Slice(payload, 11, 16));
			Assert.Equal(2, payload[27]);
			Assert.Equal(idHeaderName, Slice(payload, 28, 2));
			Assert.Equal(new byte[] { 0, 0, 0, 11 }, Slice(payload, 30, 4));
			Assert.Equal(idHeaderValue, Slice(payload, 34, 11));
			Assert.Equal(body, Slice(payload, 45, 18));

			(var extractedHeaders, var extractedBody) = EmbeddedHeaderUtils.ExtractHeaders(payload);
			Assert.Equal(2, extractedHeaders.Count);
			Assert.Equal(typeHeaderName, extractedHeaders[0].Name);
			Assert.Equal(typeHeaderValue, extractedHeaders[0].Value);
			Assert.Equal(idHeaderName, extractedHeaders[1].Name);
			Assert.Equal(idHeaderValue, extractedHeaders[1].Value);
			Assert.Equal(body, extractedBody);
		}


		static byte[] Slice(byte[] source, int from, int length)
		{
			byte[] slice = new byte[length];
			Array.Copy(source, from, slice, 0, length);
			return slice;
		}
	}
}
