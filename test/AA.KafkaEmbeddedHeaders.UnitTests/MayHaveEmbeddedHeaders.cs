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

using Xunit;

namespace AA.KafkaEmbeddedHeaders.UnitTests
{
	public class MayHaveEmbeddedHeaders
	{
		[Fact]
		public void ShortMessage_False()
		{
			Assert.False(EmbeddedHeaderUtils.MayHaveEmbeddedHeaders(new byte[] { 0xff, 0, 0, 0, 0, 0, 0, 0 }));
		}

		[Fact]
		public void NotMagical_False()
		{
			Assert.False(EmbeddedHeaderUtils.MayHaveEmbeddedHeaders(new byte[] { 0x0, 0, 0, 0, 0, 0, 0, 0, 0 }));
		}

		[Fact]
		public void LongAndMagical_True()
		{
			Assert.True(EmbeddedHeaderUtils.MayHaveEmbeddedHeaders(new byte[] { 0xff, 0, 0, 0, 0, 0, 0, 0, 0 }));
		}
	}
}
