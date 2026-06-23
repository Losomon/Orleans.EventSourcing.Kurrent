using Orleans.EventSourcing.Kurrent.Storage;

namespace Orleans.EventSourcing.Kurrent.Tests
{
    // Mainstream and edge cases for converting grainIds to stream names
    public sealed class StreamNameTests
    {
        readonly KurrentStreamName kurrentStreamName = new();

        [Fact]
        public void GetStreamPrefixInvalidGrainType()
         => Assert.ThrowsAny<ArgumentException>(() => kurrentStreamName.GetStreamPrefix(default));

        [Fact]
        public void GetStreamNameInvalidGrainId()
         => Assert.ThrowsAny<ArgumentException>(() => kurrentStreamName.GetStreamName(default));

        [Fact]
        public void GetStreamNameEmptyIdSpan()
        => Assert.Equal(GrainId.Parse("test/"), kurrentStreamName.GetGrainId(kurrentStreamName.GetStreamName(GrainId.Parse("test/"))));

        [Fact]
        public void GetStreamNameNullStateName()
        => Assert.Throws<ArgumentNullException>(() => kurrentStreamName.GetStreamName(null!, GrainId.Parse("test/")));

        [Fact]
        public void GetStreamNameGrainTypeContainsReservedStreamNamePrefix()
        => Assert.Equal("\\$-",kurrentStreamName.GetStreamName(GrainId.Parse("$/")));

        [Fact]
        public void GetStreamNameEmptyStateName()
        => Assert.Throws<ArgumentException>(() => kurrentStreamName.GetStreamName(string.Empty, GrainId.Parse("test/")));

        [Fact]
        public void GetStreamNameGrainIdKeyReservedCharacter()
        {
            var streamName = kurrentStreamName.GetStreamName(GrainId.Parse("|/|"));
            var output = kurrentStreamName.GetGrainId(streamName);
            Assert.Equal(GrainId.Parse("|/|"), output);
        }

        [Fact]
        public void GetStreamNameReservedCharacter()
        { 
            var streamName = kurrentStreamName.GetStreamName(@"|-\", GrainId.Parse("test/test"));

            Assert.Equal(@"test-test|\|\-\\", streamName);

            var output = kurrentStreamName.GetGrainId(streamName);

            Assert.Equal(GrainId.Parse("test/test"), output);
        }

        [Fact]
        public void GetStreamPrefixReturnsExpectedFormat()
            => Assert.Equal("mytype", kurrentStreamName.GetStreamPrefix(GrainType.Create("mytype")));

        [Fact]
        public void GetStreamNameWithKeyReturnsExpectedFormat()
            => Assert.Equal("test-mykey", kurrentStreamName.GetStreamName(GrainId.Parse("test/mykey")));

        [Fact]
        public void GetStreamNameWithKeyRoundtrip()
        {
            var grainId = GrainId.Parse("test/mykey");
            Assert.Equal(grainId, kurrentStreamName.GetGrainId(kurrentStreamName.GetStreamName(grainId)));
        }

        [Fact]
        public void GetStreamNameWithStateNameReturnsExpectedFormat()
            => Assert.Equal("test-mykey|mystate", kurrentStreamName.GetStreamName("mystate", GrainId.Parse("test/mykey")));

        [Fact]
        public void TryGetGrainIdValidStreamName()
        {
            var result = kurrentStreamName.TryGetGrainId("test-mykey", out var grainId);
            Assert.True(result);
            Assert.Equal(GrainId.Parse("test/mykey"), grainId);
        }

        [Fact]
        public void TryGetGrainIdNullOrWhitespace()
        {
            Assert.False(kurrentStreamName.TryGetGrainId(null!, out _));
            Assert.False(kurrentStreamName.TryGetGrainId(string.Empty, out _));
            Assert.False(kurrentStreamName.TryGetGrainId("   ", out _));
        }

        [Fact]
        public void TryGetGrainIdNoSeparator()
        {
            Assert.True(kurrentStreamName.TryGetGrainId("nostreamkey", out var grainId));
            Assert.Equal("nostreamkey", grainId.Type.ToString());
            Assert.Equal(default, grainId.Key);
        }

        [Fact]
        public void GetGrainIdUnsupportedFormatThrows()
        { 
            var grainId = kurrentStreamName.GetGrainId("nostreamkey");
            Assert.Equal("nostreamkey", grainId.Type.ToString());
            Assert.Equal(default, grainId.Key);
        }
    }
}
