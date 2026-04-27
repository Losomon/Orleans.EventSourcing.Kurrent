using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

using Orleans.Serialization;
using Orleans.Storage;

using Orleans.EventSourcing.Kurrent.Storage;

namespace Orleans.EventSourcing.Kurrent.Tests
{

    record SomeType(int Value) { }

    public class GrainStorageSerializerExtensionTests
    {
        private readonly IGrainStorageSerializer grainStorageSerializer;

        public GrainStorageSerializerExtensionTests()
        {
            var hb = new HostApplicationBuilder();

            hb.Services.AddSingleton<OrleansJsonSerializer>();
            hb.Services.AddOptions<OrleansJsonSerializerOptions>();

            hb.Services.AddSingleton<IGrainStorageSerializer, JsonGrainStorageSerializer>();
            var sp = hb.Build();

            grainStorageSerializer = sp.Services.GetRequiredService<IGrainStorageSerializer>();
        }

        [Fact]
        public void Roundtrip()
        {
            var instance = new SomeType(35353);
            var data = grainStorageSerializer.Serialize(typeof(SomeType), instance);
            var copyOfInstance = grainStorageSerializer.Deserialize(typeof(SomeType), data);

            Assert.Equal(instance, copyOfInstance);
        }
    }
}
