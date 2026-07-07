using Microsoft.Extensions.DependencyInjection;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Orleans.EventSourcing.Kurrent.Clustering;

internal class KurrentMembershipEventStorageFactory
{
    internal static KurrentMembershipEventStorage Create(IServiceProvider sp)
    {
      
        var jsonSerializerOptions = new JsonSerializerOptions
        {
            Converters = { 
                // SiloAddress is used both as a value and as a dictionary key in the persisted membership snapshot,
                // so register a converter that can serialize it in either position.
                new SiloAddressJsonConverter(), 

                // SiloStatus is easier to debug as a string
                new JsonStringEnumConverter<SiloStatus>() }
        };

        return ActivatorUtilities.CreateInstance<KurrentMembershipEventStorage>(sp, jsonSerializerOptions);
    }
}
