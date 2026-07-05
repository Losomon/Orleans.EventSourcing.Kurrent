using Microsoft.Extensions.DependencyInjection;
using System.Text.Json;

namespace Orleans.EventSourcing.Kurrent.Clustering;

internal class KurrentMembershipEventStorageFactory
{
    internal static KurrentMembershipEventStorage Create(IServiceProvider sp)
    {
        // SiloAddress is used both as a value and as a dictionary key in the persisted membership snapshot,
        // so register a converter that can serialize it in either position.
        var jsonSerializerOptions = new JsonSerializerOptions
        {
            Converters = { new SiloAddressJsonConverter() }
        };

        return ActivatorUtilities.CreateInstance<KurrentMembershipEventStorage>(sp, jsonSerializerOptions);
    }
}
