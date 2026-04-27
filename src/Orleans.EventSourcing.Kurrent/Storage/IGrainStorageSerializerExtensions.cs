using System.Collections.Concurrent;
using System.Linq.Expressions;

using Orleans.Storage;

namespace Orleans.EventSourcing.Kurrent.Storage
{

    // This adds non-generic overloads to IGrainStorageSerializer Serialize() and Deserialize()
    internal static class IGrainStorageSerializerExtensions
    {
        private static readonly ConcurrentDictionary<Type, Func<BinaryData, object>> _deserializerActivatorCache = new();
        private static readonly ConcurrentDictionary<Type, Func<object, BinaryData>> _serializerActivatorCache = new();

        public static BinaryData Serialize(this IGrainStorageSerializer storageSerializer, Type type, object instance)
        {
            var activator = _serializerActivatorCache.GetOrAdd(type,
                                                                     (instance) =>
                                                                     {
                                                                         var method = typeof(IGrainStorageSerializer).GetMethod(nameof(IGrainStorageSerializer.Serialize))!
                                                                                                                     .MakeGenericMethod(type);
                                                                         var valueParameter = Expression.Parameter(typeof(object));
                                                                         var typedParameter = Expression.Convert(valueParameter, type);
                                                                         var serializerInstanceExpr = Expression.Constant(storageSerializer, typeof(IGrainStorageSerializer));
                                                                         var factoryMethod = Expression.Call(serializerInstanceExpr, method, typedParameter);
                                                                         var lambdaExpression = Expression.Lambda<Func<object, BinaryData>>(factoryMethod, valueParameter);
                                                                         return lambdaExpression.Compile();

                                                                     }
                                                              );
            return activator(instance);
        }

        public static object Deserialize(this IGrainStorageSerializer storageSerializer, Type type, BinaryData binaryData)
        {
            var activator = _deserializerActivatorCache.GetOrAdd(type,
                                                                     (keyType) =>
                                                                     {
                                                                         var method = typeof(IGrainStorageSerializer).GetMethod(nameof(IGrainStorageSerializer.Deserialize))!
                                                                                                                     .MakeGenericMethod(type);
                                                                         var valueParameter = Expression.Parameter(typeof(BinaryData));
                                                                         var serializerInstanceExpr = Expression.Constant(storageSerializer, typeof(IGrainStorageSerializer));
                                                                         var factoryMethod = Expression.Call(serializerInstanceExpr, method, valueParameter);
                                                                         var convertedResultExpr = Expression.Convert(factoryMethod, typeof(object));
                                                                         var lambdaExpression = Expression.Lambda<Func<BinaryData, object>>(convertedResultExpr, valueParameter);
                                                                         return lambdaExpression.Compile();
                                                                     }
                                                                 );

            return activator(binaryData);
        }
    }
}
