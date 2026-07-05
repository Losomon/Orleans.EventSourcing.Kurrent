using System.Globalization;

using Microsoft.Extensions.Logging;
using Orleans.EventSourcing.Kurrent.Observability;
using Orleans.Serialization;

namespace Orleans.EventSourcing.Kurrent.LogConsistency;

/// <summary>
///     Functionality for use by _logger view adaptors that run distributed protocols.
///     This class allows access to these services to providers that cannot see runtime-internals.
///     It also stores grain-specific information like the grain reference, and caches
/// </summary>
/// <remarks>
/// </remarks>
/// <param name="grainContext"></param>
/// <param name="loggerFactory"></param>
/// <param name="deepCopier"></param>
/// <param name="siloDetails"></param>
internal sealed class DefaultProtocolServices(
    IGrainContext grainContext,
    ILoggerFactory loggerFactory,
    DeepCopier deepCopier,
    ILocalSiloDetails siloDetails) : ILogConsistencyProtocolServices
{
    private readonly ILogger _logger = loggerFactory.CreateLogger<DefaultProtocolServices>();

    /// <inheritdoc />
    public GrainId GrainId => grainContext.GrainId;

    /// <inheritdoc />
    public string MyClusterId { get; } = siloDetails.ClusterId;

    /// <inheritdoc />
    public T DeepCopy<T>(T value) => deepCopier.Copy(value);

    /// <inheritdoc />
    public void ProtocolError(string msg, bool throwexception)
    {
        if (throwexception)
        {
            _logger.ProtcolFatalError(grainContext.GrainId, msg);
            throw new OrleansException($"{msg} (grain={grainContext.GrainId}, cluster={MyClusterId})");
        }
        else
        {
            _logger.ProtocolError(grainContext.GrainId, msg);
        }
    }

    /// <inheritdoc />
    public void CaughtException(string where, Exception ex) => _logger.CaughtException(grainContext.GrainId, where, ex);

    /// <inheritdoc />
    public void CaughtUserCodeException(string callback, string where, Exception ex) => _logger.UserCodeException(grainContext.GrainId, callback, where, ex);

    /// <inheritdoc />
    public void Log(LogLevel level, string format, params object[] args)
    {
        if (_logger is null || !_logger.IsEnabled(level))
        {
            return;
        }
        var msg = $"{grainContext.GrainId} {string.Format(CultureInfo.InvariantCulture, format, args)}";
        _logger.Log(level, 0, msg, null, (m, _) => $"{m}");
    }
}
