#pragma warning disable IDE0130 // Namespace does not match folder structure:  Ideally this wikll be moved to Orleans.EventSourcing.
namespace Orleans.EventSourcing;
#pragma warning restore IDE0130 // Namespace does not match folder structure

/// <summary>
/// When applied to an event, indicates that the events prior to this do not need to be maintained.
/// 
/// <para>
/// <strong>The storage provider may delete all events written before an event with this attribute.</strong>
/// </para>
/// </summary>
[AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct, AllowMultiple = false, Inherited = true)]
public sealed class DiscardPriorEventsAttribute : Attribute;
