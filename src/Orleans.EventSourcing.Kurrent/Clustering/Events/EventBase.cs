namespace Orleans.EventSourcing.Kurrent.Clustering.Events;

abstract record EventBase
{
    internal abstract MembershipView Apply(MembershipView view);
}
