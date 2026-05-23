namespace Orleans.EventSourcing.Kurrent.Tests.Grains;

[Alias("Orleans.EventSourcing.Kurrent.Tests.Grains.IStateGrain")]
interface IStateGrain : IGrainWithGuidKey
{
    [Alias("GetValue")]
    ValueTask<int> GetValue();
    [Alias("GetEtag")]
    ValueTask<string> GetEtag();
    [Alias("SetValue")]
    Task SetValue(int value);
    [Alias("ClearValue")]
    Task ClearValue();
    [Alias("RecordExists")]
    ValueTask<bool> RecordExists();
}

record GrainState
{
    public int Value { get; set; } = 42;
}

internal class StateGrain([PersistentState("test")] IPersistentState<GrainState> state) : Grain, IStateGrain
{
    public async Task ClearValue() => await state.ClearStateAsync();

#pragma warning disable CS8613 // Nullability of reference types in return type doesn't match implicitly implemented member.
    public ValueTask<string?> GetEtag()
#pragma warning restore CS8613 // Nullability of reference types in return type doesn't match implicitly implemented member.
     => ValueTask.FromResult(state.Etag);

    public ValueTask<int> GetValue() => ValueTask.FromResult(state.State.Value);

    public ValueTask<bool> RecordExists() => ValueTask.FromResult(state.RecordExists);

    public async Task SetValue(int value)
    {
        state.State.Value = value;
        await state.WriteStateAsync();
    }
}
