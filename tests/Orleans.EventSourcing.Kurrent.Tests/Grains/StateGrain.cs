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
    public async Task ClearValue()
    {
        await state.ClearStateAsync();
    }

    public ValueTask<string> GetEtag()
    {
        return ValueTask.FromResult(state.Etag);
    }

    public ValueTask<int> GetValue()
    {
        return ValueTask.FromResult(state.State.Value);
    }

    public ValueTask<bool> RecordExists()
    {
        return ValueTask.FromResult(state.RecordExists);
    }

    public async Task SetValue(int value)
    {
        state.State.Value = value;
        await state.WriteStateAsync();
    }
}
