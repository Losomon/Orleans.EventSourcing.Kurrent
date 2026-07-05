namespace Orleans.EventSourcing.Kurrent.Tests.Grains;

[Alias("Orleans.EventSourcing.Kurrent.Tests.Grains.IUserGrain")]
interface IUserGrain : IGrainWithGuidKey
{
    [Alias("UpdateCreditRating")]
    Task UpdateCreditRating(byte newRating);
}

public class UserState
{
    byte currentCreditRating;
    public void Apply(UserEvent.UserCreditRatingChanged userCreditRatingChanged) => currentCreditRating = userCreditRatingChanged.CreditRating;
}

public abstract record UserEvent
{
    public sealed record UserCreditRatingChanged(byte CreditRating) : UserEvent;
}
internal class UserGrain : JournaledGrain<UserState, UserEvent>, IUserGrain
{
    public async Task UpdateCreditRating(byte newRating)
    {
        this.RaiseEvent(new UserEvent.UserCreditRatingChanged(newRating));
        await ConfirmEvents();
    }
}
