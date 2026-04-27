namespace Orleans.EventSourcing.Kurrent.Storage;

/// <summary>
///     Exception for throwing from Kurrent log consistent storage.
/// </summary>
[GenerateSerializer]
[Alias("Orleans.EventSourcing.Kurrent.Storage.KurrentStorageException")]
public sealed class KurrentStorageException : Exception
{
    /// <summary>
    ///     Initializes a new instance of <see cref="KurrentStorageException" />.
    /// </summary>
    public KurrentStorageException()
    {
    }

    /// <summary>
    ///     Initializes a new instance of <see cref="KurrentStorageException" />.
    /// </summary>
    /// <param name="message">The error message that explains the reason for the exception.</param>
    public KurrentStorageException(string message)
        : base(message)
    {
    }

    /// <summary>
    ///     Initializes a new instance of <see cref="KurrentStorageException" />.
    /// </summary>
    /// <param name="message">The error message that explains the reason for the exception.</param>
    /// <param name="inner">The exception that is the cause of the current exception, or a null reference (Nothing in Visual Basic) if no inner exception is specified.</param>
    public KurrentStorageException(string message, Exception inner)
        : base(message, inner)
    {
    }
}
