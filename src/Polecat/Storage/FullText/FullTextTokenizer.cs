namespace Polecat.Storage.FullText;

/// <summary>
///     How a full-text index breaks a member's text into the terms it stores and searches (#642).
/// </summary>
/// <remarks>
///     <para>
///         Chosen per index, so an identifier-like member can stay literal while prose stems. Changing an
///         existing index's tokenizer re-tokenizes every document on the next schema migration, because
///         the stored terms are keyed by tokenizer as well as by member.
///     </para>
///     <para>
///         Fisher's equivalent enum names its non-stemming option <c>Unicode</c>. Polecat's is
///         <see cref="Literal" /> on purpose: it lowercases and splits on whitespace and punctuation, but
///         does not fold diacritics the way SQLite's <c>unicode61</c> does, and the name should not claim
///         behaviour the tokenizer does not have. <see cref="Porter" /> stems exactly as Fisher's
///         <c>Porter</c> does, for words of plain ASCII letters.
///     </para>
/// </remarks>
public enum FullTextTokenizer
{
    /// <summary>
    ///     Lowercase, split on whitespace and punctuation, no stemming: terms match as written. The
    ///     default, and the only behaviour before #642.
    /// </summary>
    Literal,

    /// <summary>
    ///     <see cref="Literal" />, then Martin Porter's English stemmer on each term, so
    ///     <c>configuring retries</c> matches <c>configure retry</c>.
    /// </summary>
    /// <remarks>
    ///     Only terms of 3 to 64 lowercase ASCII letters are stemmed; anything else (digits, accented
    ///     letters, very short or very long terms) is stored and searched as written. Prefix searches
    ///     are stemmed too, as SQLite's are, so <c>configuring*</c> matches <c>configure</c>, but a
    ///     partial word whose stem is not a prefix of the stored stem does not: <c>configuri*</c> finds
    ///     nothing.
    /// </remarks>
    Porter
}
