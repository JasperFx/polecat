using Polecat.Internal;
using Weasel.Core;
using Weasel.SqlServer;
using Weasel.SqlServer.Functions;

namespace Polecat.Storage.FullText;

/// <summary>
///     The write-side half of <see cref="FullTextTokenizer.Porter" /> (#642): Porter's stemmer as two
///     T-SQL scalar functions, so the maintenance trigger and the backfill stem exactly as
///     <see cref="PorterStemmer" /> does on the query side.
/// </summary>
/// <remarks>
///     <para>
///         <b>Two Weasel <see cref="Function" /> objects, not raw DDL</b>, per the rule that every schema
///         object goes through Weasel: they appear in a generated script, are compared by a delta against
///         <c>sys.sql_modules</c>, and are refused under <c>AutoCreate.None</c>. One per schema, shared by
///         every Porter index in it.
///     </para>
///     <para>
///         ⚠️ <b>Held to <see cref="PorterStemmer" /> by an oracle, not by reading the two side by side.</b>
///         <c>porter_stemmer_tests</c> runs THIS function inside SQL Server over the same 23,549 SQLite
///         FTS5 <c>porter</c> stems the C# stemmer is pinned to. A divergence between the two is a search
///         that answers empty rather than an error, so the test is the only thing standing between a
///         one-character slip here and a silently broken index.
///     </para>
///     <para>
///         <b>How it is shaped, so the next reader can check it against the C# rather than re-derive it.</b>
///         <c>pc_porter_cv</c> classifies each character as consonant or vowel. A character's class
///         depends only on itself and the one before it, so the pattern of any prefix of a word is the
///         same prefix of the word's pattern — which turns Porter's measure <c>m</c> into "count the
///         <c>vc</c> pairs in <c>LEFT(@p, j)</c>" and <c>*v*</c> into a <c>CHARINDEX</c>. Each step's
///         suffix list is an ordered <c>VALUES</c> table taken <c>TOP 1</c>: the reference switches on the
///         word's last or second-to-last letter first, but a suffix that matches already fixes that letter,
///         so the switch is implied by the match and the global order reproduces each case's order.
///         <c>Ends</c> is STRICT — the suffix must be shorter than the word — exactly as in
///         <see cref="PorterStemmer" />.
///     </para>
/// </remarks>
internal static class PorterStemFunction
{
    internal const string StemFunctionName = "pc_porter_stem";
    internal const string PatternFunctionName = "pc_porter_cv";

    internal static DbObjectName StemIdentifier(string schema) => new SqlServerObjectName(schema, StemFunctionName);

    internal static DbObjectName PatternIdentifier(string schema) =>
        new SqlServerObjectName(schema, PatternFunctionName);

    /// <summary>The schema-qualified call the trigger and backfill render around a token.</summary>
    internal static string Call(string schema, string tokenExpression) =>
        $"{SqlEscaping.QualifiedName(schema, StemFunctionName)}({tokenExpression})";

    /// <summary>Both functions, the pattern helper first because the stemmer calls it.</summary>
    internal static IEnumerable<Function> For(string schema)
    {
        // Plain Weasel Functions since Weasel 9.44.0 (weasel#703/#704): their creation script drops first,
        // so a generated script containing them runs twice, which used to need a local subclass.
        yield return new Function(PatternIdentifier(schema), PatternSql(schema));
        yield return new Function(StemIdentifier(schema), StemSql(schema));
    }

    internal static string PatternSql(string schema) =>
        $"""
         CREATE FUNCTION {SqlEscaping.QualifiedName(schema, PatternFunctionName)}(@w varchar(70))
         RETURNS varchar(70)
         AS
         BEGIN
             -- 'c' or 'v' per character: a/e/i/o/u are vowels, y is a vowel after a consonant.
             DECLARE @p varchar(70) = '', @i int = 1, @n int = LEN(@w), @c char(1);
             WHILE @i <= @n
             BEGIN
                 SET @c = SUBSTRING(@w, @i, 1);
                 SET @p = @p + CASE
                     WHEN @c IN ('a', 'e', 'i', 'o', 'u') THEN 'v'
                     WHEN @c = 'y' THEN CASE WHEN @i = 1 OR SUBSTRING(@p, @i - 1, 1) = 'v' THEN 'c' ELSE 'v' END
                     ELSE 'c' END;
                 SET @i = @i + 1;
             END;
             RETURN @p;
         END
         """;

    internal static string StemSql(string schema)
    {
        var cv = SqlEscaping.QualifiedName(schema, PatternFunctionName);
        return $"""
                CREATE FUNCTION {SqlEscaping.QualifiedName(schema, StemFunctionName)}(@word nvarchar(4000))
                RETURNS nvarchar(4000)
                AS
                BEGIN
                    -- Only 3..64 lowercase ASCII letters are stemmed (PorterStemmer.IsStemmable). The binary
                    -- collation makes [a-z] mean exactly the ASCII range, whatever the database collation.
                    IF @word IS NULL OR LEN(@word) < 3 OR LEN(@word) > 64
                       OR @word COLLATE Latin1_General_BIN2 LIKE '%[^a-z]%'
                        RETURN @word;

                    DECLARE @w varchar(70) = CAST(@word AS varchar(70));
                    DECLARE @p varchar(70), @j int, @m int, @suffix varchar(10), @replacement varchar(10);

                    -- Step 1a: plurals
                    IF RIGHT(@w, 1) = 's'
                    BEGIN
                        IF LEN(@w) > 4 AND RIGHT(@w, 4) = 'sses' SET @w = LEFT(@w, LEN(@w) - 2);
                        ELSE IF LEN(@w) > 3 AND RIGHT(@w, 3) = 'ies' SET @w = LEFT(@w, LEN(@w) - 3) + 'i';
                        ELSE IF SUBSTRING(@w, LEN(@w) - 1, 1) <> 's' SET @w = LEFT(@w, LEN(@w) - 1);
                    END;

                    -- Step 1b: -eed, -ed, -ing
                    SET @p = {cv}(@w);
                    IF LEN(@w) > 3 AND RIGHT(@w, 3) = 'eed'
                    BEGIN
                        SET @j = LEN(@w) - 3;
                        IF (LEN(LEFT(@p, @j)) - LEN(REPLACE(LEFT(@p, @j), 'vc', ''))) / 2 > 0
                            SET @w = LEFT(@w, LEN(@w) - 1);
                    END
                    ELSE
                    BEGIN
                        SET @j = NULL;
                        IF LEN(@w) > 2 AND RIGHT(@w, 2) = 'ed' SET @j = LEN(@w) - 2;
                        ELSE IF LEN(@w) > 3 AND RIGHT(@w, 3) = 'ing' SET @j = LEN(@w) - 3;

                        IF @j IS NOT NULL AND CHARINDEX('v', LEFT(@p, @j)) > 0
                        BEGIN
                            SET @w = LEFT(@w, @j);
                            SET @p = LEFT(@p, @j);
                            IF LEN(@w) > 2 AND RIGHT(@w, 2) IN ('at', 'bl', 'iz')
                                SET @w = @w + 'e';
                            ELSE IF LEN(@w) >= 2 AND RIGHT(@w, 1) = SUBSTRING(@w, LEN(@w) - 1, 1) AND RIGHT(@p, 1) = 'c'
                            BEGIN
                                IF RIGHT(@w, 1) NOT IN ('l', 's', 'z') SET @w = LEFT(@w, LEN(@w) - 1);
                            END
                            ELSE IF (LEN(@p) - LEN(REPLACE(@p, 'vc', ''))) / 2 = 1
                                    AND LEN(@w) >= 3 AND RIGHT(@p, 3) = 'cvc' AND RIGHT(@w, 1) NOT IN ('w', 'x', 'y')
                                SET @w = @w + 'e';
                        END;
                    END;

                    IF LEN(@w) < 2 RETURN CAST(@w AS nvarchar(4000));

                    -- Step 1c: y -> i when the stem has a vowel
                    SET @p = {cv}(@w);
                    IF RIGHT(@w, 1) = 'y' AND CHARINDEX('v', LEFT(@p, LEN(@w) - 1)) > 0
                        SET @w = LEFT(@w, LEN(@w) - 1) + 'i';

                    -- Step 2: double suffixes, replaced when m(stem) > 0
                    SET @p = {cv}(@w);
                    SET @suffix = NULL;
                    SELECT TOP 1 @suffix = s.suffix, @replacement = s.replacement
                    FROM (VALUES
                        (1, 'ational', 'ate'), (2, 'tional', 'tion'),
                        (3, 'enci', 'ence'), (4, 'anci', 'ance'),
                        (5, 'izer', 'ize'),
                        (6, 'bli', 'ble'), (7, 'alli', 'al'), (8, 'entli', 'ent'), (9, 'eli', 'e'), (10, 'ousli', 'ous'),
                        (11, 'ization', 'ize'), (12, 'ation', 'ate'), (13, 'ator', 'ate'),
                        (14, 'alism', 'al'), (15, 'iveness', 'ive'), (16, 'fulness', 'ful'), (17, 'ousness', 'ous'),
                        (18, 'aliti', 'al'), (19, 'iviti', 'ive'), (20, 'biliti', 'ble'),
                        (21, 'logi', 'log')
                    ) s(ord, suffix, replacement)
                    WHERE LEN(s.suffix) < LEN(@w) AND RIGHT(@w, LEN(s.suffix)) = s.suffix
                    ORDER BY s.ord;
                    IF @suffix IS NOT NULL
                    BEGIN
                        SET @j = LEN(@w) - LEN(@suffix);
                        IF (LEN(LEFT(@p, @j)) - LEN(REPLACE(LEFT(@p, @j), 'vc', ''))) / 2 > 0
                            SET @w = LEFT(@w, @j) + @replacement;
                    END;

                    -- Step 3: -ic-, -full, -ness etc., replaced when m(stem) > 0
                    SET @p = {cv}(@w);
                    SET @suffix = NULL;
                    SELECT TOP 1 @suffix = s.suffix, @replacement = s.replacement
                    FROM (VALUES
                        (1, 'icate', 'ic'), (2, 'ative', ''), (3, 'alize', 'al'),
                        (4, 'iciti', 'ic'),
                        (5, 'ical', 'ic'), (6, 'ful', ''),
                        (7, 'ness', '')
                    ) s(ord, suffix, replacement)
                    WHERE LEN(s.suffix) < LEN(@w) AND RIGHT(@w, LEN(s.suffix)) = s.suffix
                    ORDER BY s.ord;
                    IF @suffix IS NOT NULL
                    BEGIN
                        SET @j = LEN(@w) - LEN(@suffix);
                        IF (LEN(LEFT(@p, @j)) - LEN(REPLACE(LEFT(@p, @j), 'vc', ''))) / 2 > 0
                            SET @w = LEFT(@w, @j) + @replacement;
                    END;

                    -- Step 4: single suffixes, removed when m(stem) > 1
                    SET @p = {cv}(@w);
                    SET @suffix = NULL;
                    SELECT TOP 1 @suffix = s.suffix
                    FROM (VALUES
                        (1, 'al'), (2, 'ance'), (3, 'ence'), (4, 'er'), (5, 'ic'), (6, 'able'), (7, 'ible'),
                        (8, 'ant'), (9, 'ement'), (10, 'ment'), (11, 'ent'), (12, 'ion'), (13, 'ou'),
                        (14, 'ism'), (15, 'ate'), (16, 'iti'), (17, 'ous'), (18, 'ive'), (19, 'ize')
                    ) s(ord, suffix)
                    WHERE LEN(s.suffix) < LEN(@w) AND RIGHT(@w, LEN(s.suffix)) = s.suffix
                    ORDER BY s.ord;
                    IF @suffix IS NOT NULL
                    BEGIN
                        SET @j = LEN(@w) - LEN(@suffix);
                        IF (@suffix <> 'ion' OR SUBSTRING(@w, @j, 1) IN ('s', 't'))
                           AND (LEN(LEFT(@p, @j)) - LEN(REPLACE(LEFT(@p, @j), 'vc', ''))) / 2 > 1
                            SET @w = LEFT(@w, @j);
                    END;

                    -- Step 5a: a final -e; 5b: -ll -> -l. Both measure the word as it entered this step.
                    SET @p = {cv}(@w);
                    SET @m = (LEN(@p) - LEN(REPLACE(@p, 'vc', ''))) / 2;
                    IF RIGHT(@w, 1) = 'e'
                    BEGIN
                        IF @m > 1
                           OR (@m = 1 AND NOT (LEN(@w) >= 4 AND SUBSTRING(@p, LEN(@w) - 3, 3) = 'cvc'
                                               AND SUBSTRING(@w, LEN(@w) - 1, 1) NOT IN ('w', 'x', 'y')))
                            SET @w = LEFT(@w, LEN(@w) - 1);
                    END;
                    IF RIGHT(@w, 1) = 'l' AND LEN(@w) >= 2 AND SUBSTRING(@w, LEN(@w) - 1, 1) = 'l'
                       AND SUBSTRING(@p, LEN(@w), 1) = 'c' AND @m > 1
                        SET @w = LEFT(@w, LEN(@w) - 1);

                    RETURN CAST(@w AS nvarchar(4000));
                END
                """;
    }
}
