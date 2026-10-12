namespace Polecat.Storage.FullText;

/// <summary>
///     Martin Porter's 1980 English stemmer, in the shape of his reference C implementation (#642).
///     The query-side half of <see cref="FullTextTokenizer.Porter" />; the write-side half is the
///     T-SQL function <see cref="PorterStemFunction" /> renders, and the two have to agree on every
///     word.
/// </summary>
/// <remarks>
///     <para>
///         ⚠️ <b>Two implementations of one rule, and the danger is the one this whole design is
///         most exposed to:</b> a query term stemmed differently from the stored term it should match
///         is not an error, just a search that quietly answers empty. They are held together by an
///         oracle rather than by each other — <c>porter_stemmer_tests</c> pins BOTH against SQLite
///         FTS5's <c>porter</c> tokenizer over 23,549 dictionary words, which also makes Polecat
///         stem exactly as Fisher does.
///     </para>
///     <para>
///         Terms shorter than three characters are returned unchanged, as SQLite's are.
///     </para>
/// </remarks>
internal static class PorterStemmer
{
    public static string Stem(string word)
    {
        if (!IsStemmable(word)) return word;

        var s = new State(word);
        s.Step1ab();
        if (s.K > 0)
        {
            s.Step1c();
            s.Step2();
            s.Step3();
            s.Step4();
            s.Step5();
        }

        return new string(s.B, 0, s.K + 1);
    }

    /// <summary>
    ///     Which tokens are stemmed at all: 3 to 64 characters, every one a lowercase ASCII letter.
    ///     Anything else passes through unchanged, on BOTH sides.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         The length bounds are SQLite's (<c>FTS5_PORTER_MAX_TOKEN</c> and its three-character
    ///         floor). The alphabet is Polecat's, and it is what lets the two implementations agree on
    ///         input the oracle cannot cover: Porter is defined over English letters, and T-SQL compares
    ///         under the database's collation, which is routinely accent-INSENSITIVE — so a
    ///         <c>é</c> would be a vowel to the trigger and a consonant here. Refusing to stem such a
    ///         token at all is the only rule both sides can implement identically. The T-SQL twin tests the
    ///         same thing with a binary collation for the same reason.
    ///     </para>
    /// </remarks>
    internal static bool IsStemmable(string word)
    {
        if (word.Length is < 3 or > 64) return false;
        foreach (var ch in word)
        {
            if (ch is < 'a' or > 'z') return false;
        }

        return true;
    }

    private sealed class State
    {
        public readonly char[] B;
        public int K;
        private int _j;

        public State(string word)
        {
            // room for the longest suffix replacement that can grow a word ("at" -> "ate")
            B = new char[word.Length + 1];
            word.CopyTo(0, B, 0, word.Length);
            K = word.Length - 1;
        }

        private bool Cons(int i)
        {
            switch (B[i])
            {
                case 'a': case 'e': case 'i': case 'o': case 'u':
                    return false;
                case 'y':
                    return i == 0 || !Cons(i - 1);
                default:
                    return true;
            }
        }

        // m() measures the number of consonant-vowel sequences between 0 and _j
        private int M()
        {
            var n = 0;
            var i = 0;
            while (true)
            {
                if (i > _j) return n;
                if (!Cons(i)) break;
                i++;
            }

            i++;
            while (true)
            {
                while (true)
                {
                    if (i > _j) return n;
                    if (Cons(i)) break;
                    i++;
                }

                i++;
                n++;
                while (true)
                {
                    if (i > _j) return n;
                    if (!Cons(i)) break;
                    i++;
                }

                i++;
            }
        }

        private bool VowelInStem()
        {
            for (var i = 0; i <= _j; i++)
            {
                if (!Cons(i)) return true;
            }

            return false;
        }

        private bool DoubleC(int j) => j >= 1 && B[j] == B[j - 1] && Cons(j);

        private bool Cvc(int i)
        {
            if (i < 2 || !Cons(i) || Cons(i - 1) || !Cons(i - 2)) return false;
            var ch = B[i];
            return ch != 'w' && ch != 'x' && ch != 'y';
        }

        private bool Ends(string s)
        {
            var length = s.Length;

            // STRICTLY shorter than the word: a suffix only matches with something in front of it.
            // Porter's reference C lets a whole word match its own suffix; SQLite's FTS5 porter, which
            // is the oracle and what Fisher stems with, does not, and falls through to the next rule.
            // Only a word that IS a suffix shows the difference: "sses" -> "sse", "ies" -> "ie",
            // "eed" -> "e" here, where the reference gives "ss", "i", "eed".
            if (length > K) return false;
            for (var i = 0; i < length; i++)
            {
                if (B[K - length + 1 + i] != s[i]) return false;
            }

            _j = K - length;
            return true;
        }

        private void SetTo(string s)
        {
            for (var i = 0; i < s.Length; i++) B[_j + 1 + i] = s[i];
            K = _j + s.Length;
        }

        private void R(string s)
        {
            if (M() > 0) SetTo(s);
        }

        public void Step1ab()
        {
            if (B[K] == 's')
            {
                if (Ends("sses")) K -= 2;
                else if (Ends("ies")) SetTo("i");
                else if (B[K - 1] != 's') K--;
            }

            if (Ends("eed"))
            {
                if (M() > 0) K--;
            }
            else if ((Ends("ed") || Ends("ing")) && VowelInStem())
            {
                K = _j;
                if (Ends("at")) SetTo("ate");
                else if (Ends("bl")) SetTo("ble");
                else if (Ends("iz")) SetTo("ize");
                else if (DoubleC(K))
                {
                    K--;
                    var ch = B[K];
                    if (ch == 'l' || ch == 's' || ch == 'z') K++;
                }
                else if (M() == 1 && Cvc(K))
                {
                    SetTo("e");
                }
            }
        }

        public void Step1c()
        {
            if (Ends("y") && VowelInStem()) B[K] = 'i';
        }

        public void Step2()
        {
            if (K == 0) return;
            switch (B[K - 1])
            {
                case 'a':
                    if (Ends("ational")) { R("ate"); break; }
                    if (Ends("tional")) { R("tion"); break; }
                    break;
                case 'c':
                    if (Ends("enci")) { R("ence"); break; }
                    if (Ends("anci")) { R("ance"); break; }
                    break;
                case 'e':
                    if (Ends("izer")) { R("ize"); break; }
                    break;
                case 'l':
                    if (Ends("bli")) { R("ble"); break; }
                    if (Ends("alli")) { R("al"); break; }
                    if (Ends("entli")) { R("ent"); break; }
                    if (Ends("eli")) { R("e"); break; }
                    if (Ends("ousli")) { R("ous"); break; }
                    break;
                case 'o':
                    if (Ends("ization")) { R("ize"); break; }
                    if (Ends("ation")) { R("ate"); break; }
                    if (Ends("ator")) { R("ate"); break; }
                    break;
                case 's':
                    if (Ends("alism")) { R("al"); break; }
                    if (Ends("iveness")) { R("ive"); break; }
                    if (Ends("fulness")) { R("ful"); break; }
                    if (Ends("ousness")) { R("ous"); break; }
                    break;
                case 't':
                    if (Ends("aliti")) { R("al"); break; }
                    if (Ends("iviti")) { R("ive"); break; }
                    if (Ends("biliti")) { R("ble"); break; }
                    break;
                case 'g':
                    if (Ends("logi")) { R("log"); break; }
                    break;
            }
        }

        public void Step3()
        {
            switch (B[K])
            {
                case 'e':
                    if (Ends("icate")) { R("ic"); break; }
                    if (Ends("ative")) { R(""); break; }
                    if (Ends("alize")) { R("al"); break; }
                    break;
                case 'i':
                    if (Ends("iciti")) { R("ic"); break; }
                    break;
                case 'l':
                    if (Ends("ical")) { R("ic"); break; }
                    if (Ends("ful")) { R(""); break; }
                    break;
                case 's':
                    if (Ends("ness")) { R(""); break; }
                    break;
            }
        }

        public void Step4()
        {
            if (K == 0) return;
            switch (B[K - 1])
            {
                case 'a':
                    if (Ends("al")) break;
                    return;
                case 'c':
                    if (Ends("ance")) break;
                    if (Ends("ence")) break;
                    return;
                case 'e':
                    if (Ends("er")) break;
                    return;
                case 'i':
                    if (Ends("ic")) break;
                    return;
                case 'l':
                    if (Ends("able")) break;
                    if (Ends("ible")) break;
                    return;
                case 'n':
                    if (Ends("ant")) break;
                    if (Ends("ement")) break;
                    if (Ends("ment")) break;
                    if (Ends("ent")) break;
                    return;
                case 'o':
                    if (Ends("ion") && _j >= 0 && (B[_j] == 's' || B[_j] == 't')) break;
                    if (Ends("ou")) break;
                    return;
                case 's':
                    if (Ends("ism")) break;
                    return;
                case 't':
                    if (Ends("ate")) break;
                    if (Ends("iti")) break;
                    return;
                case 'u':
                    if (Ends("ous")) break;
                    return;
                case 'v':
                    if (Ends("ive")) break;
                    return;
                case 'z':
                    if (Ends("ize")) break;
                    return;
                default:
                    return;
            }

            if (M() > 1) K = _j;
        }

        public void Step5()
        {
            _j = K;
            if (B[K] == 'e')
            {
                var a = M();
                if (a > 1 || (a == 1 && !Cvc(K - 1))) K--;
            }

            if (B[K] == 'l' && DoubleC(K) && M() > 1) K--;
        }
    }
}
