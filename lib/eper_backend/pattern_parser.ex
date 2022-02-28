defmodule EperBackend.PatternParser do

  def parse(input) do
    parser = pattern()
    parser.(input)
  end

  defp pattern() do
    many(or_parts())
    |> map(fn list ->
      # flatten
      List.first(list)
    end)
  end

  defp subpattern() do
    sequence([
      char(?(),
      lazy(fn -> pattern() end),
      char(?))
    ])
    |> map(fn [_, or_list, _] ->
      or_list
    end)
  end

  defp negative_subpattern() do
    sequence([
      char(?!),
      char(?(),
      lazy(fn -> pattern() end),
      char(?))
    ])
    |> map(fn [_, _, or_list, _] ->
      [ :not, or_list ]
    end)
  end

  defp lazy(combinator) do
    fn input ->
      parser = combinator.()
      parser.(input)
    end
  end

  defp and_parts() do
    separated_list(
      choice([
        subpattern(), negative_subpattern(), and_part()
      ]),
      many(char(?+)),
      :and)
  end

  defp or_parts() do
    separated_list(or_part(), char(?,), :or)
    |> map(fn list ->
      list
    end)
  end

  defp separated_list(element_parser, separator_parser, type) do
    sequence([
      element_parser,
      many(sequence([separator_parser, element_parser]))
    ])
    |> map(fn [first_element, rest] ->
      other_elements = Enum.map(rest, fn [_, element] ->
        element
      end)
      [first_element | other_elements]
    end)
    |> map(fn list ->
      [type | list]
    end)
  end

  defp sequence(parsers) do
    fn input ->
      case parsers do
        [] ->
          {:ok, [], input}

        [first_parser | other_parsers] ->
          with {:ok, first_term, rest} <- first_parser.(input),
               {:ok, other_terms, rest} <- sequence(other_parsers).(rest),
               do: {:ok, [first_term | other_terms], rest}
        end
    end
  end

  defp map(parser, mapper) do
    fn input ->
      with {:ok, term, rest} <- parser.(input),
           do: {:ok, mapper.(term), rest}
    end
  end

  defp or_part() do
    many(and_parts())
    |> satisfy(fn chars -> chars != [] end)
    |> map(fn list ->
      # flatten if just :and + value
      case List.first(list) do
        [:and, value] -> value
        list -> list
      end
    end)
  end

  defp and_part() do
    many(and_part_char())
    |> satisfy(fn chars -> chars != [] end)
    |> map(fn chars ->
      case chars do
        [?! | rest] -> [ :not, to_string(rest) ]
        _ -> to_string(chars)
      end
    end)
  end

  defp many(parser) do
    fn input ->
      case parser.(input) do
        {:error, _reason} ->
          {:ok, [], input}

        {:ok, first_term, rest} ->
          {:ok, other_terms, rest} = many(parser).(rest)
          {:ok, [first_term | other_terms], rest}
      end
    end
  end

  defp and_part_char(), do: choice([ascii_letter(), digit(), and_chars()])

  defp choice(parsers) do
    fn input ->
      case parsers do
        [] ->
          {:error, "no parser suceeded"}

        [first_parser | other_parsers] ->
          with {:error, _reason} <- first_parser.(input),
               do: choice(other_parsers).(input)
      end
    end
  end

  defp digit(), do: satisfy(char(), fn char -> char in ?0..?9 end)
  defp ascii_letter(), do: satisfy(char(), fn char -> char in ?A..?Z or char in ?a..?z end)
  defp and_chars(), do: satisfy(char(), fn char -> char in [?!, ?., ?/, ?_, ??, ?@] end)
  defp char(expected), do: satisfy(char(), fn char -> char == expected end)

  defp satisfy(parser, acceptor) do
    fn input ->
      with {:ok, term, rest} <- parser.(input) do
        if acceptor.(term),
          do: {:ok, term, rest},
          else: {:error, "term rejected"}
      end
    end
  end

  defp char() do
    fn input ->
      case input do
        "" -> {:error, "unexpected end of input"}
        <<char::utf8, rest::binary>> -> {:ok, char, rest}
      end
    end
  end

end
