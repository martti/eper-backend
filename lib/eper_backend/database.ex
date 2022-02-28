defmodule EperBackend.Database do
  def index_table(conn, table_name, index_name, columns) do
    :ets.new(index_name, [:set, :protected, :named_table])
    {:ok, rows} = Jetdb.Query.query(conn, :index, table_name, columns)
    Enum.each(make_map(rows), fn {key, table_rows = %{}} ->
      :ets.insert_new(index_name, {key, table_rows})
    end)
  end

  def cache_table(conn, table_name, ets_name, columns, handle_row) do
    :ets.new(ets_name, [:set, :protected, :named_table])
    {:ok, rows} = Jetdb.Query.query(conn, :select, table_name, columns)
    Enum.each(rows, fn row ->
      row_data = handle_row.(row)
      if !is_nil(row_data), do: :ets.insert_new(ets_name, row_data)
    end)
  end

  def list_cache(ets_name) do
    :ets.tab2list(ets_name)|> Enum.map(&elem(&1, 1))
  end

  def lookup_cache(ets_name, key) do
    case :ets.lookup(ets_name, key) do
      [{_, object=%{}}] -> object
      [{_, value}] -> value
      _ -> nil
    end
  end

  defp make_map(rows) do
    Enum.reduce(rows, %{}, fn [table, row, columns], acc ->
      columns = Enum.map(columns, fn c -> if is_integer(c), do: Integer.to_string(c), else: c end)
      case is_map_key(acc, columns) do
        true ->
          tr = case is_map_key(acc[columns], table) do
            true -> Map.put(acc[columns], table, [row | acc[columns][table]])
            false -> Map.put(acc[columns], table, [row])
          end
          Map.put(acc, columns, tr)
        false ->
          Map.put(acc, columns, %{table => [row]})
      end
    end)
  end

  defp convert_map(table_rows), do: Enum.map(Map.to_list(table_rows), &(Tuple.to_list(&1)))

  def query_table_index(conn, table_name, index_name, key, columns, handle_row) do
    case :ets.lookup(index_name, key) do
      [{_key, table_rows}] ->
        {:ok, rows} = Jetdb.Query.query(conn, :select, table_name, columns, convert_map(table_rows))
        Enum.map(rows, &handle_row.(&1))
      _ ->
        []
    end
  end
end
