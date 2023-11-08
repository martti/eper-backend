defmodule EperBackend.Database do
  require Logger

  def index_table(_conn, table_name, index_name, columns) do
    {:ok, conn} = Jetdb.Connection.from_file(Application.fetch_env!(:eper_backend, :parts_database))

    :ets.new(index_name, [:set, :protected, :named_table])
    {:ok, rows} = Jetdb.Query.query(conn, :index, table_name, columns)

    Enum.each(make_map(rows), fn {key, table_rows = %{}} ->
      :ets.insert_new(index_name, {key, table_rows})
    end)
  end

  def index_table_parallel(_conn, table_name, index_name, columns) do
    {:ok, conn} = Jetdb.Connection.from_file(Application.fetch_env!(:eper_backend, :parts_database))

    :ets.new(index_name, [:set, :protected, :named_table])

    schema_table = Enum.find(conn.schema, fn x -> Enum.at(Enum.at(x, 0), 2) == table_name end)
    tdef = Enum.at(schema_table, 1)
    used_pages_map = Jetdb.Table.used_pages_map(conn.data_file, tdef[:used_pages_page])
    select_columns = Enum.filter(tdef[:columns], fn x -> x[:name] in columns end)
    chunks = Enum.chunk_every(used_pages_map, 1000)

    max_concurrency = System.schedulers_online()

    chunks
    |> Task.async_stream(fn chunk ->
      Logger.info("Loading chunk: #{table_name}")
      {:ok, task_conn} = Jetdb.Connection.from_file(Application.fetch_env!(:eper_backend, :parts_database))
      rows = Jetdb.Rows.read_rows_with_index(task_conn.data_file, chunk, select_columns)
      :file.close(task_conn.data_file.data_file)
      rows
    end, timeout: 20_000, max_concurrency: max_concurrency)
    |> Enum.reduce([], fn({:ok, result}, acc) ->
      acc ++ result
    end)
    |> make_map
    |> Enum.each(fn {key, table_rows = %{}} ->
      :ets.insert_new(index_name, {key, table_rows})
    end)

    :file.close(conn.data_file.data_file)
  end

  def cache_table(_conn, table_name, ets_name, columns, handle_row) do
    {:ok, conn} = Jetdb.Connection.from_file(Application.fetch_env!(:eper_backend, :parts_database))

    :ets.new(ets_name, [:set, :protected, :named_table])
    {:ok, rows} = Jetdb.Query.query(conn, :select, table_name, columns)

    Enum.each(rows, fn row ->
      row_data = handle_row.(row)
      if !is_nil(row_data), do: :ets.insert_new(ets_name, row_data)
    end)
  end

  def query(_conn, table_name, columns) do
    {:ok, conn} = Jetdb.Connection.from_file(Application.fetch_env!(:eper_backend, :parts_database))
    {:ok, rows} = Jetdb.Query.query(conn, :select, table_name, columns)
    rows
  end

  def list_cache(ets_name) do
    :ets.tab2list(ets_name) |> Enum.map(&elem(&1, 1))
  end

  def lookup_cache(ets_name, key) do
    case :ets.lookup(ets_name, key) do
      [{_, object = %{}}] -> object
      [{_, value}] -> value
      _ -> nil
    end
  end

  def match_cache(ets_name, match_object) do
    :ets.match_object(ets_name, {:"$1", match_object})
    |> Enum.map(&elem(&1, 1))
  end

  defp make_map(rows) do
    Enum.reduce(rows, %{}, fn [table, row, columns], acc ->
      columns = Enum.map(columns, fn c -> if is_integer(c), do: Integer.to_string(c), else: c end)

      case is_map_key(acc, columns) do
        true ->
          tr =
            case is_map_key(acc[columns], table) do
              true -> Map.put(acc[columns], table, [row | acc[columns][table]])
              false -> Map.put(acc[columns], table, [row])
            end

          Map.put(acc, columns, tr)

        false ->
          Map.put(acc, columns, %{table => [row]})
      end
    end)
  end

  defp convert_map(table_rows), do: Enum.map(Map.to_list(table_rows), &Tuple.to_list(&1))

  def query_table_index(_conn, table_name, index_name, key, columns, handle_row) do
    {:ok, conn} = Jetdb.Connection.from_file(Application.fetch_env!(:eper_backend, :parts_database))

    case :ets.lookup(index_name, key) do
      [{_key, table_rows}] ->

        {:ok, rows} =
          Jetdb.Query.query(conn, :select, table_name, columns, convert_map(table_rows))

        Enum.map(rows, &handle_row.(&1))

      _ ->
        []
    end
  end

  def load_makes(conn) do
    Logger.info("Loading makes")

    cache_table(
      conn,
      "MAKES",
      :eper_makes,
      ["MK_COD", "MK_DSC", "BRAND_COD"],
      fn [make, description, brand_code] ->
        data = %{
          make: make,
          description: description,
          brand_code: brand_code,
          image: "/api/image/logo/#{make}.png"
        }

        {make, data}
      end
    )
  end

  def load_vmk_descriptions(conn) do
    Logger.info("Loading vmk descriptions")

    cache_table(
      conn,
      "VMK_DSC",
      :eper_vmk_descriptions,
      ["CAT_COD", "VMK_TYPE", "VMK_COD", "LNG_COD", "VMK_DSC"],
      fn [catalogue, type, code, language, description] ->
        if language == "3" do
          data = %{
            catalogue: catalogue,
            type: type,
            code: code,
            typecode: "#{type}#{code}",
            description: description
          }

          key = {catalogue, type, code}
          {key, data}
        end
      end
    )
  end

  def load_carat_descriptions(conn) do
    Logger.info("Loading carat descriptions")

    cache_table(
      conn,
      "CARAT_DSC",
      :eper_carat_descriptions,
      ["CAT_COD", "VMK_TYPE", "LNG_COD", "VMK_DSC"],
      fn [catalogue, type, language, description] ->
        if language == "3" do
          data = %{
            catalogue: catalogue,
            type: type,
            description: description
          }

          key = {catalogue, type}
          {key, data}
        end
      end
    )
  end

  def load_groups(conn) do
    Logger.info("Loading groups")

    cache_table(
      conn,
      "GROUPS_DSC",
      :eper_group_descriptions,
      ["LNG_COD", "GRP_COD", "GRP_DSC"],
      fn [language, code, description] ->
        if language == "3", do: {Integer.to_string(code), description}
      end
    )

    cache_table(
      conn,
      "GROUPS",
      :eper_groups,
      ["CAT_COD", "GRP_COD", "IMG_NAME"],
      fn [catalogue, group, image] ->
        description = lookup_cache(:eper_group_descriptions, group)
        key = {catalogue, group}

        data = %{
          catalogue: catalogue,
          code: group,
          image: "/api/image/#{image}",
          description: description
        }

        {key, data}
      end
    )
  end

  def load_sub_groups(conn) do
    Logger.info("Loading sub_groups")

    cache_table(
      conn,
      "SUBGROUPS_DSC",
      :eper_sub_group_descriptions,
      [
        "LNG_COD",
        "GRP_COD",
        "SGRP_COD",
        "SGRP_DSC"
      ],
      fn [language, group, sub_group, description] ->
        if language == "3", do: {[group, sub_group], description}
      end
    )

    cache_table(
      conn,
      "SUBGROUPS_BY_CAT",
      :eper_sub_groups,
      [
        "CAT_COD",
        "GRP_COD",
        "SGRP_COD",
        "IMG_NAME"
      ],
      fn [catalogue, group, sub_group, image] ->
        description =
          lookup_cache(:eper_sub_group_descriptions, [group, sub_group])

        key = [catalogue, Integer.to_string(group), Integer.to_string(sub_group)]

        data = %{
          catalogue: catalogue,
          group: Integer.to_string(group),
          code: Integer.to_string(sub_group),
          image: "/api/image/#{image}",
          description: description
        }

        {key, data}
      end
    )
  end

  def load_models(conn) do
    cache_table(
      conn,
      "COMM_MODGRP",
      :eper_models,
      ["MK2_COD", "CMG_COD", "CMG_DSC", "CMG_SORT_KEY"],
      fn [make, model, description, sort_key] ->
        key = {make, model}

        data = %{
          make: make,
          model: model,
          description: description,
          sort_key: sort_key,
          image: "/api/image/logo/#{make}/#{model}.jpg"
        }

        {key, data}
      end
    )
  end

  def load_catalogues(conn) do
    Logger.info("Loading catalogues")

    cache_table(
      conn,
      "CATALOGUES",
      :eper_catalogues,
      ["MK_COD", "CAT_COD", "CAT_DSC", "CAT_SORT_KEY", "CMG_COD", "IMG_NAME"],
      fn [make, catalogue, description, sort_key, model, image] ->
        data = %{
          make: make,
          model: model,
          code: catalogue,
          description: description,
          sort_key: sort_key,
          image: "/api/image/#{image}"
        }

        {catalogue, data}
      end
    )
  end

  def load_drawings(conn) do
    Logger.info("Loading drawings")

    cache_table(
      conn,
      "TABLES_DSC",
      :eper_drawing_descriptions,
      ["LNG_COD", "COD", "DSC"],
      fn [language, code, description] ->
        if language == "3", do: {code, description}
      end
    )

    index_table(conn, "DRAWINGS", :eper_drawings_index, [
      "CAT_COD",
      "GRP_COD",
      "SGRP_COD"
    ])

    Logger.info("Done")
  end

  def load_mvs(conn) do
    Logger.info("Loading mvs")

    # CAT_COD
    index_table(
      conn,
      "MVS",
      :eper_mvs_index,
      ~w(MOD_COD MVS_VERSION MVS_SERIE)
    )
  end

  def load_tbdata(conn) do
    Logger.info("Loading tbdata")

    index_table(
      conn,
      "TBDATA",
      :eper_tbdata_index,
      ~w(CAT_COD GRP_COD SGRP_COD SGS_COD)
    )
    Logger.info("Done")
  end
end
