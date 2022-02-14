defmodule EperBackend.PartsServer do
  use GenServer
  require Logger

  # Public API
  def start_link(_pid) do
    {:ok, _pid} = GenServer.start_link(__MODULE__, nil, name: __MODULE__)
  end

  def makes() do
    GenServer.call(__MODULE__, {:makes})
  end

  def catalogues(make) do
    GenServer.call(__MODULE__, {:catalogues, make})
  end

  def groups(catalogue) do
    GenServer.call(__MODULE__, {:groups, catalogue})
  end

  def sub_groups(catalogue, group) do
    GenServer.call(__MODULE__, {:sub_groups, catalogue, group})
  end

  def drawings(catalogue, group, sub_group) do
    GenServer.call(__MODULE__, {:drawings, catalogue, group, sub_group}, 10000)
  end

  # GenServer implementation
  def init(_args) do
    {:ok, conn} =
      Jetdb.Connection.from_file(Application.fetch_env!(:eper_backend, :parts_database))

    load_makes(conn)
    load_groups(conn)
    load_sub_groups(conn)
    load_catalogues(conn)
    load_drawings(conn)
    {:ok, conn}
  end

  defp load_makes(conn) do
    Logger.info("Loading makes")
    :ets.new(:eper_makes, [:set, :protected, :named_table])
    {:ok, rows} = Jetdb.Query.query(conn, :select, "MAKES", ["MK_COD", "MK_DSC", "BRAND_COD"])

    Enum.each(rows, fn [make, description, brand_code] ->
      data = %{
        make: make,
        description: description,
        brand_code: brand_code
      }

      :ets.insert_new(:eper_makes, {make, data})
    end)
  end

  defp load_groups(conn) do
    Logger.info("Loading groups")

    # Logger.info("Searching for #{code}")
    :ets.new(:eper_group_descriptions, [:set, :protected, :named_table])

    {:ok, desc_rows} =
      Jetdb.Query.query(conn, :select, "GROUPS_DSC", ["LNG_COD", "GRP_COD", "GRP_DSC"])

    Enum.each(desc_rows, fn [language, code, description] ->
      if language == "3" do
        :ets.insert_new(:eper_group_descriptions, {Integer.to_string(code), description})
      end
    end)

    :ets.new(:eper_groups, [:set, :protected, :named_table])
    {:ok, rows} = Jetdb.Query.query(conn, :select, "GROUPS", ["CAT_COD", "GRP_COD", "IMG_NAME"])

    Enum.each(rows, fn [catalogue, group, image] ->
      [{_dsc_code, description}] = :ets.lookup(:eper_group_descriptions, group)

      key = "#{catalogue},#{group}"

      data = %{
        catalogue: catalogue,
        code: group,
        image: "/api/image/#{image}",
        description: description
      }

      :ets.insert_new(:eper_groups, {key, data})
    end)
  end

  defp load_sub_groups(conn) do
    Logger.info("Loading sub_groups")

    # Logger.info("Searching for #{code}")
    :ets.new(:eper_sub_group_descriptions, [:set, :protected, :named_table])

    {:ok, desc_rows} =
      Jetdb.Query.query(conn, :select, "SUBGROUPS_DSC", [
        "LNG_COD",
        "GRP_COD",
        "SGRP_COD",
        "SGRP_DSC"
      ])

    Enum.each(desc_rows, fn [language, group, sub_group, description] ->
      if language == "3" do
        :ets.insert_new(:eper_sub_group_descriptions, {[group, sub_group], description})
      end
    end)

    :ets.new(:eper_sub_groups, [:set, :protected, :named_table])

    {:ok, rows} =
      Jetdb.Query.query(conn, :select, "SUBGROUPS_BY_CAT", [
        "CAT_COD",
        "GRP_COD",
        "SGRP_COD",
        "IMG_NAME"
      ])

    Enum.each(rows, fn [catalogue, group, sub_group, image] ->
      [{_dsc_code, description}] = :ets.lookup(:eper_sub_group_descriptions, [group, sub_group])

      key = [catalogue, Integer.to_string(group), Integer.to_string(sub_group)]

      data = %{
        catalogue: catalogue,
        group: Integer.to_string(group),
        code: Integer.to_string(sub_group),
        image: "/api/image/#{image}",
        description: description
      }

      :ets.insert_new(:eper_sub_groups, {key, data})
    end)
  end

  defp load_catalogues(conn) do
    Logger.info("Loading catalogues")
    :ets.new(:eper_catalogues, [:set, :protected, :named_table])

    {:ok, rows} =
      Jetdb.Query.query(conn, :select, "CATALOGUES", ["MK_COD", "CAT_COD", "CAT_DSC", "IMG_NAME"])

    Enum.each(rows, fn [make, catalogue, description, image] ->
      data = %{
        make: make,
        code: catalogue,
        description: description,
        image: "/api/image/#{image}"
      }

      :ets.insert_new(:eper_catalogues, {catalogue, data})
    end)
  end

  defp load_drawings(conn) do
    Logger.info("Loading drawings")
    :ets.new(:eper_drawing_descriptions, [:set, :protected, :named_table])
    {:ok, desc_rows} = Jetdb.Query.query(conn, :select, "TABLES_DSC", ["LNG_COD", "COD", "DSC"])

    Enum.each(desc_rows, fn [language, code, description] ->
      if language == "3" do
        :ets.insert_new(:eper_drawing_descriptions, {code, description})
      end
    end)

    :ets.new(:eper_drawings, [:set, :protected, :named_table])

    {:ok, rows} =
      Jetdb.Query.query(conn, :select, "DRAWINGS", [
        "CAT_COD",
        "GRP_COD",
        "SGRP_COD",
        "SGS_COD",
        "DRW_NUM",
        "TABLE_COD",
        "VARIANTE",
        "REVISIONE",
        "IMG_PATH",
        "MODIF",
        "TABLE_DSC_COD",
        "PATTERN",
        "HOTSPOTS"
      ])

    Enum.each(rows, fn [
                         catalogue,
                         group,
                         sub_group,
                         sgs_code,
                         drawing_number,
                         code,
                         _variant,
                         _revision,
                         image,
                         _modif,
                         dsc_code,
                         pattern,
                         _hotspots
                       ] ->
      [{_dsc_code, description}] = :ets.lookup(:eper_drawing_descriptions, dsc_code)
      key = "#{catalogue},#{group},#{sub_group},#{sgs_code},#{drawing_number}"

      data = %{
        catalogue: catalogue,
        group: Integer.to_string(group),
        sub_group: Integer.to_string(sub_group),
        code: code,
        description: description,
        image: "/api/image/#{image}",
        pattern: pattern
      }

      :ets.insert_new(:eper_drawings, {key, data})
    end)
  end

  def handle_call({:makes}, _from, conn) do
    makes =
      :ets.tab2list(:eper_makes)
      |> Enum.map(&elem(&1, 1))

    {:reply, makes, conn}
  end

  def handle_call({:catalogues, make}, _from, conn) do
    catalogues =
      :ets.match_object(:eper_catalogues, {:"$1", %{make: make}})
      |> Enum.map(&elem(&1, 1))

    {:reply, catalogues, conn}
  end

  def handle_call({:groups, catalogue}, _from, conn) do
    groups =
      :ets.match_object(:eper_groups, {:"$1", %{catalogue: catalogue}})
      |> Enum.map(&elem(&1, 1))

    {:reply, groups, conn}
  end

  def handle_call({:sub_groups, catalogue, group}, _from, conn) do
    sub_groups =
      :ets.match_object(:eper_sub_groups, {:"$1", %{catalogue: catalogue, group: group}})
      |> Enum.map(&elem(&1, 1))

    {:reply, sub_groups, conn}
  end

  def handle_call({:drawings, catalogue, group, sub_group}, _from, conn) do
    drawings =
      :ets.match_object(
        :eper_drawings,
        {:"$1", %{catalogue: catalogue, group: group, sub_group: sub_group}}
      )
      |> Enum.map(&elem(&1, 1))

    {:reply, drawings, conn}
  end
end
