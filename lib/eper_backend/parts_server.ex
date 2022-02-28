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

  def make(make) do
    GenServer.call(__MODULE__, {:make, make})
  end

  def models() do
    GenServer.call(__MODULE__, {:models})
  end

  def catalogues(make, model) do
    GenServer.call(__MODULE__, {:catalogues, make, model})
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

  def model_code(vin_code) do
    GenServer.call(__MODULE__, {:model_code, vin_code})
  end

  def mvs(model_code, version, series) do
    GenServer.call(__MODULE__, {:mvs, model_code, version, series})
  end

  def tbdata(catalogue, group, sub_group, sgs_code) do
    GenServer.call(__MODULE__, {:tbdata, catalogue, group, sub_group, sgs_code})
  end

  # GenServer implementation
  def init(_args) do
    {:ok, conn} =
      Jetdb.Connection.from_file(Application.fetch_env!(:eper_backend, :parts_database))

    load_makes(conn)
    load_models(conn)
    load_groups(conn)
    load_vmk_descriptions(conn)
    load_carat_descriptions(conn)
    load_sub_groups(conn)
    load_catalogues(conn)
    load_mvs(conn)
    load_drawings(conn)
    load_tbdata(conn)
    {:ok, conn}
  end

  defp load_makes(conn) do
    Logger.info("Loading makes")
    EperBackend.Database.cache_table(conn,
      "MAKES",
      :eper_makes,
      ["MK_COD", "MK_DSC", "BRAND_COD"],
      fn [make, description, brand_code] ->
        data = %{
          make: make,
          description: description,
          brand_code: brand_code,
          image: "/api/image/logo/#{make}.png",
        }
        {make, data}
    end)
  end

  defp load_vmk_descriptions(conn) do
    Logger.info("Loading vmk descriptions")
    EperBackend.Database.cache_table(conn,
      "VMK_DSC",
      :eper_vmk_descriptions,
      ["CAT_COD","VMK_TYPE","VMK_COD","LNG_COD","VMK_DSC"],
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
    end)
  end

  defp load_carat_descriptions(conn) do
    Logger.info("Loading carat descriptions")

    EperBackend.Database.cache_table(conn,
      "CARAT_DSC",
      :eper_carat_descriptions,
      ["CAT_COD","VMK_TYPE","LNG_COD","VMK_DSC"],
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
    end)
  end

  defp load_groups(conn) do
    Logger.info("Loading groups")

    EperBackend.Database.cache_table(conn,
      "GROUPS_DSC",
      :eper_group_descriptions,
      ["LNG_COD", "GRP_COD", "GRP_DSC"],
      fn [language, code, description] ->
        if language == "3", do: {Integer.to_string(code), description}
    end)

    EperBackend.Database.cache_table(conn,
      "GROUPS",
      :eper_groups,
      ["CAT_COD", "GRP_COD", "IMG_NAME"],
      fn [catalogue, group, image] ->
        description = EperBackend.Database.lookup_cache(:eper_group_descriptions, group)
        key = {catalogue, group}
        data = %{
          catalogue: catalogue,
          code: group,
          image: "/api/image/#{image}",
          description: description
        }
        {key, data}
    end)
  end

  defp load_sub_groups(conn) do
    Logger.info("Loading sub_groups")

    EperBackend.Database.cache_table(conn,
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
    end)

    EperBackend.Database.cache_table(conn,
      "SUBGROUPS_BY_CAT",
      :eper_sub_groups,
      [
        "CAT_COD",
        "GRP_COD",
        "SGRP_COD",
        "IMG_NAME"
      ],
      fn [catalogue, group, sub_group, image] ->
        description = EperBackend.Database.lookup_cache(:eper_sub_group_descriptions, [group, sub_group])
        key = [catalogue, Integer.to_string(group), Integer.to_string(sub_group)]

        data = %{
          catalogue: catalogue,
          group: Integer.to_string(group),
          code: Integer.to_string(sub_group),
          image: "/api/image/#{image}",
          description: description
        }
        {key, data}
    end)
  end

  defp load_models(conn) do
    EperBackend.Database.cache_table(conn,
      "COMM_MODGRP",
      :eper_models,
      ["MK2_COD", "CMG_COD", "CMG_DSC", "CMG_SORT_KEY"],
      fn [make, model, description, sort_key] ->
        key = {make, model}
        data =%{
          make: make,
          model: model,
          description: description,
          sort_key: sort_key,
          image: "/api/image/logo/#{make}/#{model}.jpg"
        }
        {key, data}
    end)
  end

  defp load_catalogues(conn) do
    Logger.info("Loading catalogues")
    EperBackend.Database.cache_table(conn,
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
    end)
  end

  defp load_drawings(conn) do
    Logger.info("Loading drawings")
    EperBackend.Database.cache_table(conn,
      "TABLES_DSC",
      :eper_drawing_descriptions,
      ["LNG_COD", "COD", "DSC"],
      fn [language, code, description] ->
        if language == "3", do: {code, description}
    end)

    EperBackend.Database.index_table(conn, "DRAWINGS", :eper_drawings_index, ["CAT_COD", "GRP_COD","SGRP_COD"])
  end

  defp load_mvs(conn) do
    Logger.info("Loading mvs")
    EperBackend.Database.index_table(conn, "MVS", :eper_mvs_index,  ~w(CAT_COD MOD_COD MVS_VERSION MVS_SERIE))
  end

  defp load_tbdata(conn) do
    Logger.info("Loading tbdata")
    EperBackend.Database.index_table(conn, "TBDATA", :eper_tbdata_index,  ~w(CAT_COD GRP_COD SGRP_COD SGS_COD))
  end

  def handle_call({:makes}, _from, conn) do
    {:reply, EperBackend.Database.list_cache(:eper_makes), conn}
  end

  def handle_call({:make, make}, _from, conn) do
    {:reply, EperBackend.Database.lookup_cache(:eper_makes, make), conn}
  end

  def handle_call({:models}, _from, conn) do
    {:reply, EperBackend.Database.list_cache(:eper_models), conn}
  end

  def handle_call({:catalogues, make, model}, _from, conn) do
    catalogues =
      :ets.match_object(:eper_catalogues, {:"$1", %{make: make, model: model}})
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
    rows = EperBackend.Database.query_table_index(
      conn,
      "DRAWINGS",
      :eper_drawings_index,
      [catalogue, group, sub_group],
      [
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
      ],
      fn [catalogue, group, sub_group, sgs_code, drawing_number, code, _variant,
      _revision, image, _modif, dsc_code, pattern, _hotspots] ->
        description = EperBackend.Database.lookup_cache(:eper_drawing_descriptions, dsc_code)
        pattern = case EperBackend.PatternParser.parse(pattern) do
          {:ok, parsed, ""} -> parsed
          _ -> pattern
        end

        %{
        catalogue: catalogue,
        group: Integer.to_string(group),
        sub_group: Integer.to_string(sub_group),
        sgs_code: Integer.to_string(sgs_code),
        drawing_number: drawing_number,
        code: code,
        description: description,
        image: "/api/image/#{image}",
        pattern: pattern
        }
    end)
    {:reply, rows, conn}
  end

  def handle_call({:model_code, search_vin_code}, _from, conn) do
    {:ok, models} = Jetdb.Query.query(conn, :select, "VIN", ["VIN_COD", "MOD_COD"])

    model_code =
      case Enum.find(models, fn [vin_code, _model_code] ->
             vin_code == search_vin_code
           end) do
        [_vin_code, model_code] -> model_code
        _ -> nil
      end

    {:reply, model_code, conn}
  end

  def handle_call({:mvs, model, version, series}, _from, conn) do
    rows = EperBackend.Database.query_table_index(
      conn,
      "MVS",
      :eper_mvs_index,
      [model, version, series],
      ~w(CAT_COD MOD_COD MVS_VERSION MVS_SERIE VMK_TYPE_M VMK_COD_M VMK_TYPE_V VMK_COD_V MVS_DSC MVS_SINCOM_VERS MVS_ENGINE_TYPE SINCOM PATTERN),
      fn [catalogue, model, version, series, motor_type, motor_code, version_type, version_code, description, sincom_vers, engine_type, sincom, pattern] ->
        catalogue = EperBackend.Database.lookup_cache(:eper_catalogues, catalogue)
        %{
          catalogue: catalogue,
          model: model,
          version: version,
          series: series,
          description: description,
          pattern:  describe_pattern(catalogue, pattern),
          vmkMotor: "#{motor_type}#{motor_code}",
          vmkVersion: "#{version_type}#{version_code}",
          engineType: engine_type,
          sincomVersion: sincom_vers,
          sincom: sincom
        }
      end)
    {:reply, rows, conn}
  end

  def handle_call({:tbdata, catalogue, group, sub_group, sgs_code}, _from, conn) do
    rows = EperBackend.Database.query_table_index(
      conn,
      "TBDATA",
      :eper_tbdata_index,
      [catalogue, group, sub_group, sgs_code],
      ["DRW_NUM","TBD_RIF","TBD_QTY","HOTSPOTS","PRT_COD"],
      fn [drawing_number, drawing_part_number, qty, hotspots, part_code] ->
        %{
          drawing_number: drawing_number,
          drawing_part_number: drawing_part_number,
          hotspots: hotspots,
          qty: qty,
          part_code: part_code,
        }
    end)
    {:reply, rows, conn}
  end

  defp describe_pattern(catalogue, pattern) do
    case pattern do
      # mvs pattern is probably simple ":and" list
      [:or, [:and | attributes]] ->
        Enum.map(attributes, fn typecode ->
          [without, typecode] = case typecode do
            [:not, typecode] ->
              [ true, typecode ]
            _ ->
              [ false, typecode ]
          end
          [{_,%{description: description, type: type, code: code}}] = :ets.match_object(:eper_vmk_descriptions, {:"$1", %{catalogue: catalogue, typecode: typecode}})
          carat_description = case [type, code] do
            [_, ""] ->
              ""
            [_, _] ->
              [{_,%{description: carat_description}}] =  :ets.lookup(:eper_carat_descriptions, {catalogue, type})
              carat_description
          end
          description = if without, do: "WITHOUT #{description}", else: description
          [typecode, carat_description, description]
        end)
      _ ->
        pattern
    end
  end
end
