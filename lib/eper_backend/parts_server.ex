defmodule EperBackend.PartsServer do
  use GenServer
  require Logger
  import EperBackend.Database

  # Public API
  def start_link(_pid) do
    {:ok, _pid} = GenServer.start_link(__MODULE__, 10_000, name: __MODULE__)
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
    {:ok, conn, {:continue, :load_database}}
  end

  def handle_continue(:load_database, conn) do
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
    {:noreply, conn}
  end


  def handle_call({:makes}, _from, conn) do
    {:reply, list_cache(:eper_makes), conn}
  end

  def handle_call({:make, make}, _from, conn) do
    {:reply, lookup_cache(:eper_makes, make), conn}
  end

  def handle_call({:models}, _from, conn) do
    {:reply, list_cache(:eper_models), conn}
  end

  def handle_call({:catalogues, make, model}, _from, conn) do
    catalogues = match_cache(:eper_catalogues, %{make: make, model: model})
    {:reply, catalogues, conn}
  end

  def handle_call({:groups, catalogue}, _from, conn) do
    groups = match_cache(:eper_groups, %{catalogue: catalogue})
    {:reply, groups, conn}
  end

  def handle_call({:sub_groups, catalogue, group}, _from, conn) do
    sub_groups = match_cache(:eper_sub_groups, %{catalogue: catalogue, group: group})
    {:reply, sub_groups, conn}
  end

  def handle_call({:drawings, catalogue, group, sub_group}, _from, conn) do
    rows =
      query_table_index(
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
        fn [
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
          description = lookup_cache(:eper_drawing_descriptions, dsc_code)

          pattern =
            case EperBackend.PatternParser.parse(pattern) do
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
        end
      )

    {:reply, rows, conn}
  end

  def handle_call({:model_code, search_vin_code}, _from, conn) do
    models = query(conn, "VIN", ["VIN_COD", "MOD_COD"])

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
    rows =
      query_table_index(
        conn,
        "MVS",
        :eper_mvs_index,
        [model, version, series],
        ~w(CAT_COD MOD_COD MVS_VERSION MVS_SERIE VMK_TYPE_M VMK_COD_M VMK_TYPE_V VMK_COD_V MVS_DSC MVS_SINCOM_VERS MVS_ENGINE_TYPE SINCOM PATTERN),
        fn [
             catalogue,
             model,
             version,
             series,
             motor_type,
             motor_code,
             version_type,
             version_code,
             description,
             sincom_vers,
             engine_type,
             sincom,
             pattern
           ] ->
          catalogue = lookup_cache(:eper_catalogues, catalogue)

          pattern = case EperBackend.PatternParser.parse(pattern) do
            {:ok, pattern, ""} -> describe_pattern(catalogue, pattern)
            _ -> pattern
          end

          %{
            catalogue: catalogue,
            model: model,
            version: version,
            series: series,
            description: description,
            pattern: pattern,
            vmkMotor: "#{motor_type}#{motor_code}",
            vmkVersion: "#{version_type}#{version_code}",
            engineType: engine_type,
            sincomVersion: sincom_vers,
            sincom: sincom
          }
        end
      )

    {:reply, rows, conn}
  end

  def handle_call({:tbdata, catalogue, group, sub_group, sgs_code}, _from, conn) do
    rows =
      query_table_index(
        conn,
        "TBDATA",
        :eper_tbdata_index,
        [catalogue, group, sub_group, sgs_code],
        ["DRW_NUM", "TBD_RIF", "TBD_QTY", "HOTSPOTS", "PRT_COD"],
        fn [drawing_number, drawing_part_number, qty, hotspots, part_code] ->
          %{
            drawing_number: drawing_number,
            drawing_part_number: drawing_part_number,
            hotspots: hotspots,
            qty: qty,
            part_code: part_code
          }
        end
      )

    {:reply, rows, conn}
  end

  defp describe_pattern(catalogue, pattern) do
    case pattern do
      # mvs pattern is probably simple ":and" list
      [:or, [:and | attributes]] ->
        Enum.map(attributes, fn typecode ->
          # [_, _typecode] =
          case typecode do
            [:not, typecode] ->
              [true, typecode]

            _ ->
              [false, typecode]
          end
        end)
        # filter without attributes
        |> Enum.filter(fn [without, _] -> !without end)
        |> Enum.map(fn [_, typecode] ->
          [%{description: description, type: type, code: code}] = match_cache(:eper_vmk_descriptions, %{catalogue: catalogue.code, typecode: typecode})

          # :ets.match_object(
          #   :eper_vmk_descriptions,
          #   {:"$1", %{catalogue: catalogue, typecode: typecode}}
          # )

          carat_description =
            case [type, code] do
              [_, ""] ->
                ""

              [_, _] ->
                %{description: carat_description} = lookup_cache(:eper_carat_descriptions, {catalogue.code, type})
                carat_description
            end

          # description = if without, do: "WITHOUT #{description}", else: description
          [typecode, carat_description, description]
        end)

      _ ->
        pattern
    end
  end
end
