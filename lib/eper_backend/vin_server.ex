defmodule EperBackend.VinServer do
  use GenServer
  require Logger

  # Public API
  def start_link(_pid) do
    {:ok, _pid} = GenServer.start_link(__MODULE__, nil, name: __MODULE__)
  end

  def search(code) do
    GenServer.call(__MODULE__, {:search, code})
  end

  # GenServer implementation
  def init(_args) do
    {:ok, Ktdreader.Reader.from_file(Application.fetch_env!(:eper_backend, :vin_database))}
  end

  def handle_call({:search, code}, _from, reader) do
    rows =
      if String.length(code) == 17 do
        vin_code = String.slice(code, 3..5)
        chassis_number = String.slice(code, 9..-1)

        chassis_length = String.length(chassis_number)
        chassis_first = String.slice(chassis_number, 0, 1)

        chassis_number =
          if chassis_length == 8 do
            case chassis_first do
              n when n in ["1", "6", "8", "9"] ->
                "0#{String.slice(chassis_number, 1..-1)}"

              _ ->
                chassis_number
            end
          end

        model_code = EperBackend.PartsServer.model_code(vin_code)
        search_pk = "#{model_code}#{chassis_number}"

        Logger.info("Searching for #{search_pk}")

        case Ktdreader.Query.find_by_primary_key(reader, 1, search_pk) do
          %{"MVS" => chassy} ->
          # [chassy, organization, motor, vin, date, color_interior, _] ->
            _model = String.slice(chassy, 0, 3)
            version = String.slice(chassy, 3, 3)
            series = String.slice(chassy, 6, 1)
            EperBackend.PartsServer.mvs(model_code, version, series)
          _ ->
            nil
        end
      end

    {:reply, rows, reader}
  end
end
