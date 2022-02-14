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

  # ZFA19200000508303
  # ZFA35600006123456

  def handle_call({:search, code}, _from, reader) do
    Logger.info("Searching for #{code}")
    block = Ktdreader.Block.find(reader, code)
    rows = Ktdreader.Rows.find(reader, block, code)
    {:reply, rows, reader}
  end
end
