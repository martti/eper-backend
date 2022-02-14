defmodule EperBackend.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application
  require Logger

  @impl true
  def start(_type, _args) do
    children = [
      EperBackend.VinServer,
      EperBackend.PartsServer,
      {Plug.Cowboy, scheme: :http, plug: EperBackend.Router, options: [port: 3001]}
      # Starts a worker by calling: EperBackend.Worker.start_link(arg)
      # {EperBackend.Worker, arg}
    ]

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: EperBackend.Supervisor]
    Logger.info("Starting application...")
    Supervisor.start_link(children, opts)
  end
end
