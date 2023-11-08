# EperBackend

Tested with ePer v.76 data

## Configuration

Use `config/mix.exs` to set paths for ePER data files.

```elixir
base_path = ""
config :eper_backend,
  image_path: "#{base_path}/images",
  vin_database: "#{base_path}/SP.CH.03818.FCTLR",
  parts_database: "#{base_path}/SP.DB.03818.FCTLR"
````

## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed
by adding `eper_backend` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:eper_backend, "~> 0.1.0"}
  ]
end
```

Documentation can be generated with [ExDoc](https://github.com/elixir-lang/ex_doc)
and published on [HexDocs](https://hexdocs.pm). Once published, the docs can
be found at <https://hexdocs.pm/eper_backend>.

