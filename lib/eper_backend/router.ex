defmodule EperBackend.Router do
  use Plug.Router
  plug(Plug.Logger)

  plug(Plug.Static,
    at: "/",
    from: :eper_backend
  )

  plug(:match)
  plug(Plug.Parsers, parsers: [:json], json_decoder: Jason)
  plug(:dispatch)

  get "/ping" do
    send_resp(conn, 200, "pong!")
  end

  get "/api/makes" do
    makes = EperBackend.PartsServer.makes()

    conn
    |> put_resp_content_type("application/json")
    |> send_resp(200, Jason.encode!(makes))
  end

  get "/api/catalogues/:make" do
    catalogues = EperBackend.PartsServer.catalogues(make)

    conn
    |> put_resp_content_type("application/json")
    |> send_resp(200, Jason.encode!(catalogues))
  end

  get "/api/groups/:catalogue" do
    groups = EperBackend.PartsServer.groups(catalogue)

    conn
    |> put_resp_content_type("application/json")
    |> send_resp(200, Jason.encode!(groups))
  end

  get "/api/sub_groups/:catalogue/:group" do
    sub_groups = EperBackend.PartsServer.sub_groups(catalogue, group)

    conn
    |> put_resp_content_type("application/json")
    |> send_resp(200, Jason.encode!(sub_groups))
  end

  get "/api/drawings/:catalogue/:group/:sub_group" do
    drawings = EperBackend.PartsServer.drawings(catalogue, group, sub_group)

    conn
    |> put_resp_content_type("application/json")
    |> send_resp(200, Jason.encode!(drawings))
  end

  defp find_alt_image(path, imagefile) do
    zip_file = "#{Application.fetch_env!(:eper_backend, :image_path)}/L_EPERFIG.res"
    imagefile = "#{path}/#{imagefile}"

    case :zip.extract(to_charlist(zip_file), [:memory, {:file_list, [to_charlist(imagefile)]}]) do
      {:ok, [{_image_file, image_data}]} -> {:ok, image_data}
      _ -> {:error, "no file"}
    end
  end

  defp find_image(path, imagefile) do
    zip_file = "#{Application.fetch_env!(:eper_backend, :image_path)}/#{path}.res"

    if File.exists?(zip_file) do
      case :zip.extract(to_charlist(zip_file), [:memory, {:file_list, [to_charlist(imagefile)]}]) do
        {:ok, [{_image_file, image_data}]} -> {:ok, image_data}
        _ -> find_alt_image(path, imagefile)
      end
    else
      {:error, "no file"}
    end
  end

  get "/api/image/:path/:imagefile" do
    # cgm, tif, jpg always try to find png
    imagefile = "#{String.slice(imagefile, 0..-5)}.png"

    case find_image(path, imagefile) do
      {:ok, image_data} -> send_resp(conn, 200, image_data)
      _ -> send_resp(conn, 404, "not found")
    end
  end

  match _ do
    send_resp(conn, 404, "oops... Nothing here :(")
  end
end
