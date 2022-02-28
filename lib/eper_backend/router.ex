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

  get "/api/make/:make" do
    make = EperBackend.PartsServer.make(make)

    conn
    |> put_resp_content_type("application/json")
    |> send_resp(200, Jason.encode!(make))
  end

  get "/api/models" do
    models = EperBackend.PartsServer.models()

    conn
    |> put_resp_content_type("application/json")
    |> send_resp(200, Jason.encode!(models))
  end

  get "/api/catalogues/:make/:model" do
    catalogues = EperBackend.PartsServer.catalogues(make, model)

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

  get "/api/parts/:catalogue/:group/:sub_group/:sgs_code" do
    tbdata = EperBackend.PartsServer.tbdata(catalogue, group, sub_group, sgs_code)

    conn
    |> put_resp_content_type("application/json")
    |> send_resp(200, Jason.encode!(tbdata))
  end

  get "/api/vin/:vin" do
    vin_data = EperBackend.VinServer.search(vin)

    conn
    |> put_resp_content_type("application/json")
    |> send_resp(200, Jason.encode!(vin_data))
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

  # image/logo/F.png
  # image/model/F-BRA.jpg
  get "/api/image/logo/:brand.png" do
    valid = brand =~ ~r/^\w+$/

    image_file =
      "#{Application.fetch_env!(:eper_backend, :site_image_path)}/#{brand}/tablet_startup_logo.png"

    case File.exists?(image_file) && valid do
      true ->
        conn
        |> put_resp_content_type("image/png")
        |> send_file(200, image_file)

      _ ->
        send_resp(conn, 404, "not found")
    end
  end

  get "/api/image/logo/:brand/:model.jpg" do
    valid = brand =~ ~r/^\w+$/ && model =~ ~r/^\w+$/

    image_file =
      "#{Application.fetch_env!(:eper_backend, :site_image_path)}/#{brand}/model_imgs/normal/#{model}"

    case File.exists?(image_file) && valid do
      true ->
        conn
        |> put_resp_content_type("image/jpeg")
        |> send_file(200, image_file)

      _ ->
        send_resp(conn, 404, "not found")
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
