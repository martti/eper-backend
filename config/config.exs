import Config

# base_path = ""
# config :eper_backend,
#   image_path: "#{base_path}/images",
#   site_image_path: "#{base_path}/../appsrv/site/2009/img",
#   vin_database: "#{base_path}/SP.CH.03818.FCTLR",
#   parts_database: "#{base_path}/SP.DB.03818.FCTLR"

import_config "#{config_env()}.exs"
