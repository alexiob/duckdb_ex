defmodule Mix.Tasks.DownloadNif do
  @moduledoc """
  Downloads precompiled NIFs or builds from source.

  This task ensures the NIF is available for the application.
  """

  use Mix.Task

  @shortdoc "Downloads or builds the DuckDB NIF"

  def run(_args) do
    # Start :inets and :ssl applications needed for HTTP requests
    Application.ensure_all_started(:inets)
    Application.ensure_all_started(:ssl)

    case DuckdbEx.NifDownloader.download_nif() do
      :ok ->
        Mix.shell().info("NIF is ready")

      {:error, reason} ->
        Mix.shell().error("Failed to download or build NIF: #{reason}")
        System.halt(1)
    end
  end
end
