defmodule DuckdbEx.NifDownloader do
  @moduledoc """
  Downloads precompiled NIFs for DuckdbEx.

  This module handles downloading platform-specific precompiled NIFs from GitHub releases
  when the package is installed, unless the DUCKDB_EX_BUILD environment variable is set.
  """

  require Logger

  @github_repo "alexiob/duckdb_ex"
  @nif_versions ["2.15", "2.16"]

  def download_nif() do
    # First check if NIF already exists and is valid
    if nif_exists?() and not should_force_rebuild?() do
      Logger.debug("NIF already exists, skipping download/build")
      :ok
    else
      if should_build_from_source?() do
        Logger.info("DUCKDB_EX_BUILD is set, building from source...")
        build_from_source()
      else
        Logger.info("Attempting to download precompiled NIF...")
        download_precompiled_nif()
      end
    end
  end

  defp should_build_from_source?() do
    case System.get_env("DUCKDB_EX_BUILD") do
      nil -> false
      "" -> false
      "false" -> false
      "0" -> false
      _ -> true
    end
  end

  defp should_force_rebuild?() do
    # Force rebuild if DUCKDB_EX_FORCE_REBUILD is set or if we're doing a clean build
    case System.get_env("DUCKDB_EX_FORCE_REBUILD") do
      nil -> false
      "" -> false
      "false" -> false
      "0" -> false
      _ -> true
    end
  end

  defp nif_exists?() do
    priv_dir = ensure_priv_dir()

    # Get target to determine correct extension
    case get_target_info() do
      {:ok, target_info} ->
        {so_ext, _} = get_extensions_for_target(target_info.target)
        nif_path = Path.join(priv_dir, "duckdb_ex#{so_ext}")
        File.exists?(nif_path) and is_valid_nif?(nif_path)

      {:error, _} ->
        # Fallback: check common extensions
        so_path = Path.join(priv_dir, "duckdb_ex.so")
        dll_path = Path.join(priv_dir, "duckdb_ex.dll")

        (File.exists?(so_path) and is_valid_nif?(so_path)) or
          (File.exists?(dll_path) and is_valid_nif?(dll_path))
    end
  end

  defp is_valid_nif?(nif_path) do
    # Basic check - file exists and has content
    case File.stat(nif_path) do
      {:ok, %File.Stat{size: size}} when size > 0 -> true
      _ -> false
    end
  end

  defp download_precompiled_nif() do
    case get_target_info() do
      {:ok, target_info} ->
        case download_for_target(target_info) do
          :ok ->
            Logger.info("Successfully downloaded precompiled NIF")
            :ok

          {:error, reason} ->
            Logger.warning("Failed to download precompiled NIF: #{reason}")
            Logger.info("Falling back to building from source...")
            build_from_source()
        end

      {:error, reason} ->
        Logger.warning("Could not determine target platform: #{reason}")
        Logger.info("Falling back to building from source...")
        build_from_source()
    end
  end

  defp get_target_info() do
    nif_version = get_nif_version()
    target = get_target_triple()

    case {nif_version, target} do
      {nif_ver, target_triple} when nif_ver in @nif_versions and is_binary(target_triple) ->
        {:ok, %{nif_version: nif_ver, target: target_triple}}

      _ ->
        {:error, "Unsupported NIF version or target"}
    end
  end

  defp get_nif_version() do
    # Get the NIF version from the Erlang system
    case :erlang.system_info(:nif_version) do
      ~c"2.15" -> "2.15"
      ~c"2.16" -> "2.16"
      # Default to latest
      _ -> "2.16"
    end
  end

  defp get_target_triple() do
    case {os_type(), cpu_arch()} do
      {:unix, :aarch64} ->
        case :os.type() do
          {:unix, :darwin} -> "aarch64-apple-darwin"
          {:unix, :linux} -> "aarch64-unknown-linux-gnu"
          _ -> nil
        end

      {:unix, :x86_64} ->
        case :os.type() do
          {:unix, :darwin} -> "x86_64-apple-darwin"
          {:unix, :linux} -> "x86_64-unknown-linux-gnu"
          _ -> nil
        end

      {:win32, :x86_64} ->
        "x86_64-pc-windows-msvc"

      _ ->
        nil
    end
  end

  defp os_type() do
    case :os.type() do
      {:win32, _} -> :win32
      {:unix, _} -> :unix
      other -> other
    end
  end

  defp cpu_arch() do
    case :erlang.system_info(:system_architecture) do
      arch when is_list(arch) ->
        arch_str = to_string(arch)

        cond do
          String.contains?(arch_str, "aarch64") or String.contains?(arch_str, "arm64") ->
            :aarch64

          String.contains?(arch_str, "x86_64") or String.contains?(arch_str, "amd64") ->
            :x86_64

          String.contains?(arch_str, "arm") ->
            :arm

          true ->
            :unknown
        end

      _ ->
        :unknown
    end
  end

  defp download_for_target(%{nif_version: nif_version, target: target}) do
    version = get_package_version()
    package_name = "duckdb_ex-nif-#{nif_version}-#{target}"
    filename = "#{package_name}.tar.gz"
    url = "https://github.com/#{@github_repo}/releases/download/v#{version}/#{filename}"

    priv_dir = ensure_priv_dir()
    temp_path = Path.join(System.tmp_dir!(), filename)

    Logger.info("Downloading #{filename} from #{url}")

    case download_file(url, temp_path) do
      :ok ->
        case extract_package(temp_path, priv_dir, target) do
          :ok ->
            File.rm(temp_path)
            :ok

          {:error, reason} ->
            File.rm(temp_path)
            {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp get_package_version() do
    case Application.spec(:duckdb_ex, :vsn) do
      vsn when is_list(vsn) ->
        to_string(vsn)

      _ ->
        # Fallback: read from mix.exs
        case File.read("mix.exs") do
          {:ok, content} ->
            case Regex.run(~r/version: "([^"]+)"/, content) do
              [_, version] -> version
              _ -> "0.4.0"
            end

          _ ->
            "0.4.0"
        end
    end
  end

  defp ensure_priv_dir() do
    priv_dir = :code.priv_dir(:duckdb_ex)

    case priv_dir do
      {:error, :bad_name} ->
        # During compilation, priv dir might not exist yet
        app_dir = Path.dirname(Path.dirname(__DIR__))
        priv_path = Path.join(app_dir, "priv")
        File.mkdir_p!(priv_path)
        priv_path

      priv_path when is_list(priv_path) ->
        priv_str = to_string(priv_path)
        File.mkdir_p!(priv_str)
        priv_str

      priv_path when is_binary(priv_path) ->
        File.mkdir_p!(priv_path)
        priv_path
    end
  end

  defp download_file(url, dest_path) do
    case :httpc.request(:get, {String.to_charlist(url), []}, [], body_format: :binary) do
      {:ok, {{_, 200, _}, _headers, body}} ->
        File.write(dest_path, body)

      {:ok, {{_, status_code, _}, _headers, _body}} ->
        {:error, "HTTP #{status_code}"}

      {:error, reason} ->
        {:error, "Download failed: #{inspect(reason)}"}
    end
  end

  defp extract_package(tarball_path, priv_dir, target) do
    temp_extract_dir = Path.join(System.tmp_dir!(), "duckdb_extract_#{:rand.uniform(1_000_000)}")

    try do
      # Extract tarball
      case System.cmd("tar", ["-xzf", tarball_path, "-C", System.tmp_dir!()]) do
        {_, 0} ->
          # Determine file extensions for this target
          {so_ext, dylib_ext} = get_extensions_for_target(target)

          # Find the extracted directory
          package_name = Path.basename(tarball_path, ".tar.gz")
          extracted_dir = Path.join(System.tmp_dir!(), package_name)

          # Copy NIF to priv directory
          nif_source = Path.join(extracted_dir, "duckdb_ex#{so_ext}")
          nif_dest = Path.join(priv_dir, "duckdb_ex#{so_ext}")

          case File.cp(nif_source, nif_dest) do
            :ok ->
              make_executable(nif_dest)

              # Copy DuckDB library to priv directory
              dylib_source = Path.join(extracted_dir, "libduckdb#{dylib_ext}")
              dylib_dest = Path.join(priv_dir, "libduckdb#{dylib_ext}")

              case File.cp(dylib_source, dylib_dest) do
                :ok ->
                  make_executable(dylib_dest)
                  Logger.info("Successfully extracted NIF and DuckDB library")
                  :ok

                {:error, reason} ->
                  {:error, "Failed to copy DuckDB library: #{inspect(reason)}"}
              end

            {:error, reason} ->
              {:error, "Failed to copy NIF: #{inspect(reason)}"}
          end

        {output, exit_code} ->
          {:error, "Failed to extract tarball (exit code: #{exit_code}): #{output}"}
      end
    after
      # Clean up extracted directory
      if File.exists?(temp_extract_dir) do
        File.rm_rf(temp_extract_dir)
      end

      # Clean up the package directory that was extracted
      package_name = Path.basename(tarball_path, ".tar.gz")
      extracted_dir = Path.join(System.tmp_dir!(), package_name)

      if File.exists?(extracted_dir) do
        File.rm_rf(extracted_dir)
      end
    end
  end

  defp get_extensions_for_target(target) do
    if String.contains?(target, "windows") do
      {".dll", ".dll"}
    else
      if String.contains?(target, "apple-darwin") do
        {".so", ".dylib"}
      else
        {".so", ".so"}
      end
    end
  end

  defp make_executable(path) do
    case :os.type() do
      {:unix, _} ->
        System.cmd("chmod", ["+x", path])

      _ ->
        :ok
    end
  end

  defp build_from_source() do
    Logger.info("Building NIF from source...")

    case System.cmd("make", [], stderr_to_stdout: true) do
      {output, 0} ->
        Logger.info("Successfully built NIF from source")
        Logger.debug("Build output: #{output}")
        :ok

      {output, exit_code} ->
        Logger.error("Failed to build NIF from source (exit code: #{exit_code})")
        Logger.error("Build output: #{output}")
        {:error, "Build failed with exit code #{exit_code}"}
    end
  end
end
