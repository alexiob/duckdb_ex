defmodule DuckdbEx.MixProject do
  use Mix.Project

  def project do
    [
      app: :duckdb_ex,
      version: "0.4.0",
      elixir: "~> 1.14",
      compilers: [:elixir_make] ++ Mix.compilers(),
      make_targets: ["all"],
      make_clean: ["clean"],
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      docs: docs(),
      package: package(),
      description: description(),
      aliases: aliases(),
      source_url: "https://github.com/alexiob/duckdb_ex",
      homepage_url: "https://github.com/alexiob/duckdb_ex",
      name: "DuckdbEx",
      author: "Alessandro Iob"
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:elixir_make, "~> 0.8", runtime: false},
      {:ex_doc, "~> 0.31", only: :dev, runtime: false},
      {:jason, "~> 1.4"}
    ]
  end

  defp description do
    """
    High-performance Elixir NIF wrapper for DuckDB analytical database.
    Provides concurrent access, prepared statements, bulk loading via Appender API,
    chunked result streaming, and comprehensive DuckDB extension support.
    """
  end

  defp package do
    [
      name: "duckdb_ex",
      files: [
        "lib",
        "c_src",
        "Makefile",
        "mix.exs",
        "README*",
        "LICENSE*",
        "docs",
        "CHANGELOG*",
        "TODO.md"
      ],
      maintainers: ["Alessandro Iob"],
      organization: nil,
      submitter: "alexiob",
      maintainer: "alessandro@iob.dev",
      licenses: ["MIT"],
      links: %{
        "GitHub" => "https://github.com/alexiob/duckdb_ex",
        "Documentation" => "https://hexdocs.pm/duckdb_ex",
        "DuckDB" => "https://duckdb.org"
      },
      exclude_patterns: [
        "duckdb_sources",
        "priv/duckdb_ex.*",
        "priv/libduckdb.*",
        "_build",
        "deps",
        "TODO.md",
        "docs/TODO.md"
      ]
    ]
  end

  defp docs do
    [
      name: "DuckdbEx",
      main: "guides",
      source_url: "https://github.com/alexiob/duckdb_ex",
      homepage_url: "https://github.com/alexiob/duckdb_ex",
      extras: [
        "README.md",
        "CHANGELOG.md",
        "docs/getting_started.md",
        "docs/query_api.md",
        "docs/chunked_api.md",
        "docs/bulk_loading.md",
        "docs/prepared_statements.md",
        "docs/transactions.md",
        "docs/configuration.md",
        "docs/extensions.md",
        "docs/data_types.md",
        "docs/performance.md",
        "docs/examples.md",
        "docs/guides.md": [title: "Documentation Index"]
      ],
      groups_for_extras: [
        "Getting Started": [
          "docs/getting_started.md"
        ],
        "Core APIs": [
          "docs/query_api.md",
          "docs/chunked_api.md",
          "docs/bulk_loading.md"
        ],
        "Advanced Features": [
          "docs/prepared_statements.md",
          "docs/transactions.md",
          "docs/configuration.md",
          "docs/extensions.md"
        ],
        Reference: [
          "docs/data_types.md",
          "docs/performance.md",
          "docs/examples.md"
        ]
      ],
      groups_for_modules: [
        Core: [
          DuckdbEx,
          DuckdbEx.Connection,
          DuckdbEx.Result
        ],
        "Specialized APIs": [
          DuckdbEx.Appender,
          DuckdbEx.PreparedStatement
        ],
        Configuration: [
          DuckdbEx.Config
        ],
        Internals: [
          DuckdbEx.Nif,
          DuckdbEx.NifDownloader
        ]
      ],
      api_reference: true,
      formatters: ["html"],
      filter_modules: fn module, _metadata ->
        # Only include modules that start with DuckdbEx
        String.starts_with?(to_string(module), "Elixir.DuckdbEx")
      end
    ]
  end

  defp aliases do
    [
      # Download NIF during deps.get (initial install) or when explicitly requested
      "deps.get": ["deps.get", "nif.download"],
      "nif.download": &download_nif_task/1,
      "nif.rebuild": &rebuild_nif_task/1,
      "nif.clean": &clean_nif_task/1,
      clean: ["clean", "nif.clean"]
    ]
  end

  # Download NIF only if it doesn't exist or is invalid
  defp download_nif_task(_args) do
    Mix.Task.run("compile")

    if Code.ensure_loaded?(DuckdbEx.NifDownloader) do
      DuckdbEx.NifDownloader.download_nif()
    else
      Mix.Shell.IO.info("NIF downloader not available, will be handled during runtime")
    end
  end

  # Force rebuild of NIF
  defp rebuild_nif_task(_args) do
    Mix.Task.run("compile")

    if Code.ensure_loaded?(DuckdbEx.NifDownloader) do
      System.put_env("DUCKDB_EX_FORCE_REBUILD", "true")
      DuckdbEx.NifDownloader.download_nif()
    end
  end

  # Clean NIF artifacts
  defp clean_nif_task(_args) do
    priv_dir = "priv"

    if File.exists?(priv_dir) do
      [
        Path.join(priv_dir, "duckdb_ex.so"),
        Path.join(priv_dir, "duckdb_ex.dll"),
        Path.join(priv_dir, "libduckdb.dylib"),
        Path.join(priv_dir, "libduckdb.so")
      ]
      |> Enum.each(fn file ->
        if File.exists?(file) do
          File.rm!(file)
          Mix.Shell.IO.info("Removed #{file}")
        end
      end)
    end
  end
end
