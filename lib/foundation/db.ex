defmodule Foundation.DB do
  @moduledoc """
  Manages Foundation connections and stores them in ETS table by name.
  You can open multiple Foundation connections to different clusters, the names just have to be unique.
  """
  use GenServer
  @clusterfile "/etc/foundationdb/fdb.cluster"

  @spec start_link(any) :: :ignore | {:error, any} | {:ok, pid}
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts)
  end

  @impl true
  @spec init(keyword) :: {:ok, keyword} | {:stop, :failed_to_connect}
  def init(opts) do
    clusterfile = Keyword.get(opts, :clusterfile, System.get_env("FDB_CLUSTER_FILE", @clusterfile))
    application = Keyword.get(opts, :application, :foundation)
    db = :erlfdb.open(clusterfile)

    db =
      case Application.get_env(application, :fdb, nil) do
        nil ->
          Application.put_env(application, :fdb, db)
          db

        db ->
          db
      end

    Application.put_env(application, :fdb, db)
    {:ok, db}
  end
end
