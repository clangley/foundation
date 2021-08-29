defmodule Foundation do
  @moduledoc """
  Documentation for Foundation.

  Opinionated and high level wrapper around :erlfdb, this library is build to provide shortcuts around the erlang api and simplify the interface.
  """
  alias Foundation.Utils
  require Logger

  @type path :: list(term()) | binary()
  @type value :: term()
  @type kv :: {path(), value()}
  @type db :: {:erlfdb_database, reference()} | tx()
  @type tx ::
          {:erlfdb_snapshot,
           {:erlfdb_snapshot, {:erlfdb_snapshot, {any, any}} | {:erlfdb_transaction, reference}}
           | {:erlfdb_transaction, reference}}
          | {:erlfdb_transaction, reference}
  @type versionstamp :: {:versionstamp, integer(), integer(), non_neg_integer()}
  @type opts :: [{atom(), term()}]

  @reverse false
  @limit 100

  @spec versionstamp(tx()) :: versionstamp()
  def versionstamp(tx) do
    txid = :erlfdb.get_next_tx_id(tx)
    {:versionstamp, 0xFFFFFFFFFFFFFFFF, 0xFFFF, txid}
  end

  @spec get_status(tx()) :: any
  def get_status(tx), do: get_system_key(tx, Utils.status_key()) |> wait()

  @spec get_metadata_version(tx()) :: any
  def get_metadata_version(tx), do: get_system_key(tx, Utils.metadata_key()) |> wait()

  @spec get_system_key(tx(), binary | [any] | tuple) :: any
  def get_system_key(tx, key), do: :erlfdb.get(tx, pack(key)) |> wait()

  def db(application \\ :foundation, name \\ :fdb) do
    Application.get_env(application, name, nil)
  end

  # TODO: Look into behaviors for a shared interface
  def directory(tx, path),
    do: Application.get_env(:foundation, :directory_fn, Foundation.Directory).directory(tx, path)

  @spec trans(db(), (tx() -> any)) :: any
  def trans(db, fun) do
    :erlfdb.transactional(db, fun)
  end

  @spec get_or_set(tx(), path(), value()) :: any
  def get_or_set(tx, path, value) do
    case get(tx, path) do
      :not_found ->
        set(tx, path, value)

      ret ->
        ret
    end
  end

  @spec set_if_exists(tx(), path(), value()) :: :not_found | :ok
  def set_if_exists(tx, path, value) do
    case get(tx, path) do
      :not_found ->
        :not_found

      _ ->
        set(tx, path, value)
    end
  end

  @spec compare_and_swap(tx(), path(), value(), value()) ::
          {:error, :not_found | :value_changed} | {:ok, :ok}
  def compare_and_swap(tx, path, prev_value, new_value) do
    case get(tx, path) do
      ^prev_value ->
        {:ok, set(tx, path, new_value)}

      val when is_binary(val) ->
        {:error, :value_changed}

      :not_found ->
        {:error, :not_found}
    end
  end

  @spec set(tx(), maybe_improper_list, any) :: :ok
  def set(tx, path, value) do
    cond do
      Utils.contains_incomplete_versionstamp?(path) ->
        :erlfdb.set_versionstamped_key(
          tx,
          pack(path),
          term_to_binary(value)
        )

      # TODO: Research more, and do not use for time being
      Utils.is_incomplete_versionstamp?(value) ->
        # Running into issues with using pack_vs for set_versionstamped_value, so just pulling out local
        # counter and using bitstring syntax. Could also call :erlfdb.get_next_tx_id(tx) for txid
        # THEORY: 12 byte versionstamps only for keys and 10 byte versionstamp for values
        # NOT sure why 14 byte (112 bits) works here, might be a client thing from :erlfdb, Need to investigate
        :erlfdb.set_versionstamped_value(tx, pack(path), <<0::112>>)

      true ->
        :erlfdb.set(tx, pack(path), term_to_binary(value))
    end
  end

  @spec get(tx(), maybe_improper_list) :: any
  def get(tx, path) do
    case :erlfdb.get(tx, pack(path)) |> wait() do
      :not_found ->
        :not_found

      data ->
        binary_to_term(data)
    end
  end

  @spec clear(tx(), path()) :: any
  def clear(tx, path) do
    :erlfdb.clear(tx, pack(path)) |> wait()
  end

  def range(tx, start_key, end_key, opts \\ []) do
    :erlfdb.get_range(tx, pack(start_key), pack(end_key), range_opts(opts))
    |> wait()
    |> decode_range()
  end

  def get_range_startswith(tx, prefix, opts \\ [])

  def get_range_startswith(tx, prefix, opts) when is_list(prefix) do
    get_range_startswith(tx, List.to_tuple(prefix), opts)
  end

  def get_range_startswith(tx, prefix, opts) when is_tuple(prefix) do
    :erlfdb.get_range_startswith(tx, :erlfdb_tuple.pack(prefix), opts)
    |> wait()
    |> decode_range()
  end

  def decode_range(range_results) do
    range_results
    |> Enum.map(fn {k, v} ->
      {Tuple.to_list(:erlfdb_tuple.unpack(k)), binary_to_term(v)}
    end)
  end

  def pack(path) when is_list(path), do: pack(List.to_tuple(path))

  def pack(path) when is_tuple(path) do
    # TODO: contains_incomplete_versionstamp? converts back to a list
    case Utils.contains_incomplete_versionstamp?(path) do
      true ->
        :erlfdb_tuple.pack_vs(path)

      false ->
        :erlfdb_tuple.pack(path)
    end
  end

  def pack(path) when is_binary(path), do: path

  @spec wait(any) :: any
  def wait(x), do: :erlfdb.wait(x)

  def range_opts(opts \\ []) do
    opts
    |> Keyword.put_new(:reverse, @reverse)
    |> Keyword.put_new(:limit, @limit)
  end

  def binary_to_term(v) do
    try do
      Application.get_env(:foundation, :decoder, &:erlang.binary_to_term/1).(v)
    rescue
      ArgumentError -> v
    end
  end

  def term_to_binary(v) do
    try do
      Application.get_env(:foundation, :encoder, &:erlang.term_to_binary/1).(v)
    rescue
      ArgumentError -> v
    end
  end

  # Default value for commonly passed options
  def reverse(opts \\ []), do: Keyword.get(opts, :reverse, @reverse)
  def limit(opts \\ []), do: Keyword.get(opts, :limit, @limit)
end
