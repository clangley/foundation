defmodule Foundation.Log do
  @moduledoc """
  Logs are an append only data structure with no formal support for deletes.
  Data *can* be deleted if a user used Foundation.clear but this api does not provide that function directly

  Log always adds a versionstamp at the end of the append for the user, this can be disabled by passing [version: false]
  """
  @spec append(
          {:erlfdb_snapshot,
           {:erlfdb_snapshot, {:erlfdb_snapshot, {any, any}} | {:erlfdb_transaction, reference}}
           | {:erlfdb_transaction, reference}}
          | {:erlfdb_transaction, reference},
          [any],
          any,
          keyword
        ) :: :ok
  def append(tx, logpath, value, opts \\ []) do
    case Keyword.get(opts, :version, true) do
      true ->
        Foundation.set(tx, logpath ++ [Foundation.versionstamp(tx)], value)

      _ ->
        Foundation.set(tx, logpath, value)
    end
  end

  def since(tx, path, opts \\ [limit: 100]) do
    maybe_versionstamp = List.last(path)

    case Foundation.Utils.is_versionstamp(maybe_versionstamp) do
      true ->
        start_path = :erlfdb_key.strinc(Foundation.pack(path))
        end_path = :erlfdb_key.strinc(Foundation.pack(path -- [maybe_versionstamp]))
        Foundation.range(tx, start_path, end_path, since_limit(opts))

      false ->
        Foundation.get_range_startswith(tx, path, opts)
    end
  end

  @spec since_limit(keyword) :: [{atom, any}, ...]
  defp since_limit(opts) when is_list(opts),
    do: Keyword.put(opts, :limit, Keyword.get(opts, :limit, 100) + 1)

  @spec before(any, any, any) :: none
  def before(tx, path, opts \\ [reverse: true]) do
    parent = path -- [List.last(path)]
    Foundation.range(tx, parent, path, opts)
  end
end
