defmodule Foundation.Locks do
  @moduledoc """
  Distributed locking semantics. This is useful if you want to do leader election or enforce 1 process to have access to a
  specific keyspace.

  WARNING: Locks are only enforced if all interested processes go through the locking process.

  Locks are also based on timeouts, the winner of a lock will set its expiration time in a unix timestamp. The same process has to ensure it
  extends the lock if it needs more time. If the process does not extend the lock it is assumed to have crashed or finished work.
  If you need finer control you could have a lock manager process that sends pings to all processes that currently have the lock to ensure
  they are still alive.
  """

  @type lock :: {term(), integer()}
  def now(seconds_to_add \\ 0) do
    DateTime.utc_now()
    |> DateTime.add(seconds_to_add)
    |> DateTime.to_unix()
  end

  @spec set_lock(Foundation.tx(), Foundation.path(), binary(), integer) :: lock()
  def set_lock(tx, path, id, expires_at \\ now(30)) do
    Foundation.set(tx, path, {id, expires_at})
    {id, expires_at}
  end

  @spec did_acquire_lock?(
          {:erlfdb_snapshot, {:erlfdb_transaction, reference}} | {:erlfdb_transaction, reference},
          [any],
          binary,
          integer
        ) :: boolean
  def did_acquire_lock?(tx, path, id, expires_at \\ now(30)) do
    acquire_lock(tx, path, id, expires_at) == {id, expires_at}
  end

  @spec acquire_lock(Foundation.tx(), Foundation.path(), binary(), integer) :: lock()
  def acquire_lock(tx, path, id, expires_at \\ now(30)) do
    results = Foundation.get(tx, path)
    do_acquire_lock(tx, path, {id, expires_at}, results)
  end

  defp do_acquire_lock(tx, path, {request_id, request_expires}, :not_found) do
    set_lock(tx, path, request_id, request_expires)
  end

  defp do_acquire_lock(tx, path, {request_id, request_expires}, {current_id, current_expires}) do
    cond do
      current_id == request_id ->
        set_lock(tx, path, request_id, request_expires)

      current_expires < now() ->
        set_lock(tx, path, request_id, request_expires)

      true ->
        {current_id, current_expires}
    end
  end
end
