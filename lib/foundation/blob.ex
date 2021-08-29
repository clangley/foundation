defmodule Foundation.Blob do
  @moduledoc """
  Implement a blob storage solution ontop of FDB. Very simple implementation with very little guarantees.
  """

  @chunk_size 10_000
  defstruct db: nil, path: [], size: 0, count: 0, value: <<>>, current: 0, chunk_size: @chunk_size

  def new(db, path, value, chunk_size \\ @chunk_size) do
    count = :math.ceil(byte_size(value) / chunk_size)

    %__MODULE__{
      path: path,
      db: db,
      size: byte_size(value),
      count: Kernel.trunc(count),
      value: value,
      current: 0,
      chunk_size: chunk_size
    }
  end

  def split(value, size) do
    try do
      :erlang.split_binary(value, size)
    rescue
      # This means we can no longer split data, we just return the last of the bytes
      _e in ArgumentError ->
        {value, ""}
    end
  end

  def upload(%__MODULE__{current: current, count: count, chunk_size: chunk_size} = blob)
      when current < count do
    {chunk, rest} = split(blob.value, chunk_size)

    Foundation.trans(blob.db, fn tx ->
      Foundation.set(tx, blob.path ++ ["index", blob.current], chunk)
    end)

    upload(%{blob | current: blob.current + 1, value: rest})
  end

  def upload(_), do: :ok

  def get(db, path) do
    Foundation.trans(db, fn tx ->
      Foundation.get_range_startswith(tx, path)
    end)
  end
end
