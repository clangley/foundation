defmodule Foundation.Utils do
  @moduledoc """
  Contains misc functions used in multiple places in the library
  """

  def status_key(), do: "\xff\xff/status/json"
  def metadata_key(), do: "\xff/metadataVersion"

  def contains_incomplete_versionstamp?(t) when is_tuple(t) do
    Tuple.to_list(t) |> contains_incomplete_versionstamp?()
  end

  def contains_incomplete_versionstamp?(path) when is_list(path) do
    Enum.any?(path, &is_incomplete_versionstamp?/1)
  end

  def contains_incomplete_versionstamp?(binary) when is_binary(binary),
    do: is_incomplete_versionstamp?(binary)

  def is_incomplete_versionstamp?({:versionstamp, 0xFFFFFFFFFFFFFFFF, 0xFFFF, _}), do: true
  def is_incomplete_versionstamp?(<<0::112>>), do: true
  def is_incomplete_versionstamp?(_), do: false

  def is_versionstamp({:versionstamp, _, _, _}), do: true
  def is_versionstamp(_), do: false

  @spec limit(keyword) :: any
  def limit(opts \\ [], default \\ 100), do: Keyword.get(opts, :limit, default)

  @spec reverse(keyword) :: any
  def reverse(opts \\ [], default \\ false), do: Keyword.get(opts, :reverse, default)

  def timeout(opts \\ [], default \\ 30), do: Keyword.get(opts, :timeout, default) * 1000
end
