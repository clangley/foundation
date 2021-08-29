defmodule Foundation.Directory do
  @moduledoc """
  Directories are a layer in Foundationdb that allows you to shorten common prefixes to a few bytes.

  As long as you give a valid path (list, tuple), Directory can shrink it down to a few bytes. Any client
  that ask for the same directory will get the same bytes.

  The only thing to be aware of is that Directory.directory(["a", 1]) and Directory.direcotry(["a",2]) will be sorted
  together like they would with other paths

  Also, FDB directories typically have a concept of renaming which is not covered here. If you want to attempt to
  rename keys you can look at :erlfdb_directory module, but it is not supported in this library currently
  """
  def get_directory(db, path) do
    Foundation.trans(db, fn tx ->
      directory(tx, path)
    end)
  end

  def directory(tx, path) when is_list(path),
    do: directory(tx, List.to_tuple(path))

  def directory(tx, path) when is_tuple(path) do
    path_as_only_strings = safe_directory(path)

    :erlfdb_directory.create_or_open(tx, :erlfdb_directory.root(), path_as_only_strings)
    |> :erlfdb_directory.get_name()
  end

  def safe_directory(path) when is_tuple(path), do: safe_directory(Tuple.to_list(path))

  def safe_directory(path) when is_list(path) do
    Enum.map(path, fn item -> "#{item}" end)
  end
end
