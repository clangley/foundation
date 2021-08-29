defmodule FoundationTest.Directory do
  use ExUnit.Case

  test "Bucket Test" do
    db = :erlfdb_util.get_test_db(empty: true)
    Application.put_env(:foundation, :directory_fn, Foundation.Directory)

    bytes =
      Foundation.trans(db, fn tx ->
        Foundation.directory(tx, ["foo", "hello"])
      end)

    assert Foundation.Directory.get_directory(db, ["foo", "hello"]) == bytes
  end
end
